/*
 * Copyright (c) 2017 Uber Technologies, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.uber.engsec.dp.dataflow.column

import com.uber.engsec.dp.dataflow.AggFunctions._
import com.uber.engsec.dp.dataflow.column.AbstractColumnAnalysis.ColumnFacts
import com.uber.engsec.dp.dataflow.domain.AbstractDomain
import com.uber.engsec.dp.schema.Schema
import com.uber.engsec.dp.sql.relational_algebra._
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.{BiRel, RelNode, SingleRel}
import org.apache.calcite.rex.{RexInputRef, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun._

import scala.collection.mutable

/** Column fact analysis on relational algebra trees. For more details see [[AbstractColumnAnalysis]].
  */
abstract class RelColumnAnalysis[E, T <: AbstractDomain[E]](domain: AbstractDomain[E])
  extends AbstractColumnAnalysis[RelOrExpr, E, T]
  with RelColumnAnalysisFunctions[E]
  with RelTreeFunctions {

  // Use a regular hashmap for results (instead of IdentityHashMap)
  override val resultMap: mutable.HashMap[RelOrExpr, ColumnFacts[E]] = mutable.HashMap()

  override final def transferNode(node: RelOrExpr, state: ColumnFacts[E]): ColumnFacts[E] = {

    val newFacts: Seq[E] = node match {
      case Relation(rel) =>
        assert (state.length == rel.getRowType.getFieldCount)
        rel match {
          case t: TableScan =>
            state.zipWithIndex.map { case (fact, idx) => transferTableScan(t, idx, fact) }

          case j: Join =>
            state.zipWithIndex.map { case (fact, idx) => transferJoin(j, idx, fact) }

          case f: Filter =>
            state.zipWithIndex.map { case (fact, idx) => transferFilter(f, idx, fact) }

          case s: Sort =>
            state.zipWithIndex.map { case (fact, idx) => transferSort(s, idx, fact) }

          case a: Aggregate =>
            state.zipWithIndex.map { case (fact, idx) =>
              val aggFunction =
                if (idx < a.getGroupCount) { // grouped columns are always the leading fields
                  None
                } else {
                  val agg = a.getAggCallList.get(idx - a.getGroupSet.cardinality).getAggregation match {
                    case _: SqlCountAggFunction => COUNT
                    case a: SqlAvgAggFunction if a.kind == SqlKind.AVG => AVG
                    case a: SqlAvgAggFunction if a.kind == SqlKind.STDDEV_POP || a.kind == SqlKind.STDDEV_SAMP => STDDEV
                    case a: SqlAvgAggFunction if a.kind == SqlKind.VAR_POP || a.kind == SqlKind.VAR_SAMP => VAR
                    case _: SqlSumAggFunction => SUM
                    case _: SqlSumEmptyIsZeroAggFunction => SUM
                    case m: SqlMinMaxAggFunction if m.getKind == SqlKind.MIN => MIN
                    case m: SqlMinMaxAggFunction if m.getKind == SqlKind.MAX => MAX
                  }
                  Some(agg)
                }

              transferAggregate(a, idx, aggFunction, fact)
            }

          case p: Project =>
            state.zipWithIndex.map { case (fact, idx) => transferProject(p, idx, fact) }
        }

      case Expression(expr) =>
        assert(state.length == 1)
        expr match {
          case r: RexInputRef => state // no need to call transfer function, we already propagated state to this node
          case _ => List(transferExpression(expr, state.head))
        }
    }

    newFacts.toIndexedSeq
  }

  // We need to keep track of the current relation to resolve InputRef nodes to their target relations in order
  // to propagate state to these nodes. We accomplish this by keeping a stack of relation nodes visited so far;
  // the target relation of any InputRef node's is an input field of the relation at the top of the stack.
  var relationStack: List[RelNode] = Nil

  override def process(node: RelOrExpr): Unit = {
    node match {
      case Relation(r) =>
        relationStack = r :: relationStack
        super.process(node)
        relationStack = relationStack.tail
      case _ => super.process(node)
    }
  }

  override def joinNode(node: RelOrExpr, children: Iterable[RelOrExpr]): ColumnFacts[E] = {
    import scala.collection.JavaConverters._

    node match {
      case Relation(t: TableScan) =>
        IndexedSeq.fill(t.getRowType.getFieldCount)(domain.bottom)

      case Relation(a: Aggregate) =>
        val inputFacts = resultMap(a.getInput)
        val groupedInputs = a.getGroupSet.toList.asScala
        val factsFromGroupedInputs = groupedInputs.map { inputFacts(_) }

        val factsFromAggCalls = a.getAggCallList.asScala.map { call =>
          val argIndexes = call.getArgList.asScala

          // Reduce (join) facts for all input arguments to this aggregation call.
          val childFacts =
            if (argIndexes.isEmpty) // e.g. COUNT(*)
              inputFacts
            else
              argIndexes.map { inputFacts(_) }

          AbstractColumnAnalysis.joinFacts(domain, childFacts)
        }

        val allFacts = factsFromGroupedInputs ++ factsFromAggCalls
        allFacts.toIndexedSeq

      case Relation(p: Project) =>
        val result = p.getProjects.asScala.map { resultMap(_).head }
        result.toIndexedSeq

      case Relation(f: Filter) =>
        resultMap(Relation(f.getInput))

      case Relation(s: Sort) =>
        resultMap(Relation(s.getInput))

      case Relation(j: Join) =>
        resultMap(j.getLeft) ++ resultMap(j.getRight)

      case Expression(i: RexInputRef) =>
        // Figure out which relation/column is being referenced so we can grab the correct column facts.
        val curRelation = relationStack.head
        val (targetRelation, targetIdx): (RelNode, Int) = curRelation match {
          case s: SingleRel => // Project, Filter, etc.
            (s.getInput, i.getIndex)
          case b: BiRel => // Join, etc. Target relation may be either .left or .right depending on the column index.
            val numColsInLeftRelation = b.getLeft.getRowType.getFieldCount
            if (i.getIndex < numColsInLeftRelation)
              (b.getLeft, i.getIndex)
            else
              (b.getRight, i.getIndex - numColsInLeftRelation)
        }

        val resultFacts = resultMap(Relation(targetRelation))(targetIdx)
        IndexedSeq(resultFacts)

      case Expression(_) => flattenJoinChildren(domain, node, children)

      case Relation(r) => throw new RuntimeException(s"Unhandled relation node type: ${node.unwrap.getClass}")
    }
  }
}

/** Subclasses may override any of these methods as appropriate. */
trait RelColumnAnalysisFunctions[E] {
  /** If aggFunction is None, the current column is a grouped column. */
  def transferAggregate(node: Aggregate, idx: Int, aggFunction: Option[AggFunction], state: E): E = state
  def transferTableScan(node: TableScan, idx: Int, state: E): E = state
  def transferJoin(node: Join, idx: Int, state: E): E = state
  def transferFilter(node: Filter, idx: Int, state: E): E = state
  def transferSort(node: Sort, idx: Int, state: E): E = state
  def transferProject(node: Project, idx: Int, state: E): E = state
  def transferExpression(node: RexNode, state: E): E = state
}