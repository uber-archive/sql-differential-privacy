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
import com.uber.engsec.dp.dataflow.{AbstractDataflowAnalysis, AggFunctions}
import com.uber.engsec.dp.sql.relational_algebra.{Expression, RelOrExpr, RelTreeFunctions, Relation}
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.{BiRel, RelNode, SingleRel}
import org.apache.calcite.rex.{RexInputRef, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun._

import scala.collection.mutable

/** An analysis that tracks facts in tandem for both nodes and columns (e.g., where node-level facts
  * inform analysis logic for columns, or vice-versa).
  */
class RelNodeColumnAnalysis[N,C](nodeDomain: AbstractDomain[N], colDomain: AbstractDomain[C])
  // extends AbstractColumnAnalysis[RelOrExpr, E, T]
  extends AbstractDataflowAnalysis[RelOrExpr, NodeColumnFacts[N,C]]
  with RelNodeColumnAnalysisFunctions[N,C]
  with RelTreeFunctions {

  // Use a regular hashmap for results (instead of IdentityHashMap)
  override val resultMap: mutable.HashMap[RelOrExpr, NodeColumnFacts[N,C]] = mutable.HashMap()

  override def transferNode(node: RelOrExpr, state: NodeColumnFacts[N,C]): NodeColumnFacts[N,C] = {
    val (colState, nodeState) = (state.colFacts, state.nodeFact)
    node match {
      case Relation(rel) =>
        assert (colState.length == rel.getRowType.getFieldCount)
        rel match {
          case t: TableScan => transferTableScan(t, state)
          case v: Values => transferValues(v, state)
          case j: Join => transferJoin(j, state)
          case f: Filter => transferFilter(f, state)
          case s: Sort => transferSort(s, state)
          case p: Project => transferProject(p, state)
          case u: Union => transferUnion(u, state)
          case m: Minus => transferMinus(m, state)
          case c: Correlate => transferCorrelate(c, state)
          case i: Intersect => transferIntersect(i, state)
          case a: Aggregate =>
            val aggFunctions: IndexedSeq[Option[AggFunctions.AggFunction]] = colState.zipWithIndex.map { case (fact, idx) =>
              if (idx < a.getGroupCount) { // grouped columns are always the leading fields
                None
              }
              else {
                val agg = a.getAggCallList.get(idx - a.getGroupSet.cardinality).getAggregation match {
                  case _: SqlCountAggFunction => COUNT
                  case a: SqlAvgAggFunction if a.kind == SqlKind.AVG => AVG
                  case a: SqlAvgAggFunction if a.kind == SqlKind.STDDEV_POP || a.kind == SqlKind.STDDEV_SAMP => STDDEV
                  case a: SqlAvgAggFunction if a.kind == SqlKind.VAR_POP || a.kind == SqlKind.VAR_SAMP => VAR
                  case _: SqlSumAggFunction => SUM
                  case _: SqlSumEmptyIsZeroAggFunction => SUM
                  case m: SqlMinMaxAggFunction if m.getKind == SqlKind.MIN => MIN
                  case m: SqlMinMaxAggFunction if m.getKind == SqlKind.MAX => MAX
                  case _: SqlSingleValueAggFunction => SINGLE_VALUE
                }
                Some(agg)
              }
            }.toIndexedSeq

            transferAggregate(a, aggFunctions, state)
        }

      case Expression(expr) =>
        assert(colState.length == 1)
        expr match {
          case r: RexInputRef => state // no need to call transfer function, we already propagated state to this node
          case _ =>
            val colResult = transferExpression(expr, colState.head)
            NodeColumnFacts(nodeState, IndexedSeq(colResult))
        }
    }
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

  override def joinNode(node: RelOrExpr, children: Iterable[RelOrExpr]): NodeColumnFacts[N,C] = {
    import scala.collection.JavaConverters._

    node match {
      case Relation(t: TableScan) =>
        val colFacts = IndexedSeq.fill(t.getRowType.getFieldCount)(colDomain.bottom)
        val nodeFact = nodeDomain.bottom
        NodeColumnFacts(nodeFact, colFacts)

      case Relation(v: Values) =>
        val colFacts = IndexedSeq.fill(v.getRowType.getFieldCount)(colDomain.bottom)
        val nodeFact = nodeDomain.bottom
        NodeColumnFacts(nodeFact, colFacts)

      case Relation(a: Aggregate) =>
        val inputResult = resultMap(a.getInput)

        val (inputColFacts, inputNodeFact) = (inputResult.colFacts, inputResult.nodeFact)

        val groupedInputs = a.getGroupSet.toList.asScala
        val factsFromGroupedInputs = groupedInputs.map { inputColFacts(_) }

        val factsFromAggCalls = a.getAggCallList.asScala.map { call =>
          val argIndexes = call.getArgList.asScala

          // Reduce (join) facts for all input arguments to this aggregation call.
          val childFacts =
            if (argIndexes.isEmpty) // e.g. COUNT(*)
              inputColFacts
            else
              argIndexes.map { inputColFacts(_) }

          AbstractColumnAnalysis.joinFacts(colDomain, childFacts)
        }

        val allFacts = factsFromGroupedInputs ++ factsFromAggCalls
        NodeColumnFacts(inputNodeFact, allFacts.toIndexedSeq)

      case Relation(p: Project) =>
        val newColFacts = p.getProjects.asScala.map { resultMap(_).colFacts.head }
        val newNodeFact = resultMap(p.getInput).nodeFact
        NodeColumnFacts(newNodeFact, newColFacts.toIndexedSeq)

      case Relation(f: Filter) =>
        resultMap(Relation(f.getInput))

      case Relation(s: Sort) =>
        resultMap(Relation(s.getInput))

      case Relation(u: SetOp) => // Union, Minus, etc.
        val inputFacts = u.getInputs.asScala.map{ resultMap(_) }
        val newNodeFact = AbstractColumnAnalysis.joinFacts(nodeDomain, inputFacts.map{ _.nodeFact })
        // Join column facts of corresponding columns from all children
        val newColFacts = inputFacts.map{ _.colFacts }.transpose.map{ _.reduce( (x,y) => colDomain.leastUpperBound(x, y) )}
        NodeColumnFacts(newNodeFact, newColFacts.toIndexedSeq)

      case Relation(b: BiRel) => // Join, Correlate, etc.
        val leftResult = resultMap(b.getLeft)
        val (leftColFacts, leftNodeFact) = (leftResult.colFacts, leftResult.nodeFact)

        val rightResult = resultMap(b.getRight)
        val (rightColFacts, rightNodeFact) = (rightResult.colFacts, rightResult.nodeFact)

        NodeColumnFacts(nodeDomain.leastUpperBound(leftNodeFact, rightNodeFact), leftColFacts ++ rightColFacts)

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

        val targetRelationFacts = resultMap(Relation(targetRelation))
        val colFactsForTarget = targetRelationFacts.colFacts
        val resultFacts = colFactsForTarget(targetIdx)
        NodeColumnFacts(nodeDomain.bottom, IndexedSeq(resultFacts))

      case Expression(_) =>
        val childrenFacts = children.flatMap{ child => resultMap(child).colFacts }
        val resultFacts = AbstractColumnAnalysis.joinFacts(colDomain, childrenFacts)
        NodeColumnFacts(nodeDomain.bottom, IndexedSeq(resultFacts))

      case Relation(r) => throw new RuntimeException(s"Unhandled relation node type: ${node.unwrap.getClass}")
    }
  }
}

/** Wrapper object to pair node facts with column facts */
case class NodeColumnFacts[N,C](nodeFact: N, colFacts: ColumnFacts[C])

/** Subclasses may override any of these methods as appropriate. */
trait RelNodeColumnAnalysisFunctions[N,C] {
  /** If aggFunction is None, the current column is a grouped column. */
  def transferAggregate(node: Aggregate, aggFunctions: IndexedSeq[Option[AggFunction]], state: NodeColumnFacts[N,C]): NodeColumnFacts[N,C] = state
  def transferTableScan(node: TableScan, state: NodeColumnFacts[N,C]): NodeColumnFacts[N,C] = state
  def transferValues(node: Values, state: NodeColumnFacts[N,C]): NodeColumnFacts[N,C] = state
  def transferJoin(node: Join, state: NodeColumnFacts[N,C]): NodeColumnFacts[N,C] = state
  def transferFilter(node: Filter, state: NodeColumnFacts[N,C]): NodeColumnFacts[N,C] = state
  def transferSort(node: Sort, state: NodeColumnFacts[N,C]): NodeColumnFacts[N,C] = state
  def transferProject(node: Project, state: NodeColumnFacts[N,C]): NodeColumnFacts[N,C] = state
  def transferUnion(node: Union, state: NodeColumnFacts[N,C]): NodeColumnFacts[N,C] = state
  def transferMinus(node: Minus, state: NodeColumnFacts[N,C]): NodeColumnFacts[N,C] = state
  def transferIntersect(node: Intersect, state: NodeColumnFacts[N,C]): NodeColumnFacts[N,C] = state
  def transferCorrelate(node: Correlate, state: NodeColumnFacts[N,C]): NodeColumnFacts[N,C] = state
  def transferExpression(node: RexNode, state: C): C = state
}