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

import com.uber.engsec.dp.dataflow.column.AbstractColumnAnalysis.ColumnFacts
import com.uber.engsec.dp.dataflow.domain.AbstractDomain
import com.uber.engsec.dp.exception.AnalysisException
import com.uber.engsec.dp.sql.dataflow_graph.reference.{ColumnReference, Function, UnstructuredReference}
import com.uber.engsec.dp.sql.dataflow_graph.relation._
import com.uber.engsec.dp.sql.dataflow_graph.{DataflowGraphFunctions, Node}

/** Column fact analysis on dataflow graphs. For more details see [[AbstractColumnAnalysis]].
  */
abstract class DataflowGraphColumnAnalysis[E, D <: AbstractDomain[E]](domain: AbstractDomain[E])
  extends AbstractColumnAnalysis[Node, E, D]
  with DataflowGraphFunctions
  with DataflowGraphColumnAnalysisFunctions[E] {

  override final def transferNode(node: Node, state: ColumnFacts[E]): ColumnFacts[E] = {

    val newFacts: Seq[E] = node match {
      case s: Select => state.zipWithIndex.map { case (fact,idx) => transferSelect(s, idx, fact) }

      case c: ColumnReference => List(transferColumnReference(c, 0, state.head))

      case f: Function =>
        assert (state.length == 1)
        List(transferFunction(f, 0, state.head))

      case u: UnstructuredReference =>
        assert (state.length == 1)
        List(transferUnstructuredReference(u, 0, state.head))

      case t: DataTable =>
        (0 until t.numCols).map { idx =>
          transferDataTable(t, idx, domain.bottom)
        }

      case j: Join =>
        if (state.size != j.numCols) throw new AnalysisException("Schema size mismatch (probably caused by unknown table) in JOIN[" + node.toString + "]. Some columns in this relation have unknown provenance, so analysis results may be incorrect.")
        state.zipWithIndex.map { case (fact,idx) => transferJoin(j, idx, fact) }

      case u: Union =>
        state.zipWithIndex.map { case (fact,idx) => transferUnion(u, idx, fact) }

      case e: Except =>
        state.zipWithIndex.map { case (fact,idx) => transferExcept(e, idx, fact) }
    }

    newFacts.toIndexedSeq
  }

  override def joinNode(node: Node, children: Iterable[Node]): ColumnFacts[E] = {
    node match {
      /** For Select, join fact from where condition (if present) with fact from each SelectItem.
        */
      case s: Select =>
        val colResults = s.items.map{ item =>
          val childResult = resultMap(item.ref)
          assert (childResult.size == 1)
          childResult.head
        }

        colResults.toIndexedSeq

      case c: ColumnReference =>
        val result = resultMap(c.of)(c.colIndex)
        IndexedSeq(result)

      /** For Function and UnstructedReference, reduce all columns from all children into a single column fact.
        */
      case f: Function => flattenJoinChildren(domain, node, children)
      case u: UnstructuredReference => flattenJoinChildren(domain, node, children)

      /** For Join, pass through state from left and right relations, joined with join condition fact.
        */
      case j: Join =>
        val colResults = resultMap(j.left) ++ resultMap(j.right)
        val result =
          if (j.condition.isDefined)
            colResults.map { x => domain.leastUpperBound(x, resultMap(j.condition.get).head) }
          else
            colResults
        result

      /** For Union and Except, join column facts of corresponding columns from all children (schemas of children are guaranteed to match).
        */
      case u: Union => children.map{ resultMap(_) }.transpose.map{ _.reduce( (x,y) => domain.leastUpperBound(x, y) )}.toIndexedSeq
      case e: Except => children.map{ resultMap(_) }.transpose.map{ _.reduce( (x,y) => domain.leastUpperBound(x, y) )}.toIndexedSeq
      case d: DataTable => IndexedSeq.empty  // we'll initialize the facts to bottom in the transfer function.
      case _ => throw new RuntimeException(s"Unsupported join node type ${node.getClass.getSimpleName}")
    }
  }
}

/** Subclasses may override any of these methods as appropriate. */
trait DataflowGraphColumnAnalysisFunctions[E] {
  def transferSelect(s: Select, idx: Int, fact: E): E = fact
  def transferColumnReference(c: ColumnReference, idx: Int, fact: E): E = fact
  def transferFunction(f: Function, idx: Int, fact: E): E = fact
  def transferUnstructuredReference(u: UnstructuredReference, idx: Int, fact: E): E = fact
  def transferDataTable(d: DataTable, idx: Int, fact: E): E = fact
  def transferJoin(j: Join, idx: Int, fact: E): E = fact
  def transferUnion(u: Union, idx: Int, fact: E): E = fact
  def transferExcept(e: Except, idx: Int, fact: E): E = fact
}