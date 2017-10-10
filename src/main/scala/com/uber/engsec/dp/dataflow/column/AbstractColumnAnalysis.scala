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

import com.uber.engsec.dp.dataflow.AbstractDataflowAnalysis
import com.uber.engsec.dp.dataflow.column.AbstractColumnAnalysis.ColumnFacts
import com.uber.engsec.dp.dataflow.domain.AbstractDomain

/** Tracks dataflow facts (abstract domains) individually for each column, automatically propagating
  * facts up the tree by figuring out which columns in a relation/reference correspond to which columns of its
  * subrelations. In other words, this analysis tracks data provenance automatically so subclasses need only define
  * methods for updating these facts at appropriate nodes.
  *
  * @tparam N The tree node type
  * @tparam E The result fact type
  * @tparam D The abstract domain for the analysis (i.e., lattice with element type E)
  */
abstract class AbstractColumnAnalysis[N <: AnyRef, E, D <: AbstractDomain[E]]
  extends AbstractDataflowAnalysis[N, ColumnFacts[E]] {

  def flattenJoinChildren(domain: AbstractDomain[E], node: N, children: Iterable[N]): ColumnFacts[E] = {
    val childrenFacts = children.flatMap{ resultMap(_) }
    val resultFacts = AbstractColumnAnalysis.joinFacts(domain, childrenFacts)
    IndexedSeq(resultFacts)
  }

  /** Implemented by analysis subclasses.
    */
  override def transferNode(node: N, state: ColumnFacts[E]): ColumnFacts[E]
  override def joinNode(node: N, children: Iterable[N]): ColumnFacts[E]
}

object AbstractColumnAnalysis {
  import scala.language.implicitConversions

  type ColumnFacts[+J] = IndexedSeq[J]
  implicit def elemListToColumnFacts[J](elems: List[J]): ColumnFacts[J] = elems.toIndexedSeq
  implicit def elemsToColumnFacts[J](elems: J*): ColumnFacts[J] = elems.toIndexedSeq
  implicit def elemToColumnFacts[J](elem: J): ColumnFacts[J] = IndexedSeq(elem)

  def joinFacts[E](domain: AbstractDomain[E], facts: Iterable[E]): E = {
    val resultFact: E =
      if (facts.isEmpty)
        domain.bottom
      else if (facts.size == 1)
        facts.head
      else
        facts.reduce( (first, second) => domain.leastUpperBound(first, second) )

    resultFact
  }
}