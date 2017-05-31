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

package com.uber.engsec.dp.dataflow.domain

/** Models a domain lattice whose elements are represented by type E and partial order is defined (implicitly) by the
  * leastUpperBound method.
  *
  * This is the common interface for abstract domains, which store a particular type of dataflow fact for an analysis.
  * A dataflow analysis updates this abstract state by modeling the semantics of nodes in the tree with respect to the
  * domain of choice.
  *
  * Each abstract domain must implement a leastUpperBound operation that computes (or at minimum, over-approximates) the
  * lowest domain element that is greater than both input elements per to the domain's partial order. This method
  * is the means by which the analysis framework conservatively combines multiple states at branches in the tree.
  */
trait AbstractDomain[E] {
  /** The bottom element for this domain.
    */
  val bottom: E

  /** The least upper bound of elements a and b as defined by the partial order of this abstract domain.
    */
  def leastUpperBound(a: E, b: E): E
}
