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

package com.uber.engsec.dp.dataflow.domain.lattice

import com.uber.engsec.dp.dataflow.domain._

/** Models a flat lattice of a finite set of elements of type [E]:
  *
  *       ⊤
  *    /  |  \
  *   /   |   \
  *  e1   e2  ...
  *   \   |   /
  *    \  |  /
  *       ⊥
  */
class FlatLatticeDomain[E] extends AbstractDomain[DomainElem[E]] {
  override val bottom: DomainElem[E] = Bottom
  override def leastUpperBound(first: DomainElem[E], second: DomainElem[E]): DomainElem[E] = FlatLatticeDomain.leastUpperBound(first, second)
}

object FlatLatticeDomain {
  def bottom[E]: DomainElem[E] = Bottom

  def leastUpperBound[E](first: DomainElem[E], second: DomainElem[E]): DomainElem[E] = {
    (first, second) match {
      case (Top, _) | (_, Top) => Top
      case (Bottom, _) => second
      case (_, Bottom) => first
      case (Mid(a), Mid(b)) => if (a == b) first else Top
      case _ => Top
    }
  }
}