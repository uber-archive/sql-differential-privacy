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

/** An abstract domain that implements a Map of facts, with join defined as map union. If A and B have intersecting
  * keys, behavior is undefined (only one value for each key is retained).
  */
class MapDomain[K,V] extends AbstractDomain[Map[K,V]] {
  override val bottom: Map[K,V] = Map.empty
  override def leastUpperBound(first: Map[K, V], second: Map[K, V]): Map[K, V] = first ++ second
}

/** An abstract domain that implements a Set of facts, with leastUpperBound defined as set union.
  */
class SetDomain[T] extends AbstractDomain[Set[T]] {
  override val bottom: Set[T] = Set.empty
  override def leastUpperBound(first: Set[T], second: Set[T]): Set[T] = first ++ second
}
