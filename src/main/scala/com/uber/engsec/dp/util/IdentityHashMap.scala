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

package com.uber.engsec.dp.util

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable

/** Identity hash map: compares objects by object ID, not value.
  */
final class IdentityHashMap[A <: AnyRef, B]() extends mutable.HashMap[A, B] with mutable.MapLike[A, B, IdentityHashMap[A, B]] {
  override protected def elemEquals(key1: A, key2: A): Boolean = key1 eq key2
  override protected def elemHashCode(key: A) = System.identityHashCode(key)
  override def empty: IdentityHashMap[A, B] = IdentityHashMap.empty
}

object IdentityHashMap {
  type Coll = IdentityHashMap[_, _]

  implicit def canBuildFrom[A <: AnyRef, B] = new CanBuildFrom[Coll, (A, B), IdentityHashMap[A, B]] {
    def apply() = newBuilder[A, B]
    def apply(from: Coll) = {
      val builder = newBuilder[A, B]
      builder.sizeHint(from.size)
      builder
    }
  }

  def empty[A <: AnyRef, B]: IdentityHashMap[A, B] = new IdentityHashMap[A, B]

  def newBuilder[A <: AnyRef, B] = new mutable.MapBuilder[A, B, IdentityHashMap[A, B]](empty[A, B]) {
    override def +=(x: (A, B)): this.type = {
      elems += x
      this
    }
    override def sizeHint(size: Int): Unit = elems.sizeHint(size)
  }

  def apply[A <: AnyRef, B](elems: (A, B)*) = (newBuilder[A, B] ++= elems).result()
}