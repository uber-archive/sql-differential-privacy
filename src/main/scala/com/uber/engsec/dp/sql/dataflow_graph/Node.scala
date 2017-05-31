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

package com.uber.engsec.dp.sql.dataflow_graph

import com.facebook.presto.sql.tree.{Node => PrestoNode}

import scala.collection.mutable

/** A dataflow graph is a custom representation of a SQL query where all data dependencies are explicitly expressed via
  * graph edges. It can be used as the basis for both column-based and relation-based dataflow analyses.
  *
  * This class is the parent type of all dataflow graph nodes.
  */
abstract class Node(val prestoSource: Option[PrestoNode]) extends Traversable[Node] {

  val nodeStr: String
  val children: List[Node]

  /** Implementing the foreach method from Traversable gives access to many useful higher-order functions on dataflow
    * graphs including fold*, reduce*, exists, collect, etc.
    *
    * Since dataflow graphs may contain cycles, our implementation of foreach must keep track of which children nodes
    * have been traversed already.
    */
  override def foreach[U](f: Node => U) = _foreach(f, new mutable.HashSet())
  private def _foreach[U](f: Node => U, visited: mutable.HashSet[Node]): Unit = {
    if (visited.contains(this))
      return
    visited += this

    f(this)
    children.foreach {
      _._foreach(f, visited)
    }
  }

  /** Optimized version of some traversable methods.
    */
  override def isEmpty: Boolean = false
  override def head: Node = this
  // tail is inherited from TraversableLike (and implemented using foreach)

  override val hashCode: Int = super.hashCode

  // We override the equals method to ensure reference equality (by default, Scala uses structural equality for case classes)
  override def equals(that: Any): Boolean =
    that match {
      case ref: AnyRef => this eq ref
      case _ => false
    }
}
