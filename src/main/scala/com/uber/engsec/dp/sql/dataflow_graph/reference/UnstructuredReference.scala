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

package com.uber.engsec.dp.sql.dataflow_graph.reference

import com.facebook.presto.sql.tree.{Node => PrestoNode}
import com.uber.engsec.dp.sql.dataflow_graph.Node

/** An unstructured reference is a type of reference node that stores a relationship between children nodes without any
  * other semantic information. It is used as a "catch all" to represent SQL constructs that need no distinct
  * representation for our analyses but for which we wish to still capture a coarse data dependency between nodes.
  */
case class UnstructuredReference(refType: String, refChildren: List[Node] = Nil)(implicit override val prestoSource: Option[PrestoNode] = None)
  extends Reference(prestoSource) {

  override val children = refChildren

  override val nodeStr: String = refType

  override def toString: String = refType
}

object UnstructuredReference {
  def apply(refType: String, children: Node*)(implicit prestoSource: Option[PrestoNode]) = new UnstructuredReference(refType, children.toList)(prestoSource)
}