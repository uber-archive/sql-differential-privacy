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

/** Function: A SQL function application, e.g., COUNT(x). In the most general sense, this node type captures
  * any SQL construct that can be modeled as a function of one of more references (the references themselves being
  * perhaps subtrees).
  */
case class Function(functionName: String, args: List[Reference] = Nil)(implicit override val prestoSource: Option[PrestoNode] = None)
  extends Reference(prestoSource) {

  override val children = args

  override val nodeStr = functionName

  override def toString: String = functionName
}

object Function {
  def apply(functionName: String, arg: Reference)(implicit prestoSource: Option[PrestoNode]) = new Function(functionName, List(arg))(prestoSource)
  def apply(functionName: String, args: Reference*)(implicit prestoSource: Option[PrestoNode]) = new Function(functionName, args.toList)(prestoSource)
}