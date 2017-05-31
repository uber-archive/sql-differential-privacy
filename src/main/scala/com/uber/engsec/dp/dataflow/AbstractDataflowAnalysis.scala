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

package com.uber.engsec.dp.dataflow

import com.uber.engsec.dp.sql.AbstractAnalysis

/** Common trait of dataflow analyses across different representations (AST, dataflow graph, and relational algebra)
  */
trait AbstractDataflowAnalysis[N <: AnyRef, T1] extends AbstractAnalysis[N, T1] {
  /** Invokes the transfer function of the analysis implementation and returns a new abstract state. Implemented by
    * analysis type subclasses.
    */
  def transferNode(node: N, state: T1): T1

  /** Returns the children of the given node. Implemented by analysis type subclasses.
    */
  def getNodeChildren(node: N): Iterable[N]

  /** Invokes the join function of the analysis implementation and returns a new abstract state. Implemented by
    * analysis type subclasses.
    */
  def joinNode(node: N, children: Iterable[N]): T1

  /** Recursive procedure to visit nodes in the tree and invoke analysis transfer/join methods.
    */
  def process(node: N): Unit = {
    if (resultMap.contains(node))
      return

    val children = getNodeChildren(node)
    children.foreach { process }

    currentNode = Some(node)

    val joinResult = joinNode(node, children)
    val transferResult = transferNode(node, joinResult)
    resultMap += (node -> transferResult)
  }
}
