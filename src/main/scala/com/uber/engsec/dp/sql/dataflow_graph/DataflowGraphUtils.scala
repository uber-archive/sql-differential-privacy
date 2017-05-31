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

import com.uber.engsec.dp.sql.dataflow_graph.reference.{ColumnReference, Function}
import com.uber.engsec.dp.sql.dataflow_graph.relation.Join

object DataflowGraphUtils {
  /** Extracts the left and right column indexes, respectively, used in an equijoin condition, or None if
    * the join node uses any other type of join condition (including an empty join condition).
    */
  def extractEquiJoinColumns(node: Join): Option[(Int,Int)] = {
    node.condition.collect {
      case Function("EQUAL", ColumnReference(leftIdx, node.left) :: ColumnReference(rightIdx, node.right) :: Nil) => (leftIdx, rightIdx)
      case Function("EQUAL", ColumnReference(rightIdx, node.right) :: ColumnReference(leftIdx, node.left) :: Nil) => (leftIdx, rightIdx)
    }
  }
}
