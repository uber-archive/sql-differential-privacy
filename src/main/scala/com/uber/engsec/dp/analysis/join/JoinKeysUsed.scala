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

package com.uber.engsec.dp.analysis.join

import com.uber.engsec.dp.dataflow.node.DFGVisitorAnalysis
import com.uber.engsec.dp.sql.dataflow_graph.Node
import com.uber.engsec.dp.sql.dataflow_graph.reference.{ColumnReference, Function}
import com.uber.engsec.dp.sql.dataflow_graph.relation.{DataTable, Join}

import scala.collection.mutable

/** Analysis that returns the set of all columns used as equi-join keys in a given query */
class JoinKeysUsed extends DFGVisitorAnalysis[JoinKeyDomain] {

  override def run(node: Node): JoinKeyDomain = {

    val state = new JoinKeyDomain()

    node.foreach {
      case d: DataTable =>
        state.tables.add(d)

      case c: ColumnReference =>
        state.tables.foreach { table => state.refs.add(table.name + "." + table.getColumnName(c.colIndex)) }
        state.tables.clear()

      case f: Function =>
        if (f.functionName == "EQUAL") {
          state.refs.foreach { state.eqKeys.add }
          state.refs.clear()
        }

      case j: Join =>
        state.eqKeys.foreach { state.joinKeys.add }
        state.eqKeys.clear()

      case _ => ()
    }

    state
  }
}

/** Abstract domain for the join keys used analysis */
class JoinKeyDomain {
  val joinKeys = new mutable.HashSet[String]()
  val refs = new mutable.HashSet[String]
  val eqKeys = new mutable.HashSet[String]
  val tables = new mutable.HashSet[DataTable]
}