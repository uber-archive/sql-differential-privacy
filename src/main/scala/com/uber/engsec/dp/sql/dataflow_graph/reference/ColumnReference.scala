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
import com.uber.engsec.dp.sql.dataflow_graph.relation.Relation

/** ColumnReference: A column reference is a node that reads a specific column (referenced by zero-indexed ordinal) from
  * a relation. For example, if my_table has columns [foo, bar, baz] then for query "SELECT baz from my_table", the
  * dataflow graph includes a ColumnReference node with .of pointing to relation DataTable[my_table] and .colIndex = 2.
  *
  * If you want to know the name of the column, you can ask the relation, e.g., this.of.getColumnName(this.colIndex) but
  * be aware a column reference is not uniquely defined by the column name since relations in a dataflow graph can
  * have more than one column the same name.
  */
case class ColumnReference(colIndex: Int, of: Relation)(implicit override val prestoSource: Option[PrestoNode] = None)
  extends Reference(prestoSource) {

  override val children = List( of )

  override val nodeStr = colIndex.toString

  override def toString: String = s"${of.toString}.${colIndex.toString}"
}