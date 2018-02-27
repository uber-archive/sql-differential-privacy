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

package com.uber.engsec.dp.sql.dataflow_graph.relation

import com.facebook.presto.sql.tree.{Node => PrestoNode}
import com.uber.engsec.dp.schema.{Database, Schema}

/** A DataTable is a leaf node of that represents a table in the database.
  */
case class DataTable(
    name: String,
    database: Database,
    override val columnNames: IndexedSeq[String])
    (implicit override val prestoSource: Option[PrestoNode] = None)
  extends Relation(columnNames, prestoSource ) {

  override val children = Nil

  override val nodeStr: String = "\"" + name + "\""

  /** Metadata properties (from the schema config file) for the columns in this table.
    */
  lazy val colProperties: IndexedSeq[Map[String,String]] = {
    val colMap = Schema.getSchemaMapForTable(database, name)
    columnNames.map { colName => colMap.get(colName).fold(Map.empty[String,String])(_.properties) }
  }

  override def toString: String = name
}

