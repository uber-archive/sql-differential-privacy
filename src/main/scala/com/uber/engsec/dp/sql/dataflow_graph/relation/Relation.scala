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
import com.uber.engsec.dp.sql.dataflow_graph.Node

/** Generic parent class for all relations in dataflow graphs, which includes: the result of the entire query, named
  * tables in the database, and subqueries.
  */
abstract class Relation(
    val columnNames: IndexedSeq[String],  // Ordered list of columns in this relation. We use IndexedSeq rather
                                          // than List to ensure fast lookups by index, an operation performed
                                          // frequently by analyses.
    override val prestoSource: Option[PrestoNode] = None)
  extends Node(prestoSource) {
  /** Optimization: because we frequently perform lookups by column name, we maintain a map to do the lookup
    * without having to loop over the list.
    *
    * Note that in Vertica, column references by name are case-insensitive, i.e., the following queries are valid:
    *   WITH t1 as (SELECT a as BLAH) select blah from t1"
    *   WITH t1 as (SELECT a as BLAH) select Blah from t1"
    *  so although we preserve case in the schema because it determines output column names, we perform name-to-index
    *  lookups without considering case.
    */
  private val colIndexMap: Map[String, List[Int]] =
    columnNames.map{ _.toUpperCase }
      .zipWithIndex
      .groupBy(_._1)
      .map { case (k,v) => (k,v.map(_._2).toList) }

  /** Returns the number of columns in this relation.
    */
  final def numCols: Int = columnNames.size

  /** Returns the index(es) of the column(s) with the given name, or Nil if this relation does not contain the given column.
    */
  def getColumnIndexes(colName: String): List[Int] = colIndexMap.getOrElse(colName.toUpperCase, Nil)

  /** Returns the name of the column at the given ordinal.
    */
  def getColumnName(index: Int): String = columnNames(index)
}
