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

import com.facebook.presto.sql.tree.{SingleColumn, Node => PrestoNode}
import com.uber.engsec.dp.sql.dataflow_graph.reference.Reference

/** Select: A relation created with SQL's SELECT. Note that in dataflow graphs, a select node has no "from" field,
  * as this information is explicitly encoded inside each column reference, which maintains a pointer to the node
  * to which it refers.
  */
case class Select(
    items: List[SelectItem],
    where: Option[Reference] = None,
    groupBy: List[Int] = Nil)
    (implicit override val prestoSource: Option[PrestoNode] = None)
  extends Relation(items.map{ _.as }.toIndexedSeq, prestoSource ) {

  override val children = items.map{ _.ref } ++ List(where).flatten ++ groupBy.map{ items(_).ref }

  override val nodeStr : String = ""

  override def toString: String = items.toString
}

/** A selection of a single column.
  */
case class SelectItem(as: String, ref: Reference, prestoSource: Option[SingleColumn] = None)
