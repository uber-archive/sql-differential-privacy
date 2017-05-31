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

/** Generic parent class for Reference nodes.
  *
  * Conceptually, a reference node captures a specific and well-defined data dependence into a relation, either by
  * direct column reference, e.g., "SELECT a.x from blah", or function application, e.g., "SELECT count(*) from blah".
  * In both examples, the part immediately after the SELECT is represented by a specific subclass of this class
  * which knows that it is executed w.r.t. relation "blah". For functions, this is tracked by the 'args' field; for
  * ColumnReference, it's tracked by the 'of' field.
  */
abstract class Reference(override val prestoSource: Option[PrestoNode]) extends Node(prestoSource)
