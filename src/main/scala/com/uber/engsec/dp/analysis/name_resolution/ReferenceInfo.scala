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

package com.uber.engsec.dp.analysis.name_resolution

import com.facebook.presto.sql.tree.{Node, Query}
import com.uber.engsec.dp.exception.TransformationException

/** Stores information about references into relations as needed by TreeTransformAnalysis.
  */
class RefOption(val first: Node, val second: Option[Node] = None) {
  def isUnique: Boolean = !hasTwoRelations
  def hasTwoRelations: Boolean = second.isDefined

  def getOnly: Node = {
    if (hasTwoRelations) throw new TransformationException("getOnly called on reference with multiple possible relations.")
    first
  }
}

case class ReferenceInfo(relation1: Node,
                         relation2: Option[Node] = None,
                         // The node representing the "inner relation" being referenced, or None if the reference does not
                         // specify an inner relation. See comments in NameResolutionDomain for details about inner relation references.
                         var innerRelation: Option[Node] = None) {

  /** The presto node representing the relation being referenced into. This is usually a single relation, but it may
    * include two relations if the reference might point to either of them and must be resolved using schema
    * information. For example, in query
    *
    *    SELECT blah from a JOIN b ON col1 = col2
    *
    * both col1 and col2 may refer to either a or b, and we can only determine which one it is by consulting the schema
    * (which is only available during tree transformation).
    */
  val ref = new RefOption(relation1, relation2)

  override def toString: String = {
    def node2Str(node: Node): String = node match {
      case _ : Query => "Query"
      case _ => node.toString
    }

    var refStr = ""
    if (ref.second.isDefined)
      refStr = "Refs[1:" + node2Str(ref.first) + ", 2:" + node2Str(ref.second.get) + "]"
    else refStr = "Ref[" + node2Str(ref.first) + "]"

    refStr += (if (innerRelation.isEmpty) "" else " InnerRelation[" + node2Str(innerRelation.get) + "]")
    refStr
  }
}