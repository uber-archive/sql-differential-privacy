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
import com.uber.engsec.dp.exception.AmbiguousColumnReference
import com.uber.engsec.dp.sql.dataflow_graph.reference.Reference

/** A relation created by JOIN.
  */
object JoinType {
  sealed trait JoinType
  case object CROSS extends JoinType
  case object INNER extends JoinType
  case object LEFT extends JoinType
  case object RIGHT extends JoinType
  case object FULL extends JoinType

  def parse(name: String): JoinType = {
    name match {
      case "CROSS" => CROSS
      case "INNER" => INNER
      case "LEFT" => LEFT
      case "RIGHT" => RIGHT
      case "FULL" => FULL
      case "IMPLICIT" => CROSS  // implicit joins are effectively cross joins
      case _ => throw new IllegalArgumentException(s"Unknown join type: $name")
    }
  }
}

case class Join(
    left: Relation,
    right: Relation,
    joinType: JoinType.JoinType,
    condition: Option[Reference] = None)
    (implicit override val prestoSource: Option[PrestoNode] = None)
  extends Relation(left.columnNames ++ right.columnNames, prestoSource) {

  val children = List(left, right) ++ (if (condition.isDefined) List(condition.get) else Nil)

  /** Returns the column index for the column appearing in the *specified* inner relation. This is a substitute for the
    * Relation.getColumnIndex method for cases where the select item references a joined table by name. For example,
    * if table1 and table2 both have a column "uuid", this method can be used to resolve the correct index for the
    * query:  SELECT table2.uuid from table1 JOIN table2 ...
    *
    * Returns the index of the specific column, or None if the column doesn't exist.
    */
  def getColumnIndexForInnerRelation(colName: String, innerRelation: Relation): Option[Int] = {

    def visitRelation(indexSoFar: Int, relation: Relation): Option[Int] =

      if (relation == innerRelation) {
        // We found the specified inner relation. Return.
        val result = relation.getColumnIndexes(colName)
        result.size match {
          case 0 => None  // Column not found
          case 1 => Some(indexSoFar + result(0))
          case _ => throw new AmbiguousColumnReference("Relation " + this.toString + " has more than one column named " + colName)
        }

      } else {
        relation match {
          case join: Join =>
            // We are processing a Join node. Call the visitRelation method recursively on both the left and right relations
            val leftResult = visitRelation(indexSoFar, join.left)
            val rightResult = visitRelation(indexSoFar + join.left.numCols, join.right)

            (leftResult, rightResult) match {
              case (Some(a), None) => Some(a)  // we found the column in .left
              case (None, Some(b)) => Some(b)  // we found the column in .right
              case (None, None) => None
              case (Some(a), Some(b)) =>
                if (join.left == join.right) // a table is joined with itself (so the graph node is shared, and both sides match). Return the left one; it doesn't matter.
                  Some(a)
                else  // This should never happen
                  throw new AmbiguousColumnReference("Children of relation " + this.toString + " both match target inner relation node and have a column named " + colName)
            }

          case _ => None
        }
      }

    visitRelation(0, this)
  }

  override val nodeStr = joinType.toString
  override def toString = joinType.toString + " JOIN"
}