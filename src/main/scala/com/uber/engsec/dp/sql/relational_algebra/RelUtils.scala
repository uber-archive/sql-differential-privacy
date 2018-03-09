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

package com.uber.engsec.dp.sql.relational_algebra

import com.uber.engsec.dp.rewriting.rules.Expr.{ColumnReferenceByOrdinal, col}
import com.uber.engsec.dp.schema.{Database, Schema}
import org.apache.calcite.plan.hep.{HepPlanner, HepProgram}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Aggregate, Join, TableScan}
import org.apache.calcite.rel.rel2sql.RelToSqlConverter
import org.apache.calcite.rel.rules.FilterJoinRule
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.calcite.sql.SqlDialect.DatabaseProduct
import org.apache.calcite.sql.SqlKind

object RelUtils {
  /** Extracts the left and right column indexes, respectively, used in an equijoin condition, or None if the join node
    * uses any other type of join condition (including an empty join condition). Note the returned indexes are relative
    * to the schemas of the left/right relations rather than the schema of the join.
    *
    * @param node The join node
    * @param condition The clause of the join condition. If desired, caller can decompose AND-clauses in join condition
    *                  using the [decomposeConjunction] method, and call this method on each clause.
    * @return The indices of the equijoin columns, or None if clause is not equijoin.
    */
  def extractEquiJoinColumns(node: Join, condition: RexNode): Option[(Int,Int)] = {
    condition match {
      case c: RexCall if c.op.kind == SqlKind.EQUALS && c.operands.size == 2 =>

        val numColsLeft = node.getLeft.getRowType.getFieldCount

        (c.getOperands.get(0), c.getOperands.get(1)) match {
          case (first: RexInputRef, second: RexInputRef) =>
            val firstIdx = first.getIndex
            val secondIdx = second.getIndex

            if ((firstIdx < numColsLeft) && (secondIdx >= numColsLeft))
              Some((firstIdx, secondIdx-numColsLeft))
            else if ((secondIdx < numColsLeft) && (firstIdx >= numColsLeft))
              Some((secondIdx, firstIdx-numColsLeft))
            else
              None

          case _ => None
        }

      case _ => None
    }
  }

  /** Decomposes a given expression into a list of conjunctive clauses. */
  def decomposeConjunction(expression: RexNode): List[RexNode] = {
    import scala.collection.JavaConverters._

    if (expression == null || expression.isAlwaysTrue)
      Nil

    expression match {
      case c: RexCall if c.isA(SqlKind.AND) => c.getOperands.asScala.flatMap{ decomposeConjunction }.toList
      case _ => List(expression)
    }
  }

  /** Returns the grouped columns of an aggregation node
    *
    * @param agg The target aggregation node
    * @return A list of column expressions which reference each grouping column in the aggregation.
    */
  def getGroupedCols(agg: Aggregate): Seq[ColumnReferenceByOrdinal] = {
    import scala.collection.JavaConverters._
    agg.getGroupSet.asList.asScala.map { col(_) }
  }

  /** Converts the given relational algebra tree to a SQL string.
    */
  def relToSql(rel: RelNode, dialect: String): String = {
    val _dialect = DatabaseProduct.valueOf(dialect.toUpperCase).getDialect
    val converter = new RelToSqlConverter(_dialect)
    converter.visitChild(0, rel).asStatement.toSqlString(_dialect).getSql
  }

  /** Returns a new tree with filter predicates pushed down into join nodes (where possible).
    * For example, the tree representing the following query:
    *
    *   SELECT * FROM a JOIN b WHERE a.x = b.x
    *
    * would be transformed into:
    *
    *   SELECT * FROM a JOIN b ON a.x = b.x
    */
  def pushFiltersOnJoins(rel: RelNode): RelNode = {
    val program = HepProgram.builder.addRuleInstance(FilterJoinRule.FILTER_ON_JOIN).build
    val optPlanner = new HepPlanner(program)
    optPlanner.setRoot(rel)
    optPlanner.findBestExp
  }

  /******************************************************************************************************************
    * Helper methods, may be called by analyses and rewriters.
    ****************************************************************************************************************/

  /** Returns the fully qualified name of the table represented by the given TableScan node.
    */
  def getQualifiedTableName(node: TableScan): String = {
    import scala.collection.JavaConverters._
    node.getTable.getQualifiedName.asScala.mkString(".")
  }

  /** Retrieves the config properties for the database table represented by the given TableScan node.
    *
    * @param node Node representing target table.
    * @return Map of table properties, or empty map if no config is defined for the table.
    */
  def getTableProperties(node: TableScan, database: Database): Map[String, String] = {
    val tableName = getQualifiedTableName(node)
    Schema.getTableProperties(database, tableName)
  }

  /** Retrieves the config properties for a specific column in the given table.
    *
    * @param node Node representing target table.
    * @param colIdx Column ordinal in target table.
    * @return Map of column properties, or empty map if no config is defined for the column.
    */
  def getColumnProperties(node: TableScan, colIdx: Int, database: Database): Map[String, String] = {
    val tableName = RelUtils.getQualifiedTableName(node)
    val colName = node.getRowType.getFieldNames.get(colIdx)
    Schema.getSchemaMapForTable(database, tableName).get(colName).map { _.properties }.getOrElse{ Map.empty }
  }

  def getColumnProperty[T](propName: String, node: TableScan, colIdx: Int, database: Database): Option[T] = {
    val tableName = RelUtils.getQualifiedTableName(node)
    val colName = node.getRowType.getFieldNames.get(colIdx)
    Schema.getSchemaMapForTable(database, tableName).get(colName).get.get[T](propName)
  }
}
