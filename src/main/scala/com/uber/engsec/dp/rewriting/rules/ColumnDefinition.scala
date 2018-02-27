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

package com.uber.engsec.dp.rewriting.rules

import com.uber.engsec.dp.dataflow.column.AbstractColumnAnalysis.ColumnFacts
import com.uber.engsec.dp.dataflow.column.NodeColumnFacts
import com.uber.engsec.dp.rewriting.rules.Expr.ColumnReferenceByName
import com.uber.engsec.dp.sql.relational_algebra.{Relation, Transformer}
import org.apache.calcite.rel.logical.{LogicalProject, LogicalValues}
import org.apache.calcite.tools.Frameworks

/** Methods for specifying column references and definitions in rewriting operations. */

class ColumnDefinition[+T <: Expr](val expr: T)
case class ColumnDefinitionWithAlias[+T <: Expr](override val expr: T, alias: String) extends ColumnDefinition[T](expr)
case class ColumnDefinitionWithOrdinal[+T <: Expr](override val expr: T, alias: String, idx: Int) extends ColumnDefinition[T](expr)

object ColumnDefinition {
  import scala.collection.JavaConverters._
  import scala.language.implicitConversions

  // Automatically cast to column if alias is attached to an expression
  implicit class ExprColumnAlias[T <: Expr](expr: T) {
    def AS(alias: String): ColumnDefinitionWithAlias[T] = ColumnDefinitionWithAlias[T](expr, alias)
    def AS(alias: ColumnReferenceByName): ColumnDefinitionWithAlias[T] = ColumnDefinitionWithAlias[T](expr, alias.name)
  }

  // Allow renaming of a column (keeping the same expression)
  implicit class ColumnAlias[T <: Expr](col: ColumnDefinition[T]) {
    def AS(alias: String): ColumnDefinitionWithAlias[T] = ColumnDefinitionWithAlias[T](col.expr, alias)
  }

  // Allow easy lookup of the column fact from an analysis result
  implicit class ColumnFactLookup[F](results: ColumnFacts[F]) {
    def apply[T <: Expr](col: ColumnDefinitionWithOrdinal[T]): F = results(col.idx)
  }
  implicit class NodeColumnFactLookup[F](results: NodeColumnFacts[_,F]) {
    def apply[T <: Expr](col: ColumnDefinitionWithOrdinal[T]): F = results.colFacts(col.idx)
  }

  // Creates a relation from a list of column definitions
  def rel(cols: ColumnDefinition[Expr]*): Relation = columnDefsToRelation(cols)
  implicit def columnDefsToRelation(cols: Seq[ColumnDefinition[Expr]]): Relation = {
    val cluster = new Transformer(
      Frameworks.newConfigBuilder
        .defaultSchema(Frameworks.createRootSchema(true))
        .build
    ).cluster

    val inputRel = LogicalValues.createOneRow(cluster)
    val projections = cols.map{ _.expr.toRex(Relation(inputRel)) }
    val rowType = Helpers.getRecordType( cols.zip(projections) )
    val result = LogicalProject.create(inputRel, projections.asJava, rowType)
    Relation(result)
  }

  implicit def columnReferenceToColumnDefinitionWithName(col: ColumnReferenceByName): ColumnDefinitionWithAlias[ColumnReferenceByName] = ColumnDefinitionWithAlias[ColumnReferenceByName](col, col.name)
  implicit def columnDefinitionWithAliasToColumnReferenceByName[T <: Expr](col: ColumnDefinitionWithAlias[T]): ColumnReferenceByName = Expr.col(col.alias)
  implicit def exprToColumnDefinition[T <: Expr](expr: T): ColumnDefinition[T] = new ColumnDefinition(expr)
}
