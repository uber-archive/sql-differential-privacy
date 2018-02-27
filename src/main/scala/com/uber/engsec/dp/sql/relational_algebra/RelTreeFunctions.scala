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

import com.uber.engsec.dp.schema.Database
import com.uber.engsec.dp.sql.{AbstractAnalysis, QueryParser, TreeFunctions, TreePrinter}
import org.apache.calcite.rel.core._
import org.apache.calcite.rex._

import scala.collection.JavaConverters._

/** Common trait for analyses on relational algebra trees.
  */
trait RelTreeFunctions extends TreeFunctions[RelOrExpr] {
  this: AbstractAnalysis[RelOrExpr, _] =>
  override def getNodeChildren(node: RelOrExpr): Iterable[RelOrExpr] = RelTreeFunctions.getChildren(node)

  override def parseQueryToTree(query: String, database: Database): RelOrExpr = {
    QueryParser.parseToRelTree(query, database)
  }

  override def printTree(node: RelOrExpr): Unit = TreePrinter.printRelTree(node, resultMap, currentNode)
}

object RelTreeFunctions {
  def getChildren(node: RelOrExpr): Iterable[RelOrExpr] = node match {
    case Relation(p: Project) =>  Relation(p.getInput) :: p.getProjects.asScala.map{Expression}.toList
    case Relation(a: Aggregate) => List(a.getInput)
    case Relation(t: TableScan) => Nil
    case Relation(j: Join) => j.getInputs.asScala.map{Relation} ++ List(Expression(j.getCondition))
    case Relation(c: Correlate) => c.getInputs.asScala.map{Relation}
    case Relation(f: Filter) => Relation(f.getInput) :: Expression(f.getCondition) :: Nil
    case Relation(s: Sort) => (Relation(s.getInput) :: Expression(s.fetch) :: Expression(s.offset) :: Nil).filter{ _.unwrap != null }
    case Relation(v: Values) => Nil
    case Relation(u: SetOp) => u.getInputs.asScala.map{Relation}

    case Expression(c: RexCall) => c.operands.asScala
    case Expression(i: RexInputRef) => Nil
    case Expression(l: RexLiteral) => Nil
    case Expression(f: RexFieldAccess) => List(f.getReferenceExpr)
    case Expression(c: RexCorrelVariable) => Nil
    case Expression(e) => throw new RuntimeException("Unimplemented: " + e.getClass.getSimpleName)
    case Relation(e) => throw new RuntimeException("Unimplemented: " + e.getClass.getSimpleName)
  }
}