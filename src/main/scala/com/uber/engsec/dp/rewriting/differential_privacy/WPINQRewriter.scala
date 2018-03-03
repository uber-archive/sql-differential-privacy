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

package com.uber.engsec.dp.rewriting.differential_privacy

import com.uber.engsec.dp.analysis.histogram.{HistogramAnalysis, QueryType}
import com.uber.engsec.dp.dataflow.AggFunctions.COUNT
import com.uber.engsec.dp.dataflow.domain.UnitDomain
import com.uber.engsec.dp.exception.UnsupportedQueryException
import com.uber.engsec.dp.rewriting.rules.ColumnDefinition._
import com.uber.engsec.dp.rewriting.rules.Expr.{col, _}
import com.uber.engsec.dp.rewriting.rules.Operations._
import com.uber.engsec.dp.rewriting.rules._
import com.uber.engsec.dp.rewriting.{DPRewriterConfig, DPUtil, Rewriter}
import com.uber.engsec.dp.schema.Database
import com.uber.engsec.dp.sql.relational_algebra.{RelUtils, Relation}
import org.apache.calcite.rel.core._

/**
  * Rewriter for WPINQ. Converts a SQL counting query into a query that returns a noisy count of weights.
  *
  * @see [[https://arxiv.org/abs/1203.3453 Calibrating Data to Sensitivity in Private Data Analysis]]
  */
class WPINQRewriter(config: WPINQConfig) extends Rewriter(config) {
  def rewrite(root: Relation): Relation = {
    // Reject unsupported queries
    val histogramResults = new HistogramAnalysis().runAll(root, config.database)
    val queryType = QueryType.getQueryType(histogramResults(root))

    val isValidQueryType =
      Set(QueryType.HISTOGRAM, QueryType.NON_HISTOGRAM_STATISTICAL).contains(queryType) &&
        histogramResults(root).colFacts.filter(_.isAggregation).forall(_.outermostAggregation.contains(COUNT))

    if (!isValidQueryType) throw new UnsupportedQueryException("This rewriter only works on counting queries")

    val joinNodes = root.collect{ case Relation(j: Join) => j }
    if (joinNodes.exists{ join => RelUtils.extractEquiJoinColumns(join, join.getCondition).isEmpty })
      throw new UnsupportedQueryException("This rewriter only works on queries with equijoins")

    root.rewriteRecursive(UnitDomain) { (node, orig, _) =>
      node match {
        // Add initial weight column to tables.
        case Relation(tbl: TableScan) => (node.project(*, (config.initialWeights(tbl): ValueExpr) AS "_weight"), ())

        // Ensure the weight column is projected through project nodes.
        case Relation(p: Project) => (p.reproject(*, col("_weight")), ())

        case Relation(j: Join) =>
          val (leftJoinCol, rightJoinCol) = RelUtils.extractEquiJoinColumns(j, j.getCondition).getOrElse(throw new UnsupportedQueryException("This rewriter only supports equijoin conditions."))

          val A = Relation(j.getLeft).rename(col("_weight") AS "_A_w").asAlias("_A")
          val B = Relation(j.getRight).rename(col("_weight") AS "_B_w").asAlias("_B")

          val Ak = A.agg (col(leftJoinCol)) (Sum(col("_A_w")) AS "_A_s")
          val Bk = B.agg (col(rightJoinCol)) (Sum(col("_B_w")) AS "_B_s")

          val newNode = node
            .replaceInputs(_ => List(A, B))
            .join(Ak, left(leftJoinCol) == right(0))
            .join(Bk, left(leftJoinCol) == right(0))
            .project(*, ((col("_A_w") * col("_B_w")) / (col("_A_s") + col("_B_s"))) AS "_weight")
            .remove(col("_A_w"), col("_B_w"), col("_A_s"), col("_B_s"))

          (newNode, ())

        case Relation(a: Aggregate) =>
          val groupedCols = RelUtils.getGroupedCols(a)
          val origColName = a.getRowType.getFieldNames.get(groupedCols.length)
          val weightSumRelation = Relation(a.getInput).agg (groupedCols: _*) (Sum(col("_weight")) AS "_weight_sum")

          // For histogram queries, ensure all values from domain appear in result set, assigning weighted sum 0 to
          // absent bins.
          val withFilledBins = DPUtil.addBinsFromDomain(weightSumRelation.unwrap.asInstanceOf[Aggregate], histogramResults(orig).colFacts, config)

          // Add noise to weights
          val result = withFilledBins
            .project(*, col("_weight_sum") + (1.0 / config.epsilon) * DPUtil.LaplaceSample AS origColName)
            .remove(col("_weight_sum"))

          (result, ())

        case _ => (node, ())
      }
    }
  }
}

class WPINQConfig(
    override val epsilon: Double,
    override val database: Database,
    override val fillMissingBins: Boolean = true)
  extends DPRewriterConfig(epsilon, database, fillMissingBins) {
  /** The initial weight assigned to each record in the given table. Default is 1.0. */
  def initialWeights(table: TableScan): Double = 1.0
}