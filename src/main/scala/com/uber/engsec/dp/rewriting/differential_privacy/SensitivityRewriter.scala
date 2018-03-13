package com.uber.engsec.dp.rewriting.differential_privacy

import com.uber.engsec.dp.analysis.histogram.HistogramAnalysis
import com.uber.engsec.dp.dataflow.domain.UnitDomain
import com.uber.engsec.dp.rewriting.rules.ColumnDefinition._
import com.uber.engsec.dp.rewriting.rules.Operations._
import com.uber.engsec.dp.rewriting.{DPRewriterConfig, DPUtil, Rewriter}
import com.uber.engsec.dp.sql.relational_algebra.Relation
import org.apache.calcite.rel.core.Aggregate


/** Parent class for sensitivity-based mechanisms, which add Laplace noise scaled to each output column's sensitivity.
  * Each mechanism has a specific way of computing the scale of this noise for its supported class of queries.
  *
  * See [ElasticSensitivityRewriter] and [RestrictedSensitivityRewriter].
  */
abstract class SensitivityRewriter[C <: DPRewriterConfig](config: C) extends Rewriter(config) {

  /** Returns the scale of Laplace noise required for the given column as defined by the mechanism. Implemented by subclasses. */
  def getLaplaceNoiseScale(node: Relation, colIdx: Int): Double

  def rewrite(root: Relation): Relation = {
    root.rewriteRecursive(UnitDomain) { (node, orig, _) =>
      node match {
        case Relation(a: Aggregate) =>
          // For histogram queries, ensure all values from domain appear in result set
          val withFilledBins =
            if (a.getGroupCount > 0) {
              val histogramResults = new HistogramAnalysis().run(node, config.database).colFacts
              DPUtil.addBinsFromDomain(a, histogramResults, config)
            }
            else Relation(a)

          val result = withFilledBins.mapCols { col =>
            // Compute the scale of Laplace noise for the column.
            val laplaceNoiseScale = getLaplaceNoiseScale(node, col.idx)

            if (laplaceNoiseScale == 0)
            // No noise added to histogram bins that are marked safe for release.
              col
            else {
              // Rewrite the column expression to add scaled Laplace noise.
              val noiseExpr = laplaceNoiseScale * DPUtil.LaplaceSample
              (col.expr + noiseExpr) AS col.alias
            }
          }

          (result, ())

        case _ => (node, ())
      }
    }
  }
}
