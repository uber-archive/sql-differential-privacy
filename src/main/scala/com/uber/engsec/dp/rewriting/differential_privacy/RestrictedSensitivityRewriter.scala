package com.uber.engsec.dp.rewriting.differential_privacy

import com.uber.engsec.dp.analysis.differential_privacy.RestrictedSensitivityAnalysis
import com.uber.engsec.dp.rewriting.DPRewriterConfig
import com.uber.engsec.dp.schema.Database
import com.uber.engsec.dp.sql.relational_algebra.Relation

/** Rewriter that enforces differential privacy using Restricted Sensitivity. */
class RestrictedSensitivityRewriter(config: RestrictedSensitivityConfig) extends SensitivityRewriter(config) {
  def getLaplaceNoiseScale(node: Relation, colIdx: Int): Double =
    new RestrictedSensitivityAnalysis().run(node, config.database).colFacts(colIdx).sensitivity.get / config.epsilon
}

class RestrictedSensitivityConfig(
    override val epsilon: Double,
    override val database: Database,
    override val fillMissingBins: Boolean = true)
  extends DPRewriterConfig(epsilon, database, fillMissingBins)