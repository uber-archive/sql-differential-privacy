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

import com.uber.engsec.dp.rewriting._
import com.uber.engsec.dp.schema.Database
import com.uber.engsec.dp.sql.relational_algebra.Relation
import com.uber.engsec.dp.util.ElasticSensitivity

/** Rewriter that enforces differential privacy using Elastic Sensitivity. */
class ElasticSensitivityRewriter(config: ElasticSensitivityConfig) extends SensitivityRewriter(config) {
  def getLaplaceNoiseScale(node: Relation, colIdx: Int): Double =
    2 * ElasticSensitivity.smoothElasticSensitivity(node, config.database, colIdx, config.epsilon, config.delta) / config.epsilon
}

class ElasticSensitivityConfig(
    override val epsilon: Double,
    val delta: Double,
    override val database: Database,
    override val fillMissingBins: Boolean = true)
  extends DPRewriterConfig(epsilon, database, fillMissingBins)
