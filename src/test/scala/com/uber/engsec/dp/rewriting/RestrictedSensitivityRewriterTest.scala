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

package com.uber.engsec.dp.rewriting

import com.uber.engsec.dp.rewriting.differential_privacy.{RestrictedSensitivityConfig, RestrictedSensitivityRewriter}
import com.uber.engsec.dp.schema.Schema
import com.uber.engsec.dp.sql.QueryParser
import junit.framework.TestCase

class RestrictedSensitivityRewriterTest extends TestCase {
  val database = Schema.getDatabase("test")

  def checkResult(query: String, epsilon: Double, expected: String, fillMissingBins: Boolean = false): Unit = {
    val root = QueryParser.parseToRelTree(query, database)
    val config = new RestrictedSensitivityConfig(epsilon, database, fillMissingBins)
    val result = new RestrictedSensitivityRewriter(config).run(root)
    TestCase.assertEquals(expected.stripMargin.stripPrefix("\n"), result.toSql())
  }

  def testSimpleHistogram() {
    val query = "SELECT order_date, COUNT(*) FROM orders GROUP BY 1"

    // Sensitivity of this query is 2.0 so scale of Laplace noise for epsilon 0.1 is (2/0.1) = 20
    checkResult(query, 0.1, """
      |SELECT order_date, COUNT(*) + 20.0 * (CASE WHEN RAND() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(RAND() - 0.5)))
      |FROM public.orders
      |GROUP BY order_date"""
    )
  }

  /** Restricted Sensitivity rewriter uses the same code as Elastic Sensitivity rewriter, the only difference being
    * the call to the sensitivity calculation analysis. Hence, see [ElasticSensitivityRewriterTest] and
    * [RestrictedSensitivityAnalysis] for additional test cases relevant to this mechanism.
    */
}