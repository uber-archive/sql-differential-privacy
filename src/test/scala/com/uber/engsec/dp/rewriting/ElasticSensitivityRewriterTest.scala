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

import com.uber.engsec.dp.rewriting.differential_privacy.{ElasticSensitivityConfig, ElasticSensitivityRewriter}
import com.uber.engsec.dp.schema.Schema
import com.uber.engsec.dp.sql.QueryParser
import junit.framework.TestCase

class ElasticSensitivityRewriterTest extends TestCase {
  val database = Schema.getDatabase("test")

  def checkResult(query: String, epsilon: Double, delta: Double, expected: String, fillMissingBins: Boolean = false): Unit = {
    val root = QueryParser.parseToRelTree(query, database)
    val config = new ElasticSensitivityConfig(epsilon, delta, database, fillMissingBins)
    val result = new ElasticSensitivityRewriter(config).run(root)
    TestCase.assertEquals(expected.stripMargin.stripPrefix("\n"), result.toSql())
  }

  def testUnsupportedQueries() = {
    // This rewriter calls ElasticSensitivity analysis; see ElasticSensitivityAnalysisTest for tests of unsupported queries
  }

  def testCountQueryWithoutJoin() = {
    // the sensitivity of this query is 1
    val query = """
      SELECT COUNT(*) FROM orders
    """

    // scale of Laplace noise for epsilon 0.1 is 2*(1/0.1) = 20
    checkResult(query, 0.1, 1e-8, """
      |SELECT COUNT(*) + 20.0 * (CASE WHEN RAND() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(RAND() - 0.5)))
      |FROM public.orders"""
    )

    // scale of Laplace noise for epsilon 1 is 2*(1/1) = 2
    checkResult(query, 1, 1e-8, """
      |SELECT COUNT(*) + 2.0 * (CASE WHEN RAND() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(RAND() - 0.5)))
      |FROM public.orders"""
    )
  }

  def testCountQueryWithJoin() = {
    val query = """
      SELECT COUNT(*)
      FROM orders JOIN recommendations ON orders.customer_id = recommendations.customer_id
      WHERE orders.product_id = 1
    """

    checkResult(query, 0.1, 1e-8, """
      |SELECT COUNT(*) + 5409.181856298167 * (CASE WHEN RAND() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(RAND() - 0.5)))
      |FROM (SELECT customer_id, product_id
      |FROM public.orders) t
      |INNER JOIN (SELECT customer_id
      |FROM public.recommendations) t0 ON t.customer_id = t0.customer_id
      |WHERE t.product_id = 1"""
    )
  }

  def testHistogramQueryWithJoin() = {
    val query = """
      SELECT orders.product_id, COUNT(*)
      FROM orders JOIN recommendations ON orders.product_id = recommendations.product_id
      WHERE orders.product_id = 1
      GROUP BY 1
    """


    checkResult(query, 0.1, 1e-8, """
      |SELECT t.product_id, COUNT(*) + 80000.0 * (CASE WHEN RAND() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(RAND() - 0.5)))
      |FROM (SELECT product_id
      |FROM public.orders) t
      |INNER JOIN (SELECT product_id
      |FROM public.recommendations) t0 ON t.product_id = t0.product_id
      |WHERE t.product_id = 1
      |GROUP BY t.product_id"""
    )

    // Test histogram bin enumeration
    checkResult(query, 0.1, 1e-8, """
      |WITH _orig AS (
      |  SELECT t.product_id, COUNT(*) _agg
      |  FROM (SELECT product_id
      |  FROM public.orders) t
      |  INNER JOIN (SELECT product_id
      |  FROM public.recommendations) t0 ON t.product_id = t0.product_id
      |  WHERE t.product_id = 1
      |  GROUP BY t.product_id
      |)
      |SELECT t0._domain product_id, CASE WHEN product_id IS NULL THEN 0 ELSE _agg END + 80000.0 * (CASE WHEN RAND() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(RAND() - 0.5)))
      |FROM (SELECT product_id, _agg
      |FROM _orig) t
      |RIGHT JOIN (SELECT product_id _domain
      |FROM public.products) t0 ON product_id = t0._domain""",
      true
    )
  }

  def testHistogramQueryWithAggAlias() = {
    val query = """
      SELECT orders.product_id, COUNT(*) AS "mycount"
      FROM orders JOIN recommendations ON orders.product_id = recommendations.product_id
      WHERE orders.product_id = 1
      GROUP BY 1
    """

    // Test histogram bin enumeration when aggregation already has explicit alias
    checkResult(query, 0.1, 1e-8, """
        |WITH _orig AS (
        |  SELECT t.product_id, COUNT(*) mycount
        |  FROM (SELECT product_id
        |  FROM public.orders) t
        |  INNER JOIN (SELECT product_id
        |  FROM public.recommendations) t0 ON t.product_id = t0.product_id
        |  WHERE t.product_id = 1
        |  GROUP BY t.product_id
        |)
        |SELECT t0._domain product_id, CASE WHEN product_id IS NULL THEN 0 ELSE mycount END + 80000.0 * (CASE WHEN RAND() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(RAND() - 0.5))) mycount
        |FROM (SELECT product_id, mycount
        |FROM _orig) t
        |RIGHT JOIN (SELECT product_id _domain
        |FROM public.products) t0 ON product_id = t0._domain""",
      true
    )
  }
}