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

import com.uber.engsec.dp.rewriting.mechanism.{ElasticSensitivityConfig, ElasticSensitivityRewriter}
import com.uber.engsec.dp.sql.QueryParser
import junit.framework.TestCase

class ElasticSensitivityRewriterTest extends TestCase {

  def checkResult(query: String, epsilon: Double, expected: String): Unit = {
    val root = QueryParser.parseToRelTree(query)
    val config = ElasticSensitivityConfig(epsilon)
    val result = (new ElasticSensitivityRewriter).run(root, config)
    TestCase.assertEquals(expected.stripMargin.stripPrefix("\n"), result.toSql)
  }

  def testSimpleQuery() = {
    // the sensitivity of this query is 1
    val query = """
      SELECT COUNT(*) FROM orders
    """

    // scale of Laplace noise for epsilon 0.1 is (1/0.1) = 10
    checkResult(query, 0.1, """
      |SELECT COUNT(*) + 10.0 * (CASE WHEN RAND() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(RAND() - 0.5)))
      |FROM public.orders"""
    )

    // scale of Laplace noise for epsilon 1 is (1/1) = 1
    checkResult(query, 1, """
      |SELECT COUNT(*) + 1.0 * (CASE WHEN RAND() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(RAND() - 0.5)))
      |FROM public.orders"""
    )
  }

  def testStatisticalQueryWithJoin() = {
    val query = """
      SELECT COUNT(*)
      FROM orders JOIN recommendations ON orders.customer_id = recommendations.customer_id
      WHERE orders.product_id = 1
    """

    checkResult(query, 0.1, """
      |SELECT COUNT(*) + 2500.0 * (CASE WHEN RAND() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(RAND() - 0.5)))
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

    checkResult(query, 0.1, """
      |SELECT t.product_id, COUNT(*) + 40000.0 * (CASE WHEN RAND() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(RAND() - 0.5)))
      |FROM (SELECT product_id
      |FROM public.orders) t
      |INNER JOIN (SELECT product_id
      |FROM public.recommendations) t0 ON t.product_id = t0.product_id
      |WHERE t.product_id = 1
      |GROUP BY t.product_id"""
    )
  }
}