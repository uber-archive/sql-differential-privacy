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

import com.uber.engsec.dp.exception.UnsupportedQueryException
import com.uber.engsec.dp.rewriting.differential_privacy.{WPINQConfig, WPINQRewriter}
import com.uber.engsec.dp.schema.Schema
import com.uber.engsec.dp.sql.QueryParser
import junit.framework.TestCase

class WPINQRewriterTest extends TestCase {
  val database = Schema.getDatabase("test")

  def assertUnsupported(query: String) {
    try {
      checkResult(query, 0.1, "")
    }
    catch {
      case e: UnsupportedQueryException => // Pass!
      case e: Exception => TestCase.fail("Wrong exception type")
    }
  }

  def checkResult(query: String, epsilon: Double, expected: String, fillMissingBins: Boolean = false): Unit = {
    val root = QueryParser.parseToRelTree(query, database)
    val config = new WPINQConfig(epsilon, database, fillMissingBins)
    val result = new WPINQRewriter(config).run(root)
    TestCase.assertEquals(expected.stripMargin.stripPrefix("\n"), result.toSql())
  }

  def testUnsupportedQueries() = {
    List(
      // not a counting query
      "SELECT AVG(quantity) FROM orders WHERE product_id = 1",
      // not equijoin condition (conjunction)
      "SELECT COUNT(*) FROM orders JOIN customers ON orders.customer_id = 1 AND customers.customer_id = 1",
      // not equijoin condition (two columns from the same relation)
      "SELECT COUNT(*) FROM orders, customers WHERE orders.customer_id = orders.customer_id"
    ).foreach{ assertUnsupported }
  }

  def testCountQueryWithoutJoin() = {
    val query = """
      SELECT COUNT(*) FROM orders
    """

    // scale of Laplace noise for epsilon 0.1 is (1/0.1) = 10
    checkResult(query, 0.1, """
      |SELECT SUM(1.0) + 10.0 * (CASE WHEN RAND() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(RAND() - 0.5)))
      |FROM public.orders"""
    )

    // scale of Laplace noise for epsilon 1 is (1/1) = 1
    checkResult(query, 1, """
      |SELECT SUM(1.0) + 1.0 * (CASE WHEN RAND() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(RAND() - 0.5)))
      |FROM public.orders"""
    )
  }

  def testCountQueryWithJoin() = {
    val query = """
      SELECT COUNT(*)
      FROM orders JOIN recommendations ON orders.customer_id = recommendations.customer_id
      WHERE orders.product_id = 1
    """

    checkResult(query, 0.1, """
      |WITH _A AS (
      |  SELECT customer_id, product_id, 1.0 _A_w
      |  FROM public.orders
      |), _B AS (
      |  SELECT customer_id, 1.0 _B_w
      |  FROM public.recommendations
      |)
      |SELECT SUM(_weight) + 10.0 * (CASE WHEN RAND() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(RAND() - 0.5)))
      |FROM (SELECT _A.customer_id, _A.product_id, _B.customer_id customer_id0, t.customer_id customer_id1, t0.customer_id customer_id2, _A._A_w * _B._B_w / (t._A_s + t0._B_s) _weight
      |FROM _A
      |INNER JOIN _B ON _A.customer_id = _B.customer_id
      |INNER JOIN (SELECT customer_id, SUM(_A_w) _A_s
      |FROM _A
      |GROUP BY customer_id) t ON _A.customer_id = t.customer_id
      |INNER JOIN (SELECT customer_id, SUM(_B_w) _B_s
      |FROM _B
      |GROUP BY customer_id) t0 ON _A.customer_id = t0.customer_id) t1
      |WHERE t1.product_id = 1"""
    )
  }

  def testHistogramQueryWithJoin() = {
    val query = """
      SELECT orders.product_id, COUNT(*)
      FROM orders JOIN recommendations ON orders.product_id = recommendations.product_id
      WHERE orders.product_id = 1
      GROUP BY 1
    """

    // Result without filling histogram bins
    checkResult(query, 0.1, """
      |WITH _A AS (
      |  SELECT product_id, 1.0 _A_w
      |  FROM public.orders
      |), _B AS (
      |  SELECT product_id, 1.0 _B_w
      |  FROM public.recommendations
      |)
      |SELECT product_id, SUM(_weight) + 10.0 * (CASE WHEN RAND() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(RAND() - 0.5)))
      |FROM (SELECT _A.product_id, _B.product_id product_id0, t.product_id product_id1, t0.product_id product_id2, _A._A_w * _B._B_w / (t._A_s + t0._B_s) _weight
      |FROM _A
      |INNER JOIN _B ON _A.product_id = _B.product_id
      |INNER JOIN (SELECT product_id, SUM(_A_w) _A_s
      |FROM _A
      |GROUP BY product_id) t ON _A.product_id = t.product_id
      |INNER JOIN (SELECT product_id, SUM(_B_w) _B_s
      |FROM _B
      |GROUP BY product_id) t0 ON _A.product_id = t0.product_id) t1
      |WHERE t1.product_id = 1
      |GROUP BY product_id"""
    )

    // Test histogram bin enumeration
    checkResult(query, 0.1, """
      |WITH _orig AS (
      |  WITH _A AS (
      |    SELECT product_id, 1.0 _A_w
      |    FROM public.orders
      |  ), _B AS (
      |    SELECT product_id, 1.0 _B_w
      |    FROM public.recommendations
      |  )
      |  SELECT product_id, SUM(_weight) _weight_sum
      |  FROM (SELECT _A.product_id, _B.product_id product_id0, t.product_id product_id1, t0.product_id product_id2, _A._A_w * _B._B_w / (t._A_s + t0._B_s) _weight
      |  FROM _A
      |  INNER JOIN _B ON _A.product_id = _B.product_id
      |  INNER JOIN (SELECT product_id, SUM(_A_w) _A_s
      |  FROM _A
      |  GROUP BY product_id) t ON _A.product_id = t.product_id
      |  INNER JOIN (SELECT product_id, SUM(_B_w) _B_s
      |  FROM _B
      |  GROUP BY product_id) t0 ON _A.product_id = t0.product_id) t1
      |  WHERE t1.product_id = 1
      |  GROUP BY product_id
      |)
      |SELECT t0._domain product_id, CASE WHEN product_id IS NULL THEN 0 ELSE _weight_sum END + 10.0 * (CASE WHEN RAND() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(RAND() - 0.5)))
      |FROM (SELECT product_id, _weight_sum
      |FROM _orig) t
      |RIGHT JOIN (SELECT product_id _domain
      |FROM public.products) t0 ON product_id = t0._domain""",
      true
    )
  }
}