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

package com.uber.engsec.dp.analysis.differential_privacy

import com.uber.engsec.dp.exception.{UnsupportedConstructException, UnsupportedQueryException}
import com.uber.engsec.dp.sql.QueryParser
import com.uber.engsec.dp.sql.ast.Transformer
import junit.framework.TestCase

class ElasticSensitivityAnalysisTest extends TestCase {

  /********************
    * HELPER FUNCTIONS
    *******************/

  def calculateSensitivities(query: String, k: Int) = {
    println(s"Processing query (distance ${k}): ${query}")
    Transformer.schemaMode = Transformer.SCHEMA_MODE.STRICT
    val root = QueryParser.parseToRelTree(query)
    val analysis = new ElasticSensitivityAnalysis()
    analysis.setK(k)
    analysis.run(root)
  }

  def validateSensitivity(query: String, k: Int, expectedSensitivities: Double*) {
    try {
      val results = calculateSensitivities(query, k)
      TestCase.assertEquals(
        expectedSensitivities.toList,
        results.map(fact => fact.sensitivity.get).toList
      )
    }
    catch {
      case e: Exception =>
        System.err.println(s"While processing query: ${query}")
        throw e
    }
  }

  def assertException(query: String, expectedException: Class[_ <: Exception]) {
    try {
      calculateSensitivities(query, 0)
      TestCase.fail("Unexpected successful analysis (was expecting exception " + expectedException.getSimpleName + ")")
    }
    catch {
      case e: Exception if e.getClass == expectedException => // test pass
      case e: Exception =>
        e.printStackTrace()
        TestCase.failNotEquals("Exception type mismatch. Stack trace printed above.", expectedException, e)
    }
  }

  /******************
    * TESTS
    *****************/

  def testSimpleCount() {
    // Some simple counting queries using various SQL constructs (without join). Sensitivity should be 1.0 for each.
    val q1 = "SELECT COUNT(*) FROM orders WHERE product_id = 1"
    val q2 = "SELECT COUNT(order_id) FROM orders"
    val q3 = "WITH t1 AS (SELECT * FROM orders) SELECT COUNT(*) FROM t1"
    val q4 = "WITH t1 AS (SELECT COUNT(*) FROM orders) SELECT * FROM t1"
    val q5 = "SELECT COUNT(ROUND(order_date, 1)) FROM orders"
    val q6 = "SELECT COUNT(DISTINCT order_id) FROM orders"
    List(q1, q2, q3, q4, q5).foreach { validateSensitivity(_, 0, 1.0) }
  }

  def testSimpleHistogram() {
    val query = "SELECT order_date, COUNT(*) FROM orders GROUP BY 1"
    validateSensitivity(query, 0, 0.0, 1.0)
  }

  def testHistogramProtectedBin() {
    val q1 = "SELECT customer_id, COUNT(*) FROM orders GROUP BY 1"
    val q2 = "SELECT order_date+customer_id, COUNT(*) FROM orders GROUP BY 1"
    val q3 = "SELECT CASE WHEN customer_id = 0 THEN 0 else 1 END, COUNT(*) FROM orders GROUP BY 1"
    List(q1, q2, q3).foreach { assertException(_, classOf[ProtectedBinException]) }
  }

  def testEqualityCheckOnAggregation() = {
    val query = "SELECT COUNT(order_id)=100 FROM orders"
    validateSensitivity(query, 0, Double.PositiveInfinity)
  }

  def testJoinSimple() {
    // maxFreqs:
    //   orders.customer_id = 100
    //   recommendations.customer_id = 250
    val query = """
      SELECT COUNT(*)
      FROM orders JOIN recommendations ON orders.customer_id = recommendations.customer_id
      WHERE orders.product_id = 1
    """
    validateSensitivity(query, 0, 250.0)
    validateSensitivity(query, 2, 252.0)
  }

  def testHistogramWithJoin() {
    // maxFreqs:
    //   orders.customer_id = 100
    //   recommendations.customer_id = 250
    val query = """
      SELECT orders.product_id, COUNT(*)
      FROM orders JOIN recommendations ON orders.customer_id = recommendations.customer_id
      WHERE orders.product_id = 1
      GROUP BY 1
    """
    validateSensitivity(query, 0, 0.0, 250.0)
    validateSensitivity(query, 10, 0.0, 260.0)
  }

  def testUniquenessOptimization() {
    // maxFreqs:
    //   orders.customer_id = 100
    //   customers.customer_id [unique=true]
    val query = """
      SELECT COUNT(*)
      FROM orders JOIN customers ON orders.customer_id = customers.customer_id
      WHERE product_id = 1
    """
    // with uniqueness optimization, sensitivity is 1.0
    validateSensitivity(query, 0, 1.0)
  }

  def testPublicTableOptimization() {
    // maxFreqs:
    //   orders.product_id = 500
    //   products [public=true]
    val query = """
      SELECT COUNT(*)
      FROM orders JOIN products ON orders.product_id = products.product_id
      WHERE orders.product_id = 1
    """
    validateSensitivity(query, 0, 500.0)
    validateSensitivity(query, 25, 525.0)
  }

  def testCompoundJoin() {
    // maxFreqs:
    //   orders.customer_id = 100
    //   customers.address = 5
    //   recommendations.customer_id = 250
    val query = """
      WITH t1 AS (
        SELECT orders.customer_id FROM orders JOIN customers ON orders.customer_id = customers.address
      )
      SELECT COUNT(*)
      FROM t1 JOIN recommendations ON t1.customer_id = recommendations.customer_id
    """
    validateSensitivity(query, 0, 25000.0)
    validateSensitivity(query, 10, 28600.0)
    validateSensitivity(query, 25, 34375.0)
  }

  def testCompoundJoinUniquenessOptimization() {
    // maxFreqs:
    //   orders.customer_id = 100
    //   customers.customer_id [unique=true]
    //   recommendations.customer_id = 250
    val query = """
      WITH t1 AS (
        SELECT orders.customer_id FROM orders JOIN customers ON orders.customer_id = customers.customer_id
      )
      SELECT COUNT(*)
      FROM t1 JOIN recommendations ON t1.customer_id = recommendations.customer_id
    """
    validateSensitivity(query, 0, 250.0)
  }

  def testCompoundJoinPublicTableOptimization() {
    // maxFreqs:
    //   orders.product_id = 500
    //   products.customer_id [public=true]
    //   recommendations.customer_id = 250
    val query = """
      WITH t1 AS (
        SELECT orders.customer_id FROM orders JOIN products ON orders.product_id = products.product_id
      )
      SELECT COUNT(*)
      FROM t1 JOIN recommendations ON t1.customer_id = recommendations.customer_id
    """
    validateSensitivity(query, 0, 125000.0)
    validateSensitivity(query, 25, 144375.0)
  }

  def testSelfJoinSimple() {
    // maxFreq: orders.order_id [unique=true]
    val query1 = "SELECT COUNT(*) FROM orders o1 JOIN orders o2 ON o1.order_id = o2.order_id"
    validateSensitivity(query1, 0, 1.0)

    // maxFreq: orders.customer_id = 100
    val query2 = "SELECT COUNT(*) FROM orders o1 JOIN orders o2 ON o1.customer_id = o2.customer_id"
    validateSensitivity(query2, 0, 201)
    validateSensitivity(query2, 2, 205)
    validateSensitivity(query2, 15, 231)
  }

  def testSelfJoinFromSubquery() {
    // maxFreqs:
    //   orders.customer_id = 100
    //   orders.order_id [unique=true]
    val query = """
      WITH t1 AS
        (SELECT * FROM orders JOIN customers ON orders.customer_id = customers.customer_id)
      SELECT COUNT(*) FROM t1 JOIN orders ON t1.order_id = orders.order_id
    """
    validateSensitivity(query, 0, 1.0)
  }

  def testDeepUniqueSelfJoin() = {
    val query ="""
      WITH r1 AS
        (SELECT c.customer_id AS tra_id FROM orders o JOIN customers c ON o.order_id = c.customer_id)
      SELECT COUNT(*) FROM r1 a JOIN r1 b ON a.tra_id = b.tra_id
    """
    validateSensitivity(query, 0, 1.0)
  }

  def testAggregationInNestedQuery() = {
    // maxFreqs:
    //   orders.customer_id = 100
    //   customers.customer_id [unique=true]
    val query = """
      WITH t1 AS
        (SELECT COUNT(*) as mycount FROM orders JOIN customers ON orders.customer_id = customers.customer_id)
      SELECT mycount FROM t1
    """
    validateSensitivity(query, 0, 1.0)
  }

  def testNonCountingQuery() = {
    val query = "SELECT name, order_date FROM orders JOIN customers ON orders.customer_id = customers.customer_id"
    assertException(query, classOf[UnsupportedQueryException])
  }

  def testArithmeticOnResult() = {
    val q1 = "SELECT COUNT(*)+100 FROM orders"
    val q2 = "SELECT COUNT(cnt) FROM (SELECT COUNT(*) as cnt FROM orders)"
    val q3 = "WITH t1 AS (SELECT COUNT(*) as mycount from products) SELECT mycount/100 FROM t1"
    val q4 = "WITH t1 AS (SELECT product_id, AVG(quantity) as avg_quantity from orders GROUP BY 1) SELECT COUNT(avg_quantity) FROM t1"
    List(q1, q2, q3, q4).foreach { assertException(_, classOf[ArithmeticOnAggregationResultException]) }

    // don't throw error if arithmetic is applied in ways that don't affect the output column values
    val q5 = """
      WITH t1 AS (SELECT order_date, COUNT(*)+1 as num_orders_plus_one FROM orders GROUP BY 1)
      SELECT COUNT(order_date) FROM t1 WHERE num_orders_plus_one < 10
    """
    validateSensitivity(q5, 0, 1.0)
  }

  def testNonEquijoin() = {
    // Elastic sensitivity supports equijoins on direct column values. This test verifies that the analysis correctly
    // rejects unsupported join types and conditions.
    val q1 = "SELECT COUNT(*) FROM orders JOIN customers ON orders.order_id < customers.customer_id"
    val q2 = "SELECT COUNT(*) FROM orders JOIN customers ON orders.order_id = 1"
    val q3 = "SELECT COUNT(*) FROM orders JOIN customers ON orders.order_id = orders.customer_id"
    val q4 = "SELECT COUNT(*) FROM orders, customers WHERE orders.customer_id = customers.customer_id"
    val q5 = "SELECT COUNT(*) FROM orders, customers"
    List(q1, q2, q3, q4, q5).foreach { assertException(_, classOf[UnsupportedConstructException]) }
  }

  def testMissingMetric() = {
    // maxFreq metric is (intentionally) undefined for customers.name
    val query = "SELECT COUNT(*) FROM orders JOIN customers ON orders.customer_id = customers.name"
    assertException(query, classOf[MissingMetricException])
  }

  def testDerivedJoinKey() = {
    // These queries join on an altered column values, which means the metrics are not applicable and therefore
    // the query should be rejected.
    val q1 = """
      WITH t1 AS (SELECT ROUND(customer_id, 1) as customer_id FROM customers)
      SELECT COUNT(*) FROM recommendations JOIN t1 ON recommendations.customer_id = t1.customer_id
    """
    val q2 = """
      WITH t1 AS (SELECT customer_id+1 as customer_id FROM customers)
      SELECT COUNT(*) FROM recommendations JOIN t1 ON recommendations.customer_id = t1.customer_id
    """
    val q3 = """
      WITH _orders AS (SELECT COUNT(*) as product_id FROM orders)
      SELECT COUNT(*) from _orders JOIN products ON _orders.product_id = products.product_id
    """

    List(q1, q2, q3).foreach { assertException(_, classOf[MissingMetricException]) }
  }
}