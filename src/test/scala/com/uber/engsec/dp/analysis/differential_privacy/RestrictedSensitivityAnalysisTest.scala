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

import com.uber.engsec.dp.exception.UnsupportedQueryException
import com.uber.engsec.dp.schema.Schema
import com.uber.engsec.dp.sql.QueryParser
import com.uber.engsec.dp.sql.ast.Transformer
import junit.framework.TestCase

class RestrictedSensitivityAnalysisTest extends TestCase {
  val database = Schema.getDatabase("test")

  /********************
    * HELPER FUNCTIONS
    *******************/

  def calculateSensitivities(query: String) = {
    println(s"Processing query: $query")
    Transformer.schemaMode = Transformer.SCHEMA_MODE.STRICT
    val root = QueryParser.parseToRelTree(query, database)
    val analysis = new RestrictedSensitivityAnalysis()
    analysis.run(root, database).colFacts
  }

  def validateSensitivity(query: String, expectedSensitivities: Double*) {
    try {
      val results = calculateSensitivities(query)
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
      calculateSensitivities(query)
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
    List(
      "SELECT COUNT(*) FROM orders WHERE product_id = 1",
      "SELECT COUNT(order_id) FROM orders",
      "WITH t1 AS (SELECT * FROM orders) SELECT COUNT(*) FROM t1",
      "WITH t1 AS (SELECT COUNT(*) FROM orders) SELECT * FROM t1",
      "SELECT COUNT(ROUND(order_date, 1)) FROM orders",
      "SELECT COUNT(DISTINCT order_id) FROM orders"
    ).foreach { validateSensitivity(_, 1.0) }
  }

  def testSimpleHistogram() {
    val query = "SELECT order_date, COUNT(*) FROM orders GROUP BY 1"
    validateSensitivity(query, 0.0, 2.0)
  }

  def testHistogramProtectedBin() {
    List(
      "SELECT customer_id, COUNT(*) FROM orders GROUP BY 1",
      "SELECT order_date+customer_id, COUNT(*) FROM orders GROUP BY 1",
      "SELECT CASE WHEN customer_id = 0 THEN 0 else 1 END, COUNT(*) FROM orders GROUP BY 1"
    ).foreach { assertException(_, classOf[ProtectedBinException]) }
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
    assertException(query, classOf[UnsupportedQueryException])
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
    assertException(query, classOf[UnsupportedQueryException])
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

    assertException(query, classOf[UnsupportedQueryException])
  }

  def testSelfJoin() {
    // Restricted sensitivity does not support self-joins
    val query1 = "SELECT COUNT(*) FROM orders o1 JOIN orders o2 ON o1.order_id = o2.order_id"
    assertException(query1, classOf[UnsupportedQueryException])
  }

  def testDeepSelfJoin() = {
    val query ="""
      WITH r1 AS
        (SELECT c.customer_id AS tra_id FROM orders o JOIN customers c ON o.order_id = c.customer_id)
      SELECT COUNT(*) FROM r1 a JOIN r1 b ON a.tra_id = b.tra_id
    """
    assertException(query, classOf[UnsupportedQueryException])
  }

  def testNonCountingQuery() = {
    val query = "SELECT name, order_date FROM orders JOIN customers ON orders.customer_id = customers.customer_id"
    assertException(query, classOf[UnsupportedQueryException])
  }

  def testNonEquijoin() = {
    List(
      "SELECT COUNT(*) FROM orders JOIN customers ON orders.order_id < customers.customer_id",
      "SELECT COUNT(*) FROM orders JOIN customers ON orders.order_id = 1",
      "SELECT COUNT(*) FROM orders JOIN customers ON orders.order_id = orders.customer_id",
      "SELECT COUNT(*) FROM orders, customers"
    ).foreach { assertException(_, classOf[UnsupportedQueryException]) }
  }

  def testMissingMetric() = {
    // maxFreq metric is (intentionally) undefined for customers.name
    val query = "SELECT COUNT(*) FROM orders JOIN customers ON orders.customer_id = customers.name"
    assertException(query, classOf[UnsupportedQueryException])
  }

  def testDerivedJoinKey() = {
    // These queries join on an altered column values, which means the metrics are not applicable and therefore
    // the query should be rejected.
    List("""
      WITH t1 AS (SELECT ROUND(customer_id, 1) as customer_id FROM customers)
      SELECT COUNT(*) FROM recommendations JOIN t1 ON recommendations.customer_id = t1.customer_id
    """, """
      WITH t1 AS (SELECT customer_id+1 as customer_id FROM customers)
      SELECT COUNT(*) FROM recommendations JOIN t1 ON recommendations.customer_id = t1.customer_id
    """, """
      WITH t1 AS (SELECT COUNT(*) as customer_id FROM orders)
      SELECT COUNT(*) from t1 JOIN recommendations ON t1.customer_id = recommendations.customer_id
    """
    ).foreach { assertException(_, classOf[UnsupportedQueryException]) }
  }
}