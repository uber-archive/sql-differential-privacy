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
import com.uber.engsec.dp.schema.Schema
import com.uber.engsec.dp.sql.QueryParser
import com.uber.engsec.dp.sql.ast.Transformer
import junit.framework.TestCase

class ElasticSensitivityAnalysisTest extends TestCase {
  val database = Schema.getDatabase("test")

  /********************
    * HELPER FUNCTIONS
    *******************/

  def calculateSensitivities(query: String, k: Int) = {
    println(s"Processing query (distance ${k}): ${query}")
    Transformer.schemaMode = Transformer.SCHEMA_MODE.STRICT
    val root = QueryParser.parseToRelTree(query, database)
    val analysis = new ElasticSensitivityAnalysis()
    analysis.setK(k)
    analysis.run(root, database).colFacts
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
    List(
      "SELECT COUNT(*) FROM orders WHERE product_id = 1",
      "SELECT COUNT(order_id) FROM orders",
      "WITH t1 AS (SELECT * FROM orders) SELECT COUNT(*) FROM t1",
      "WITH t1 AS (SELECT COUNT(*) FROM orders) SELECT * FROM t1",
      "SELECT COUNT(ROUND(order_date, 1)) FROM orders",
      "SELECT COUNT(DISTINCT order_id) FROM orders"
    ).foreach { validateSensitivity(_, 0, 1.0) }
  }

  def testSimpleHistogram() {
    val query = "SELECT order_date, COUNT(*) FROM orders GROUP BY 1"
    validateSensitivity(query, 0, 0.0, 2.0)
  }

  def testHistogramProtectedBin() {
    List(
      "SELECT customer_id, COUNT(*) FROM orders GROUP BY 1",
      "SELECT order_date+customer_id, COUNT(*) FROM orders GROUP BY 1",
      "SELECT CASE WHEN customer_id = 0 THEN 0 else 1 END, COUNT(*) FROM orders GROUP BY 1"
    ).foreach { assertException(_, classOf[ProtectedBinException]) }
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

  def testFilterPushdown() = {
    // ensure the analysis can handle filter conditions in the WHERE clause of a cross-join relation, which is
    // semantically equivalent to an equijoin. We ensure this case is supported because it occurs fairly often in SQL
    // queries.

    /** Positive test cases */
    List(
      "SELECT COUNT(*) FROM orders, customers WHERE orders.customer_id = customers.customer_id",
      // ensure we can decompose the equijoin condition from the remaining conjunctive clauses
      "SELECT COUNT(*) FROM orders, customers WHERE orders.customer_id = customers.customer_id AND orders.customer_id != 1"
    ).foreach { validateSensitivity(_, 0, 100.0) }

    /** Negative test cases */
    List(
      // not equijoin condition (two columns from the same relation)
      "SELECT COUNT(*) FROM orders, customers WHERE orders.customer_id = orders.customer_id",
      // a disjunction in the filter cannot be transformed into an equijoin
      "SELECT COUNT(*) FROM orders, customers WHERE orders.customer_id = customers.customer_id OR orders.customer_id != 1"
    ).foreach { assertException(_, classOf[UnsupportedConstructException]) }
  }

  def testConjuctiveJoinPredicate() = {
    // elastic sensitivity can handle join predicates that are conjunctions, as long as at least one of the clauses is
    // an equijoin (since the remaining clauses can only decrease, never increase, the stability of the joined node)
    /** Positive test cases */
    List(
      // simple case
      "SELECT COUNT(*) FROM orders JOIN customers ON orders.customer_id = customers.customer_id AND customers.customer_id > 10",
      // order of clauses shouldn't matter
      "SELECT COUNT(*) FROM orders JOIN customers ON orders.customer_id > 10 AND customers.customer_id = orders.customer_id",
      // only top-level predicates need to be conjunctive
      "SELECT COUNT(*) FROM orders JOIN customers ON orders.customer_id = customers.customer_id AND (orders.product_id = 1 OR customers.customer_id = 1)",
      // conjunctive clauses might be nested in the parse tree. Analysis needs to flatten this predicate to find the equijoin condition
      "SELECT COUNT(*) FROM orders JOIN customers ON (orders.product_id = 1 AND (customers.customer_id = 1 AND (orders.customer_id = customers.customer_id)))",
      // if more than one clause is equijoin, analysis should choose the one that results in lower sensitivity (regardless of the order of clauses)
      "SELECT COUNT(*) FROM orders JOIN customers ON (orders.customer_id = customers.customer_id) AND (orders.product_id = customers.address)",
      "SELECT COUNT(*) FROM orders JOIN customers ON (orders.product_id = customers.address) AND (orders.customer_id = customers.customer_id)"
    ).foreach { validateSensitivity(_, 0, 100.0) }

    /** Negative test cases */
    List(
      // non equijoin condition
      "SELECT COUNT(*) FROM orders JOIN customers ON orders.customer_id = 1 AND customers.customer_id = 1",
      // disjunction
      "SELECT COUNT(*) FROM orders JOIN customers ON orders.customer_id = orders.customer_id OR orders.customer_id > 10"
    ).foreach { assertException(_, classOf[UnsupportedConstructException]) }
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
    validateSensitivity(query, 0, 0.0, 500.0)
    validateSensitivity(query, 10, 0.0, 520.0)
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
    validateSensitivity(query, 0, 75000.0)
    validateSensitivity(query, 25, 82500.0)
  }

  def testSelfJoinSimple() {
    // maxFreq: orders.order_id = 1
    val query1 = "SELECT COUNT(*) FROM orders o1 JOIN orders o2 ON o1.order_id = o2.order_id"
    validateSensitivity(query1, 0, 3.0)

    // maxFreq: orders.customer_id = 100
    val query2 = "SELECT COUNT(*) FROM orders o1 JOIN orders o2 ON o1.customer_id = o2.customer_id"
    validateSensitivity(query2, 0, 201)
    validateSensitivity(query2, 2, 205)
    validateSensitivity(query2, 15, 231)
  }

  def testSelfJoinFromSubquery() {
    // maxFreqs:
    //   orders.customer_id = 100
    //   orders.order_id = 1
    val query = """
      WITH t1 AS
        (SELECT * FROM orders JOIN customers ON orders.customer_id = customers.customer_id)
      SELECT COUNT(*) FROM t1 JOIN orders ON t1.order_id = orders.order_id
    """
    validateSensitivity(query, 0, 201.0)
  }

  def testDeepUniqueSelfJoin() = {
    val query ="""
      WITH r1 AS
        (SELECT c.customer_id AS tra_id FROM orders o JOIN customers c ON o.order_id = c.customer_id)
      SELECT COUNT(*) FROM r1 a JOIN r1 b ON a.tra_id = b.tra_id
    """
    validateSensitivity(query, 0, 3.0)
  }

  def testAggregationInNestedQuery() = {
    // maxFreqs:
    //   orders.customer_id = 100
    //   customers.customer_id = 1
    val query = """
      WITH t1 AS
        (SELECT COUNT(*) as mycount FROM orders JOIN customers ON orders.customer_id = customers.customer_id)
      SELECT mycount FROM t1
    """
    validateSensitivity(query, 0, 100.0)
  }

  def testNonCountingQuery() = {
    val query = "SELECT name, order_date FROM orders JOIN customers ON orders.customer_id = customers.customer_id"
    assertException(query, classOf[UnsupportedQueryException])
  }

  def testArithmeticOnResult() = {
    List(
      "SELECT COUNT(*)+100 FROM orders",
      "WITH t1 AS (SELECT COUNT(*) as mycount from products) SELECT mycount/100 FROM t1",
      "WITH t1 AS (SELECT product_id, COUNT(quantity) as avg_quantity from orders GROUP BY 1) SELECT ROUND(avg_quantity,1) FROM t1"
    ).foreach { assertException(_, classOf[ArithmeticOnAggregationResultException]) }

    // don't throw error if arithmetic is applied in ways that don't affect the output column values
    val query = """
      WITH t1 AS (SELECT order_date, COUNT(*)+1 as num_orders_plus_one FROM orders GROUP BY 1)
      SELECT COUNT(order_date) FROM t1 WHERE num_orders_plus_one < 10
    """
    validateSensitivity(query, 0, 2.0)
  }

  def testNonEquijoin() = {
    List(
      "SELECT COUNT(*) FROM orders JOIN customers ON orders.order_id < customers.customer_id",
      "SELECT COUNT(*) FROM orders JOIN customers ON orders.order_id = 1",
      "SELECT COUNT(*) FROM orders JOIN customers ON orders.order_id = orders.customer_id",
      "SELECT COUNT(*) FROM orders, customers"
    ).foreach { assertException(_, classOf[UnsupportedConstructException]) }
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
    validateSensitivity(query, 0, 300.0)
  }

  def testPublicOnlyQuery() = {
    // Queries that access *only* public data should produce sensitivity zero (i.e., no noise required)
    List("""
      SELECT COUNT(*) FROM products WHERE product_id = 1
    """, """
      SELECT COUNT(*)
      FROM products p1 JOIN products p2 ON p1.product_id = p2.product_id
    """).foreach { q =>
      validateSensitivity(q, 0, 0.0)
      validateSensitivity(q, 10, 0.0)
    }
  }

  def testPropagatePublicFlag() {
    // this test ensures that the public-ness of the products table is propagated through the subquery & filter
    // nodes so that the optimization can be applied at the join. Previously the flag worked only when the query
    // joined on root table nodes.
    val query = """
      WITH t1 AS (SELECT * from products WHERE product_id = 1)
      SELECT COUNT(*)
      FROM orders JOIN t1 ON orders.product_id = t1.product_id
    """
    validateSensitivity(query, 0, 300.0)
  }

  def testMissingMetric() = {
    // maxFreq metric is (intentionally) undefined for customers.name
    val query = "SELECT COUNT(*) FROM orders JOIN customers ON orders.customer_id = customers.name"
    assertException(query, classOf[MissingMetricException])
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
    ).foreach { assertException(_, classOf[MissingMetricException]) }
  }
}