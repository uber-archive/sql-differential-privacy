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

package com.uber.engsec.dp.util

import com.uber.engsec.dp.analysis.differential_privacy.ElasticSensitivityAnalysis
import com.uber.engsec.dp.sql.QueryParser

/** DPExample: A simple differential privacy implementation using elastic sensitivity.
  *
  * This example code supports queries that return a single column and single row. The code can be extended to support
  * queries returning multiple columns and rows by generating independent noise samples for each cell based the
  * appropriate column sensitivity.
  *
  * Caveats:
  *
  * Histogram queries (using SQL's GROUP BY) must be handled carefully so as not to leak information in the bin labels.
  * The analysis throws an error to warn about this, but this behavior can overridden if you know what you're doing.
  *
  * This example does not implement a privacy budget management strategy. Each query is executed using the full budget
  * value of EPSILON. Correct use of differential privacy requires allocating a fixed privacy from which a portion is
  * depleted to run each query. A privacy budget strategy depends on the problem domain and threat model and is
  * therefore beyond the scope of this tool.
  */
object DPExample extends App {
  // Use the table schemas and metadata defined by the test classes
  System.setProperty("schema.config.path", "src/test/resources/schema.yaml")

  // example query: How many US customers ordered product #1?
  val query = """
    SELECT COUNT(*) FROM orders
    JOIN customers ON orders.customer_id = customers.customer_id
    WHERE orders.product_id = 1 AND customers.address LIKE '%United States%'
  """

  // query result when executed on the database
  val QUERY_RESULT = 100000

  // privacy budget
  val EPSILON = 0.1

  println(s"Query: $query")
  println(s"Private result: $QUERY_RESULT\n")

  (1 to 10).foreach { i =>
    val noisyResult = DPUtils.addNoise(query, QUERY_RESULT, EPSILON)
    println(s"Noisy result (run $i): %.0f".format(noisyResult))
  }
}

/** Utility methods for the example code. */
object DPUtils {
  /** Generate Laplace noise centered at 0 with the given scale.
    *
    * @param scale The scale of the noise
    * @return A single random number drawn from the distribution
    */
  def laplace(scale: Double): Double = {
    val u = 0.5 - scala.util.Random.nextDouble()
    -math.signum(u) * scale * math.log(1 - 2*math.abs(u))
  }

  /** Compute the elastic sensitivity of the query at distance k.
    *
    * Note: if you intend to calculate elastic sensitivity for sequential values of k (e.g., to use a smoothing
    * function) you should use the stream method below, which caches the query parse tree and is therefore much
    * more efficient.
    *
    * @param query The input query
    * @param k The desired distance from the true database
    * @return Elastic sensitivity of query at distance k
    */
  def elasticSensitivity(query: String, k: Int): Double = {
    val analysis = new ElasticSensitivityAnalysis()
    analysis.setK(k)

    val result = analysis.analyzeQuery(query)
    assert (result.size == 1)  // this example code works for single-column queries.
    result.head.sensitivity.get
  }

  /** Returns a (lazily evaluated) stream of elastic sensitivities of the query for every distance k.
    *
    * @param query The input query
    * @return Elastic sensitivities for every distance k from the true database (k = 0, 1, 2, ...)
    */
  def elasticSensitivityStream(query: String): Stream[Double] = {
    val analysis = new ElasticSensitivityAnalysis()
    val tree = QueryParser.parseToRelTree(query)

    Stream.from(0).map{ k =>
      analysis.setK(k)
      val result = analysis.analyzeQuery(tree)
      assert (result.size == 1)  // this example code works for single-column queries. See note above about extending to multi-column queries.
      result.head.sensitivity.get
    }
  }

  /** Compute the smoothed elastic sensitivity of the query with given epsilon.
    *
    * @param query The input query
    * @param epsilon The desired privacy budget
    * @return The smoothed elastic sensitivity
    */
  def smoothSensitivity(query: String, epsilon: Double): Double = {
    /** Calculates the smooth elastic sensitivity by recursively computing smooth sensitivity for each value of k
      * until the function decreases at k+1. Since elastic sensitivity increases polynomially (at worst) in k while the
      * smoothing factor decays exponentially in k, this provides the correct (maximum) smooth sensitivity without
      * requiring computation for every k up to the size of the database.
      */
    def sensitivityAtDistance(k: Int, prevSensitivity: Double, esStream: Stream[Double]): Double = {
      val elasticSensitivityAtK = esStream.head
      val smoothSensitivity = Math.exp(-k * epsilon) * elasticSensitivityAtK

      if (smoothSensitivity < prevSensitivity) prevSensitivity
      else sensitivityAtDistance(k+1, smoothSensitivity, esStream.tail)
    }

    sensitivityAtDistance(0, 0, elasticSensitivityStream(query))
  }

  /** Produce a differentially private result for a query given its non-private result
    * and the desired privacy budget.
    *
    * @param query The input query. It must return a single row and single column.
    * @param result The non-private result of running the query (a single number).
    * @param epsilon The desired privacy budget (e.g. 0.1).
    * @return A differentially private answer to the input query.
    */
  def addNoise(query: String, result: Double, epsilon: Double): Double = {
    val sensitivity = smoothSensitivity(query, epsilon)
    result + laplace(sensitivity / epsilon)
  }
}
