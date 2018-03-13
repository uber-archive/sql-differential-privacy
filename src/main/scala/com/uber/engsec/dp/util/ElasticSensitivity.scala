package com.uber.engsec.dp.util

import com.uber.engsec.dp.analysis.differential_privacy.ElasticSensitivityAnalysis
import com.uber.engsec.dp.schema.Database
import com.uber.engsec.dp.sql.QueryParser
import com.uber.engsec.dp.sql.relational_algebra.Relation

/** Utility methods for elastic sensitivity-based differential privacy. */
object ElasticSensitivity {
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
    * Note: when calculating elastic sensitivity for sequential values of k (e.g., to use a smoothing function), use the
    * stream method below, which caches the query parse tree and is therefore much more efficient.
    *
    * @param query The input query
    * @param k The desired distance from the true database
    * @return Elastic sensitivity of query at distance k
    */
  def elasticSensitivity(query: Relation, database: Database, k: Int): Double = {
    val analysis = new ElasticSensitivityAnalysis()
    analysis.setK(k)

    val result = analysis.analyzeQuery(query, database).colFacts
    assert (result.size == 1)  // this function works for single-column queries.
    result.head.sensitivity.get
  }

  /** Returns a (lazily evaluated) stream of elastic sensitivities of the given column for the query at every distance k.
    *
    * @param query The input query
    * @return Elastic sensitivities for every distance k from the true database (k = 0, 1, 2, ...)
    */
  def elasticSensitivityStream(query: Relation, database: Database, col: Int): Stream[Double] = {
    val analysis = new ElasticSensitivityAnalysis()

    Stream.from(0).map{ k =>
      analysis.setK(k)
      val result = analysis.analyzeQuery(query, database).colFacts
      result(col).sensitivity.get
    }
  }

  /** Compute the smoothed elastic sensitivity for a given column of the query with a given epsilon.
    *
    * @param query The input query
    * @param col The index of the target column (0-based)
    * @param epsilon The desired privacy budget
    * @param delta The value of the delta parameter
    * @return The smoothed elastic sensitivity
    */
  def smoothElasticSensitivity(query: Relation, database: Database, col: Int, epsilon: Double, delta: Double): Double = {
    /** Calculates the smooth elastic sensitivity by recursively computing smooth sensitivity for each value of k
      * until the function decreases at k+1. Since elastic sensitivity increases polynomially (at worst) in k while the
      * smoothing factor decays exponentially in k, this provides the correct (maximum) smooth sensitivity without
      * requiring computation for every k up to the size of the database.
      */
    def sensitivityAtDistance(k: Int, prevSensitivity: Double, esStream: Stream[Double]): Double = {
      val elasticSensitivityAtK = esStream.head
      val beta = epsilon / (2 * Math.log(2 / delta))
      val smoothSensitivity = Math.exp(-k * beta) * elasticSensitivityAtK

      if ((elasticSensitivityAtK == 0) || (smoothSensitivity < prevSensitivity)) prevSensitivity
      else sensitivityAtDistance(k+1, smoothSensitivity, esStream.tail)
    }

    sensitivityAtDistance(0, 0, elasticSensitivityStream(query, database, col))
  }

  /** Produce a differentially private result for a query given its non-private result and the desired privacy budget.
    *
    * @param query The input query. It must return a single row and single column.
    * @param result The non-private result of running the query (a single number).
    * @param epsilon The desired privacy budget (e.g. 0.1).
    * @param delta The desired delta parameter (e.g. 1/n^2)
    * @return A differentially private answer to the input query.
    */
  def addNoise(query: String, database: Database, result: Double, epsilon: Double, delta: Double): Double = {
    val tree = QueryParser.parseToRelTree(query, database)
    val sensitivity = ElasticSensitivity.smoothElasticSensitivity(tree, database, 0, epsilon, delta)
    result + laplace(2 * sensitivity / epsilon)
  }
}
