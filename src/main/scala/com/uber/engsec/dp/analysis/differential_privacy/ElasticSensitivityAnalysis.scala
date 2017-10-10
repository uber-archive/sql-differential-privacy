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

import com.uber.engsec.dp.analysis.histogram.{HistogramAnalysis, QueryType}
import com.uber.engsec.dp.dataflow.AggFunctions._
import com.uber.engsec.dp.dataflow.column.{NodeColumnFacts, RelNodeColumnAnalysis}
import com.uber.engsec.dp.exception.{UnsupportedConstructException, UnsupportedQueryException}
import com.uber.engsec.dp.schema.Schema
import com.uber.engsec.dp.sql.relational_algebra._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Aggregate, Join, TableScan}
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.calcite.sql.SqlKind

/** Elastic sensitivity analysis.
  *
  * Calculates an upper bound on the local sensitivity of a query, based on the query, information about the maximum
  * frequency of join keys used in the query, and constraints on the data model of the database being queried.
  * Elastic sensitivity can be combined with a smoothing function (such as smooth sensitivity) to arrive at a
  * sensitivity value that can be used to calibrate the scale of Laplace noise needed to achieve differential privacy.
  */
class ElasticSensitivityAnalysis extends RelNodeColumnAnalysis(StabilityDomain, SensitivityDomain) {

  var k: Int = 0
  val checkBinsForRelease: Boolean = System.getProperty("dp.check_bins", "true").toBoolean

  /** Reject non-statistical queries before analysis begins. */
  override def run(root: RelOrExpr): NodeColumnFacts[RelStability,ColSensitivity] = {
    val histogramResults = new HistogramAnalysis().run(root)
    val queryType = QueryType.getQueryType(histogramResults)

    if ((queryType != QueryType.HISTOGRAM && queryType != QueryType.NON_HISTOGRAM_STATISTICAL) ||
      histogramResults.filter {
        _.isAggregation
      }.exists {
        _.outermostAggregation.get != COUNT
      })
      throw new UnsupportedQueryException("This analysis currently works only on statistical and histogram counting queries")

    val results = super.run(root)

    // Print helpful error message if any column sensitivity is infinite due to post-processed aggregation results
    // (e.g., SELECT COUNT(*)+1000 FROM ORDERS), which this analysis does not currently support.
    val idx = results.colFacts.indexWhere { col => col.sensitivity.contains(Double.PositiveInfinity) && col.postAggregationArithmeticApplied }
    if (idx != -1) {
      val colName = root.unwrap.asInstanceOf[RelNode].getRowType.getFieldNames.get(idx)
      throw new ArithmeticOnAggregationResultException(colName)
    }

    results
  }

  /** Set the distance ''k'' from the true database for the elastic sensitivity calculation.
    *
    * @param k Desired distance from true database (default 0, to approximate local sensitivity)
    */
  def setK(k: Int): Unit = this.k = k

  import scala.collection.JavaConverters._

  override def transferAggregate(node: Aggregate,
                                 aggFunctions: IndexedSeq[Option[AggFunction]],
                                 state: NodeColumnFacts[RelStability,ColSensitivity]): NodeColumnFacts[RelStability,ColSensitivity] = {
    val newColFacts = state.colFacts.zipWithIndex.map { case (colState, idx) =>
      val aggFunction = aggFunctions(idx)
      aggFunction match {
        case None => // histogram bin
          if (checkBinsForRelease && !colState.canRelease) {
            val binName = node.getRowType.getFieldNames.get(idx)
            throw new ProtectedBinException(binName)
          }

          // Sensitivity of bins that are safe for release is zero (i.e. no noise added)
          colState.copy(sensitivity = Some(0.0))

        case Some(func) => // aggregated column
          val newSensitivity = func match {
            // The sensitivity of a count column is the stability of the target node. For all other aggregation
            // functions the sensitivity is raised to infinity (since these functions are not yet supported)
            case COUNT => state.nodeFact.stability
            case _     => Double.PositiveInfinity
          }

          colState.copy(
            sensitivity = Some(newSensitivity),
            maxFreq = Double.PositiveInfinity,
            aggregationApplied = true,
            postAggregationArithmeticApplied = colState.aggregationApplied,
            canRelease = false)
      }
    }

    state.copy(colFacts = newColFacts)
  }

  override def transferTableScan(node: TableScan,
                                 state: NodeColumnFacts[RelStability,ColSensitivity]): NodeColumnFacts[RelStability,ColSensitivity] = {
    // Fetch metadata for the table
    val tableName = node.getTable.getQualifiedName.asScala.mkString(".")
    val isTablePublic = RelUtils.getTableProperties(node).get("isPublic").fold(false)(_.toBoolean)

    val newColFacts = state.colFacts.zipWithIndex.map { case (colState, idx) =>
      // Fetch metadata for this column
      val colName = node.getRowType.getFieldNames.get(idx)
      val colProperties = Schema.getSchemaMapForTable(tableName)(colName).properties
      val colMaxFreq = colProperties.get("maxFreq").fold(Double.PositiveInfinity)(_.toDouble)
      val maxFreqAtK = if (isTablePublic) colMaxFreq else (colMaxFreq + k)
      val canRelease = colProperties.get("canRelease").fold(false)(_.toBoolean) || isTablePublic

      colState.copy(
        maxFreq = maxFreqAtK,
        canRelease = canRelease
      )
    }

    val newNodeFact = state.nodeFact.copy(
      isPublic = isTablePublic,
      /** Public table optimization: on joins with public tables, new stability is equal to the maxFreq of the joined
        * column from the public table times stability of the other relation.
        *
        * Setting initial stability to zero for public tables produces exactly this result, as the other terms
        * fall out (see stability calculation in transferJoin). Additionally, it ensures that counting queries on
        * only-public data will produce sensitivity of zero (i.e., no noise) without requiring any additional logic.
        *
        * Note that max column frequencies are always updated at joins regardless of public-ness.
        */
      stability = if (isTablePublic) 0.0 else 1.0,
      ancestors = Set(tableName)
    )

    NodeColumnFacts(newNodeFact, newColFacts)
  }

  override def transferExpression(node: RexNode, state: ColSensitivity): ColSensitivity = {

    node match {
      case c: RexCall =>
        // Equality and cast expressions should not trigger post aggregation arithmetic exceptions
        val isArithmeticExpression = !List(SqlKind.EQUALS, SqlKind.CAST).contains(c.getOperator.getKind)

        // We could model standard SQL functions here but for this implementation we treat every function conservatively.
        state.copy(
          sensitivity = Some(Double.PositiveInfinity),
          maxFreq = Double.PositiveInfinity,
          postAggregationArithmeticApplied = state.aggregationApplied && isArithmeticExpression)

      case _ =>
        // conservatively handle all other expression nodes, which could arbitrarily alter column values such that
        // current metrics are invalidated (e.g., in the case of arithmetic expressions).
        state.copy(maxFreq = Double.PositiveInfinity)
    }
  }

  override def transferJoin(node: Join, state: NodeColumnFacts[RelStability,ColSensitivity]): NodeColumnFacts[RelStability,ColSensitivity] = {
    /** Update the stability at every join, per elastic sensitivity definition. Throws UnsupportedConstructException
      * if node contains no equijoin conditions, or uses other types of join predicates. See test cases for more info.
      *
      * If the join has more than one AND-ed equijoin condition (e.g., SELECT a JOIN b ON a.x = b.x AND a.y = b.y) we
      * evaluate each condition and select the most restrictive, i.e., the one producing the lowest stability. This is
      * sound because each conjunction predicate adds further restrictions that may decrease (but will never increase)
      * the true stability of the join.
      */
    val conjunctiveClauses = RelUtils.decomposeConjunction(node.getCondition)
    val equijoinConjuncts = conjunctiveClauses.flatMap { clause => RelUtils.extractEquiJoinColumns(node, clause) }
    if (equijoinConjuncts.isEmpty)
      throw new UnsupportedConstructException(s"This analysis only works on equijoins. Can't support join condition: ${node.getCondition.toString}")

    val leftState = resultMap(Relation(node.getLeft))
    val rightState = resultMap(Relation(node.getRight))

    val leftStability = leftState.nodeFact.stability
    val rightStability = rightState.nodeFact.stability

    // Determine if this is a self-join: get the intersection of ancestors for the left and right relations
    // If the intersection is not empty, then this is a self-join
    val isSelfJoin = (leftState.nodeFact.ancestors intersect rightState.nodeFact.ancestors).nonEmpty

    case class ConjunctResult(maxFreqLeftJoinColumn: Double, maxFreqRightJoinColumn: Double, stability: Double)

    // Calculate stability for each conjunct, then select the one producing the tightest stability.
    val conjunctResults = equijoinConjuncts.map { case (leftColumnIndex, rightColumnIndex) =>

      val leftColFact = leftState.colFacts(leftColumnIndex)
      val rightColFact = rightState.colFacts(rightColumnIndex)

      val maxFreqLeftJoinColumn = leftColFact.maxFreq
      val maxFreqRightJoinColumn = rightColFact.maxFreq

      // Calculate new stability according to elastic sensitivity definition
      val newStability =
        if (isSelfJoin)
          maxFreqLeftJoinColumn * rightStability + maxFreqRightJoinColumn * leftStability + rightStability * leftStability
        else
          Math.max(maxFreqRightJoinColumn * leftStability, maxFreqLeftJoinColumn * rightStability)

      // Print a helpful error message if any metrics are missing (resulting in infinite stability)
      if (newStability == Double.PositiveInfinity)
        throw new MissingMetricException()

      ConjunctResult(maxFreqLeftJoinColumn, maxFreqRightJoinColumn, newStability)
    }

    val newNodeState = state.nodeFact.copy(
      stability = conjunctResults.map{ _.stability }.min
    )

    /** Update the max frequency for every column by a factor of the max frequency of the join key in the opposing
      * relation. This models the worst-case situation where each record containing the most-frequent-key is duplicated
      * this many times by the join.
      *
      * By the same reasoning above, if the join contains a conjunction of equijoin clauses it suffices to consider the
      * one producing the lowest duplication, since each constraint serves only to further decrease the number of join
      * matches.
      */
    val lowestMaxFreqLeft = conjunctResults.map{ _.maxFreqLeftJoinColumn }.min
    val lowestMaxFreqRight = conjunctResults.map{ _.maxFreqRightJoinColumn }.min
    val newColState =
      leftState.colFacts.map { x => x.copy(maxFreq = x.maxFreq * lowestMaxFreqRight) } ++
      rightState.colFacts.map { x => x.copy(maxFreq = x.maxFreq * lowestMaxFreqLeft) }

    NodeColumnFacts(newNodeState, newColState)
  }
}

/** Thrown if the query joins on column for which the maxFreq metric is missing, either because it
  * is not defined in the schema config or because the query alters the column in a way that invalidates the metric.
  */
class MissingMetricException() extends UnsupportedQueryException(
  "Missing maxFreq metrics for one or more equijoin columns. Either the metric is undefined or the query uses a derived join key."
)

/** Thrown if the query computes a histogram using one or more protected bin labels, in which the bin labels must be
  * made differentially privacy.
  */
class ProtectedBinException(binName: String) extends UnsupportedQueryException(
  s"This query returns a histogram bin column ('$binName') that is not safe for release. " +
    "The bin labels must be made differentially private, which requires additional data model knowledge. " +
    "If you know what you're doing, you can disable this message by setting canRelease=true for columns of protected " +
    " tables that are safe for release, or setting flag -Ddp.check_bins=false to disable this check entirely."
)

/** Thrown if operations/functions were applied to post-aggregated values; this check is not needed for soundness of the
  * analysis but allows us to print a helpful error message rather than returning a sensitivity of infinity with no
  * explanation.
  */
class ArithmeticOnAggregationResultException(colName: String) extends UnsupportedQueryException(
  s"Arithmetic or function applied to aggregation results; sensitivity cannot be determined (output column '$colName')"
)
