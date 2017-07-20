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
import com.uber.engsec.dp.dataflow.column.AbstractColumnAnalysis.ColumnFacts
import com.uber.engsec.dp.dataflow.column.RelColumnAnalysis
import com.uber.engsec.dp.exception.{UnsupportedConstructException, UnsupportedQueryException}
import com.uber.engsec.dp.schema.Schema
import com.uber.engsec.dp.sql.relational_algebra._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Aggregate, Join, Project, TableScan}
import org.apache.calcite.rex.{RexCall, RexLiteral, RexNode}
import org.apache.calcite.sql.SqlKind

/** Elastic sensitivity analysis.
  *
  * Calculates an upper bound on the local sensitivity of a query, based on the query, information about the maximum
  * frequency of join keys used in the query, and constraints on the data model of the database being queried.
  * Elastic sensitivity can be combined with a smoothing function (such as smooth sensitivity) to arrive at a
  * sensitivity value that can be used to calibrate the scale of Laplace noise needed to achieve differential privacy.
  */
class ElasticSensitivityAnalysis extends RelColumnAnalysis(SensitivityDomain) {

  var k: Int = 0
  val checkBinsForRelease: Boolean = System.getProperty("dp.check_bins", "true").toBoolean

  /** Reject non-statistical queries before analysis begins. */
  override def run(root: RelOrExpr): ColumnFacts[SensitivityInfo] = {
    val histogramResults = new HistogramAnalysis().run(root)
    val queryType = QueryType.getQueryType(histogramResults)

    if ((queryType != QueryType.HISTOGRAM && queryType != QueryType.NON_HISTOGRAM_STATISTICAL) ||
        histogramResults.filter{ _.isAggregation }.exists{ _.outermostAggregation.get != COUNT })
      throw new UnsupportedQueryException("This analysis currently works only on statistical and histogram counting queries")

    val results = super.run(root)

    // Print helpful error message if any column sensitivity is infinite due to post-processed aggregation results
    // (e.g., SELECT COUNT(*)+1000 FROM ORDERS), which this analysis does not currently model.
    val idx = results.indexWhere{ _.postAggregationArithmeticApplied }
    if (idx != -1) {
      val colName = root.unwrap.asInstanceOf[RelNode].getRowType.getFieldNames.get(idx)
      throw new ArithmeticOnAggregationResultException(colName)
    }

    results
  }

  /** Set the distance ''k'' from the true database for the elastic sensitivity calculation
    *
    * @param k Desired distance from true database (default 0, to approximate local sensitivity)
    */
  def setK(k: Int): Unit = this.k = k

  import scala.collection.JavaConverters._

  override def transferAggregate(node: Aggregate, idx: Int, aggFunction: Option[AggFunction], state: SensitivityInfo): SensitivityInfo = {
    aggFunction match {
      case None => // histogram bin
        if (checkBinsForRelease && !state.canRelease) {
          val binName = node.getRowType.getFieldNames.get(idx)
          throw new ProtectedBinException(binName)
        }

        // Sensitivity of bins that are safe for release is zero (i.e. no noise added)
        state.copy(sensitivity=Some(0.0))

      case Some(COUNT) =>
        // The sensitivity of count() is the stability of the target node (stored in the current column fact)
        state.copy(
          sensitivity=Some(state.stability),
          maxFreq=Double.PositiveInfinity,
          aggregationApplied=true,
          postAggregationArithmeticApplied=state.aggregationApplied,
          isUnique=false,
          canRelease=false
        )

      case _ => // other aggregation functions -> return Top (conservative)
        state.copy(
          sensitivity=Some(Double.PositiveInfinity),
          stability=Double.PositiveInfinity,
          maxFreq=Double.PositiveInfinity,
          aggregationApplied=true,
          postAggregationArithmeticApplied=state.aggregationApplied,
          isUnique=false,
          canRelease=false
        )
    }
  }


  override def transferProject(node: Project, idx: Int, state: SensitivityInfo): SensitivityInfo = {
    val projectNode = node.getProjects.get(idx)

    // In our relational algebra trees, COUNT(*) is expressed as a COUNT of the projection of literal value 0 on the
    // input. Accordingly, for projections of literal values, for which we would otherwise miss the implicit stability
    // dependence on the .input relation, we expliticly copy the stability of the input so the correct stability wil
    // propagate any upstream COUNT aggregation.
    projectNode match {
      case r: RexLiteral =>
        val inputState = resultMap(node.getInput).head
        state.copy(stability=inputState.stability)

      case _ => state
    }
  }

  override def transferTableScan(node: TableScan, idx: Int, state: SensitivityInfo): SensitivityInfo = {
    val tableName = node.getTable.getQualifiedName.asScala.mkString(".")
    val colName = node.getRowType.getFieldNames.get(idx)

    val colProperties = Schema.getSchemaMapForTable(tableName)(colName).properties
    val isUnique = colProperties.get("isUnique").fold(false)(_.toBoolean)
    val maxFreq = if (isUnique) 1.0 else colProperties.get("maxFreq").fold(Double.PositiveInfinity)(_.toDouble) + k
    val canRelease = colProperties.get("canRelease").fold(false)(_.toBoolean) || getTableProperties(node).get("isPublic").fold(false)(_.toBoolean)

    SensitivityInfo(
      sensitivity=None, // sensitivity is undefined until aggregations are applied
      stability=1.0,  // stability of tables (before SQL joins) is 1.0
      maxFreq=maxFreq,
      isUnique=isUnique,
      optimizationUsed=false,
      aggregationApplied=false,
      postAggregationArithmeticApplied=false,
      canRelease=canRelease,
      ancestors=Set(tableName)
    )
  }

  override def transferExpression(node: RexNode, state: SensitivityInfo): SensitivityInfo = {
    node match {
      case c: RexCall =>
        // We could model standard SQL functions here but for this implementation we treat every function conservatively.
        state.copy(
          sensitivity = Some(Double.PositiveInfinity),
          maxFreq = Double.PositiveInfinity,
          isUnique = false,
          postAggregationArithmeticApplied = state.aggregationApplied && !List(SqlKind.EQUALS, SqlKind.CAST).contains(c.getOperator.getKind)
        )
      case _ =>
        // conservatively handle all other expression nodes, which could arbitrarily alter column values such that
        // current metrics are invalidated (e.g., in the case of arithmetic expressions).
        state.copy(
          maxFreq = Double.PositiveInfinity,
          isUnique = false
        )
    }
  }

  override def joinNode(node: RelOrExpr, children: Iterable[RelOrExpr]): ColumnFacts[SensitivityInfo] = {
    node match {
      case Relation(j: Join) =>
        // Stability must be updated at every Join node in the query.

        val (leftColumnIndex, rightColumnIndex) =
          RelUtils.extractEquiJoinColumns(j)
                  .getOrElse(throw new UnsupportedConstructException(s"This analysis only works on equijoins. Can't support join condition: ${j.getCondition.toString}"))

        val conditionDomains = resultMap(Expression(j.getCondition))
        var optimizationUsed = false

        val leftState = resultMap(Relation(j.getLeft))
        val rightState = resultMap(Relation(j.getRight))

        val leftColFact = leftState(leftColumnIndex)
        val rightColFact = rightState(rightColumnIndex)

        val maxFreqLeftJoinColumn = leftColFact.maxFreq
        val maxFreqRightJoinColumn = rightColFact.maxFreq

        val leftStability = leftColFact.stability
        val rightStability = rightColFact.stability

        var newStaticStability = 0.0

        // Determine if this is a self-join: get the intersection of ancestors for the left and right relations
        // If the intersection is not empty, then this is a self-join
        val allAncestors = leftColFact.ancestors intersect rightColFact.ancestors
        if (allAncestors.nonEmpty) {
          // self-join case
          newStaticStability = maxFreqLeftJoinColumn*rightStability +
            maxFreqRightJoinColumn*leftStability +
            rightStability*leftStability
        } else {
          // non self-join case
          newStaticStability = Math.max(leftStability * maxFreqRightJoinColumn, rightStability * maxFreqLeftJoinColumn)
        }

        // Uniqueness optimization.
        if (leftColFact.isUnique || rightColFact.isUnique) {
          if (leftColFact.isUnique) {
            newStaticStability = leftStability
          } else {
            newStaticStability = rightStability
          }
          optimizationUsed = true
        }

        // Public table optimization.
        j.getRight match {
          case d: TableScan if getTableProperties(d).getOrElse("isPublic", "false") equals "true" =>
            newStaticStability = math.min(newStaticStability, rightStability * maxFreqLeftJoinColumn)
            optimizationUsed = true
          case _ => ()
        }

        j.getLeft match {
          case d: TableScan if getTableProperties(d).getOrElse("isPublic", "false") equals "true" =>
            newStaticStability = math.min(newStaticStability, leftStability * maxFreqRightJoinColumn)
            optimizationUsed = true
          case _ => ()
        }

        // Print a helpful error message if metrics are missing (resulting in infinite stability)
        if (newStaticStability == Double.PositiveInfinity)
          throw new MissingMetricException()

        List(j.getLeft, j.getRight).flatMap { relation =>
          val maxFreqOfOtherRelationJoinKey = if (relation eq j.getLeft) maxFreqRightJoinColumn else maxFreqLeftJoinColumn
          val childState = resultMap(relation)

          childState.map { fact =>
            fact.copy(
              optimizationUsed=fact.optimizationUsed || optimizationUsed,
              stability=newStaticStability,
              // The max frequency of each column changes by a factor of the max frequency of the join key in the other relation.
              maxFreq=fact.maxFreq * maxFreqOfOtherRelationJoinKey
            )
          }
        }.toIndexedSeq

      case _ => super.joinNode(node, children)
    }
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