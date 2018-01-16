package com.uber.engsec.dp.rewriting.mechanism

import com.uber.engsec.dp.analysis.histogram.{HistogramAnalysis, QueryType}
import com.uber.engsec.dp.dataflow.AggFunctions.{AVG, COUNT, SUM}
import com.uber.engsec.dp.dataflow.domain.UnitDomain
import com.uber.engsec.dp.exception.UnsupportedQueryException
import com.uber.engsec.dp.rewriting.Rewriter
import com.uber.engsec.dp.rewriting.rules.ColumnDefinition.{rel, _}
import com.uber.engsec.dp.rewriting.rules.Expr.{Sum, col, _}
import com.uber.engsec.dp.rewriting.rules.Operations._
import com.uber.engsec.dp.rewriting.rules._
import com.uber.engsec.dp.schema.Schema
import com.uber.engsec.dp.sql.relational_algebra.Relation
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.rules.FilterProjectTransposeRule

class SampleAndAggregateRewriter extends Rewriter[SampleAndAggregateConfig] {
  /** Returns the number of partitions to use for the given table. Default is pow(n, 0.4), where n is the size of the
    * dataset, as recommended by Mohan et al. (https://dl.acm.org/citation.cfm?id=2213876). Selection of this
    * value does not affect the privacy guarantee but may impact utility.
    */
  def getNumPartitions(tableName: String): Int = {
    val approxRowCount = Schema.getTableProperties(tableName).getOrElse("approxRowCount",
      throw new IllegalArgumentException(s"Must define metric 'approxRowCount' for table '$tableName'")).toLong

    math.floor(math.pow(approxRowCount, 0.4)).toInt
  }

  /** Returns an expression that partitions the table into distinct groups of approximately equal size.
    * The expression should return [numGroups] different values (the values themselves don't matter). The expression
    * may be derived from columns in the relation, the row index, etc.
    *
    * IMPORTANT: this function must be deterministic. The grouping expression is referenced several times in the
    * rewritten query, hence the database may evaluate it multiple time, leading to undefined (and incorrect!) behavior
    * if the value changes between evaluations. Note this behavior occurs even if the databases is configured to
    * materialize views, since such directives may be ignored for views using nondeterministic functions.
    */
  def getPartitionExpression(tableName: String, numPartitions: Int): ValueExpr = { RowNumber % (numPartitions-1) }

  /** Returns a randomly sampled record according to probability density value in column [prob].
    * The probabilities must be normalized (i.e. they must sum to 1). The rows *do not* need be sorted by [prob].
    *
    * [primaryKey] is a column containing a unique row key. This column must be an comparable data type in SQL, as it
    * it is also used to sort the rows when computing the cumulative probability.
    */
  def sampleRowFromDistribution(relation: Relation, prob: ColumnReferenceByName, primaryKey: ColumnReferenceByName): Relation = relation
    /** Calculate cumulative probabilities, sorting rows by the primary key column */
    .filter (prob > 0)
    .joinSelf (s"_probs_${prob.name}", left(primaryKey) <= right(primaryKey))
    .agg (left(primaryKey)) (Sum(right(prob)) AS "_cdf")

    /** Sample from uniform distribution [0,1) and find row with lowest cumulative probability value greater than sampled value */
    .join (rel(Rand AS "_uniform"), true)
    .filter (col("_cdf") >= col("_uniform"))
    .sort (col("_cdf"))
    .fetch (1)
    .project (primaryKey)

    /** Join with the original table to return all columns from sampled row. */
    .join(relation, left(primaryKey) == right(primaryKey))
    .project(relation.* : _*)

  /** Creates an expression which clamps the given expression within the specified range */
  def clamp(expr: ValueExpr, min: ValueExpr, max: ValueExpr): ValueExpr = Case(expr < min, min, Case(expr > max, max, expr))

  /** Rewriting function. */
  def rewrite(root: Relation, config: SampleAndAggregateConfig): Relation = {
    // Reject unsupported queries.
    val histogramResults = new HistogramAnalysis().run(root)
    val queryType = QueryType.getQueryType(histogramResults)

    val aggregatedColumns = histogramResults.colFacts.filter(_.isAggregation)
    if ((aggregatedColumns.length > 1) || (queryType != QueryType.NON_HISTOGRAM_STATISTICAL))
      throw new UnsupportedQueryException("This mechanism currently works only for single-column, non-histogram queries")
    else if (!aggregatedColumns.forall(col => Set(AVG, SUM, COUNT).exists( col.outermostAggregation.contains(_) )))
      throw new UnsupportedQueryException("This mechanism supports aggregation functions: AVG, SUM, and COUNT")

    // The target column is the first aggregated (non-binned) column
    val targetColIdx = histogramResults.colFacts.indexWhere( _.isAggregation )
    val targetColOriginalName = root.getRowType.getFieldNames.get(targetColIdx)
    val targetColAggregation = histogramResults.colFacts(targetColIdx)
    val targetCol = col("_col")

    if (targetColAggregation.references.isEmpty)
      throw new UnsupportedQueryException("Query does not reference any tables")
    else if (targetColAggregation.references.size > 1)
      throw new UnsupportedQueryException(s"Aggregation in column $targetColIdx is derived from multiple sources")

    // Set S&A parameter values based on metrics of table/column being aggregated.
    val targetColSource = targetColAggregation.references.head
    val (tableName, colName) = (targetColSource.table, targetColSource.column)

    val numPartitions = getNumPartitions(tableName)
    val groupingExpression = getPartitionExpression(tableName, numPartitions) AS "_grp"
    val isStatisticalEstimator = targetColAggregation.outermostAggregation.contains(AVG)

    // If aggregation function is not statistical estimator, scale the differentially private result by the number of partitions.
    val scalingFactor = if (isStatisticalEstimator) 1 else numPartitions

    /** Step 1: Rewrite original query so that data tables are divided into k partitions and aggregation is applied to
      * each partition independently.
      */
    val queryWithPartitionColumn = root.mapCols { col =>
      /** Give the target column an explicit alias. This is done for two reasons. First, the original query may have
        * left the column name unspecified and the rewritten query needs to reference this column. (We could attempt
        * to infer the column name but this would require database-specific logic.) Additionally, multiple output
        * columns may share the same alias; in such cases the rewritten query would raise an "ambiguous column
        * reference" error in the database. Assigning a unique alias addresses both issues.
        */
      if (col.idx == targetColIdx)
        /** Adding zero is due to a quirk in Calcite's RelToSql code; if we project only column references,
          * Calcite will ignore the aliases specified in the projection and use the schema (and column name) of the projection's
          * input. Projecting a compound expression prevents this from happening.
          */
        col.expr+0 AS targetCol.name
      else
        col
    }.rewriteRecursive(UnitDomain){ (node, _, _) =>
      node match {
        // Add new partition column to table.
        case Relation(t: TableScan) => (node.project(*, groupingExpression), ())

        // Ensure the grouping column is projected through existing project nodes.
        case Relation(p: Project) => (p.reproject(*, col("_grp")), ())

        // Group by the partition column at each aggregation node.
        case Relation(a: Aggregate) => (a addGroupedColumn col("_grp"), ())

        case Relation(j: Join) => throw new UnsupportedQueryException("Sample and Aggregate does not support queries with join")

        case _ => (node, ())
      }
    }.optimize(FilterProjectTransposeRule.INSTANCE)  // push filters down to grouping expression relation, if possible, to improve query performance
      .asAlias("_partitioned_query")

    /** Step 2: Using the exponential mechanism, calculate differentially private quartiles (1/4 and 3/4) of partitioned
      * aggregation results, allocating one-quarter of the total privacy budget to each quartile calculation.
      */
    val rowNumber = RowNumber AS "idx"
    val clampedColumn = clamp(targetCol, 0, config.lambda) AS s"_clamped_${targetCol.name}"
    val y_14 = (left(clampedColumn) - right(clampedColumn)) * Exp(-1 * (config.epsilon/4) * Abs(left(rowNumber) - 0.25 * numPartitions)) AS "_y_14"
    val y_34 = (left(clampedColumn) - right(clampedColumn)) * Exp(-1 * (config.epsilon/4) * Abs(left(rowNumber) - 0.75 * numPartitions)) AS "_y_34"

    val y_calc = queryWithPartitionColumn
      .project (targetCol)
      .union (rel(0: Expr))
      .union (rel(config.lambda: Expr))
      .project (clampedColumn)
      .sort (clampedColumn)
      .project (*, rowNumber)
      .joinSelf ("_clamped_with_idx", left(rowNumber) == right(rowNumber) + 1)
      .project (*,
        left(clampedColumn) AS "_range_max",
        right(clampedColumn) AS "_range_min",
        y_14,
        y_34)
      .asAlias ("_y_calc")

    val probs = y_calc
      .agg () (Sum(col(y_14)) AS "y_14_normalizing", Sum(col(y_34)) AS "y_34_normalizing")
      .join (y_calc, true)
      .project (*,
        y_14 / col("y_14_normalizing") AS "_y_14_normalized_prob",
        y_34 / col("y_34_normalizing") AS "_y_34_normalized_prob")
      .asAlias("_prob_table")

    val sampledRow_14 = sampleRowFromDistribution(probs, col("_y_14_normalized_prob"), col(rowNumber))
      .project( Rand * (col("_range_max") - col("_range_min")) + col("_range_min") AS "_draw_result_14" )

    val sampledRow_34 = sampleRowFromDistribution(probs, col("_y_34_normalized_prob"), col(rowNumber))
      .project( Rand * (col("_range_max") - col("_range_min")) + col("_range_min") AS "_draw_result_34" )

    val sampledResult = sampledRow_14.join(sampledRow_34, true)

    /** Step 3: Estimate the range [u,l] of partitioned aggregation values from differentially private quartiles.
      */
    val rad: ValueExpr = math.pow(numPartitions, 1.0 / 3 + 1.0 / 10)
    val privateBounds = sampledResult
      .project(*,
        (col("_draw_result_14") + col("_draw_result_34"))/2 AS "u_crude",
        Abs(col("_draw_result_34") - col("_draw_result_14")) AS "iqr_crude")
      .project(
        col("u_crude") + 4 * rad * col("iqr_crude") AS "u",
        col("u_crude") - 4 * rad * col("iqr_crude") AS "l")
      .asAlias("_private_bounds")

    /** Step 4: Calculate winsorized mean by clamping aggregation results to this range and computing the mean value
      * across all partitions.
      */
    val privateClampedTbl = queryWithPartitionColumn
      .join(privateBounds, true)
      .project(*, clamp(targetCol, col("l"), col("u")) AS "_private_clamped")
      .agg () (Sum(col("_private_clamped")) AS "_private_sum")
      .project(*, col("_private_sum") / numPartitions AS "_u_private")

    /** Step 5: Add Laplace noise with scale |u-l|/2*epsilon*k to winsorized mean to obtain differentially
      * private result.
      */
    val privateResult = privateClampedTbl
      .join(privateBounds.project(col("l"), col("u")).fetch(1), true)
      .project((col("_u_private") + (Abs(col("u") - col("l"))/(2 * config.epsilon * numPartitions)) * DPExpr.LaplaceSample) * scalingFactor AS s"${targetCol.name}_private")

    privateResult
  }
}

case class SampleAndAggregateConfig(
 /** The privacy budget. */
 epsilon: Double,

 /** The lambda parameter is an upper bound for aggregated results within each partition. The algorithm clips values
   * above this threshold. While there is no definitive rule for selecting an optimal value of lambda, the general
   * goal is to use a value that is moderately conservative, so few values are clipped but no so great as to produce
   * a disproportionately large bin that skews the quartile estimation in Step #2.
   */
 lambda: Double
)