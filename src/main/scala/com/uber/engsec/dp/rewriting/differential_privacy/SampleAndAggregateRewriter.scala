package com.uber.engsec.dp.rewriting.differential_privacy

import com.uber.engsec.dp.analysis.histogram.{HistogramAnalysis, QueryType}
import com.uber.engsec.dp.dataflow.AggFunctions.{AVG, COUNT, SUM}
import com.uber.engsec.dp.dataflow.domain.UnitDomain
import com.uber.engsec.dp.exception.UnsupportedQueryException
import com.uber.engsec.dp.rewriting.rules.ColumnDefinition.{rel, _}
import com.uber.engsec.dp.rewriting.rules.Expr.{Sum, col, _}
import com.uber.engsec.dp.rewriting.rules.Operations._
import com.uber.engsec.dp.rewriting.rules._
import com.uber.engsec.dp.rewriting.{DPRewriterConfig, DPUtil, Rewriter}
import com.uber.engsec.dp.schema.{Database, Schema}
import com.uber.engsec.dp.sql.relational_algebra.Relation
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.rules.FilterProjectTransposeRule

class SampleAndAggregateRewriter(config: SampleAndAggregateConfig) extends Rewriter(config) {
  def rewrite(root: Relation): Relation = {
    val histogramResults = new HistogramAnalysis().run(root, config.database)
    val queryType = QueryType.getQueryType(histogramResults)
    val aggregatedColumns = histogramResults.colFacts.filter(_.isAggregation)

    // Reject unsupported queries.
    if ((aggregatedColumns.length > 1) || (queryType != QueryType.NON_HISTOGRAM_STATISTICAL))
      throw new UnsupportedQueryException("This mechanism currently works only for single-column, non-histogram queries")
    else if (!aggregatedColumns.forall(col => Set(AVG, SUM, COUNT).exists( col.outermostAggregation.contains(_) )))
      throw new UnsupportedQueryException("This rewriter supports aggregation functions: AVG, SUM, and COUNT")

    // The target column is the first aggregated (non-binned) column
    val targetColIdx = histogramResults.colFacts.indexWhere( _.isAggregation )
    val targetColOriginalName = root.getRowType.getFieldNames.get(targetColIdx)
    val targetColAggregation = histogramResults.colFacts(targetColIdx)
    val targetCol = col("_col")

    if (targetColAggregation.references.isEmpty)
      throw new UnsupportedQueryException("Query does not reference any tables")
    else if (targetColAggregation.references.size > 1)
      throw new UnsupportedQueryException(s"Aggregation in column $targetColIdx is derived from multiple tables")

    // Set S&A parameter values based on metrics of table/column being aggregated.
    val targetColSource = targetColAggregation.references.head
    val (tableName, colName) = (targetColSource.table, targetColSource.column)

    val numPartitions = config.getNumPartitions(tableName)
    val rad = config.getWideningFactor(numPartitions)

    val groupingExpression = config.getPartitionExpression(tableName, numPartitions) AS "_grp"
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
        * reference" error in the database.
        */
      if (col.idx == targetColIdx)
        EnsureAlias(col.expr) AS targetCol.name
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
    val clampedColumn = clamp(targetCol, 0, config.lambda) AS s"_clamped_${targetCol.name}"

    val y_calc = queryWithPartitionColumn
      .project (targetCol)
      .union (rel(0: Expr))
      .union (rel(config.lambda: Expr))
      .project (clampedColumn)
      .sort (clampedColumn)
      .project (*, RowNumber AS "_idx")
      .joinSelf ("_clamped_with_idx", left(col("_idx")) == right(col("_idx")) + 1)
      .project (
        right(col("_idx"))-1 AS "_i",
        right(clampedColumn) AS "_z_i",
        left(clampedColumn) AS "_z_i_next"
      )
      .project(*,
        (col("_z_i_next") - col("_z_i")) * Exp(-1 * (config.epsilon/4) * Abs(col("_i") - 0.25 * numPartitions)) AS "_y_i_1qrt",
        (col("_z_i_next") - col("_z_i")) * Exp(-1 * (config.epsilon/4) * Abs(col("_i") - 0.75 * numPartitions)) AS "_y_i_3qrt"
      )
      .asAlias ("_y_calc")

    val probs = y_calc
      .agg () (Sum(col("_y_i_1qrt")) AS "_y_1qrt_sum", Sum(col("_y_i_3qrt")) AS "_y_3qrt_sum")
      .join (y_calc, true)
      .project (*,
        col("_y_i_1qrt") / col("_y_1qrt_sum") AS "_y_i_1qrt_normalized",
        col("_y_i_3qrt") / col("_y_3qrt_sum") AS "_y_i_3qrt_normalized")
      .asAlias("_prob_tbl")

    val sampledRow_14 = sampleRowFromDistribution(probs, col("_y_i_1qrt_normalized"), col("_i"))
      .project( Rand * (col("_z_i_next") - col("_z_i")) + col("_z_i") AS "_draw_result_1qrt" )

    val sampledRow_34 = sampleRowFromDistribution(probs, col("_y_i_3qrt_normalized"), col("_i"))
      .project( Rand * (col("_z_i_next") - col("_z_i")) + col("_z_i") AS "_draw_result_3qrt" )

    val sampledResult = sampledRow_14.join(sampledRow_34, true)

    /** Step 3: Estimate the range [u,l] of partitioned aggregation values from differentially private quartiles.
      */
    val privateBounds = sampledResult
      .project(*,
        (col("_draw_result_1qrt") + col("_draw_result_3qrt")) / 2 AS "u_crude",
        Abs(col("_draw_result_3qrt") - col("_draw_result_1qrt")) AS "iqr_crude")
      .project(
        col("u_crude") + 4 * rad * col("iqr_crude") AS "u",
        col("u_crude") - 4 * rad * col("iqr_crude") AS "l")
      .asAlias("_priv_bounds")

    /** Step 4: Calculate winsorized mean by clamping aggregation results to this range and computing the mean value
      * across all partitions.
      */
    val privateClampedTbl = queryWithPartitionColumn
      .join(privateBounds, true)
      .project(*, clamp(targetCol, col("l"), col("u")) AS "_private_clamped")
      .agg () (Sum(col("_private_clamped")) AS "_private_sum")
      .project(*, col("_private_sum") / numPartitions AS "_winsorized_mean")

    /** Step 5: Add Laplace noise with scale |u-l|/2*epsilon*k to winsorized mean to obtain differentially
      * private result.
      */
    val privateResult = privateClampedTbl
      .join(privateBounds.project(col("l"), col("u")).fetch(1), true)
      .project(col("_winsorized_mean"), Abs(col("u") - col("l"))/(2 * config.epsilon * numPartitions) AS "_laplace_scale")
      .project((col("_winsorized_mean") + (col("_laplace_scale") * DPUtil.LaplaceSample)) * scalingFactor AS s"${targetCol.name}_private")

    privateResult
  }

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
}

class SampleAndAggregateConfig(
    override val epsilon: Double,
    /** The lambda parameter is an upper bound for aggregated results within each partition. The algorithm clips values
      * above this threshold. While there is no definitive rule for selecting an optimal value of lambda, the general
      * goal is to use a value that is moderately conservative, so few values are clipped but no so great as to produce
      * a disproportionately large bin that skews the quartile estimation in Step #2.
      */
    val lambda: Double,
    override val database: Database)
  extends DPRewriterConfig(epsilon, database, false) {
  /** Returns the number of partitions to use for the given table. This value does not affect the privacy guarantee but
    * may impact utility. Default is pow(n, 0.4), where n is the size of the dataset, as recommended by Mohan et al.
    * If using this default implementation, the table schema must include a property 'approxRowCount' that contains the
    * approximate cardinality of the table.
    *
    * @see [[https://dl.acm.org/citation.cfm?id=2213876 GUPT: Privacy Preserving Data Analysis Made Easy]]
    */
  def getNumPartitions(tableName: String): Int = {
    val approxRowCount = Schema.getTableProperties(database, tableName).getOrElse("approxRowCount",
      throw new IllegalArgumentException(s"Must define metric 'approxRowCount' for table '$tableName' in database '$database'")).toLong

    math.floor(math.pow(approxRowCount, 0.4)).toInt
  }

  /** Returns an expression that partitions the table into distinct groups of approximately equal size. The expression
    * should return [numPartitions] distinct values (the actual values don't matter). The expression may be derived from
    * columns in the relation, the row index, etc.
    *
    * IMPORTANT: this function must be deterministic. The grouping expression is referenced several times in the
    * rewritten query, hence the database may evaluate it multiple time, leading to undefined (and incorrect!) behavior
    * if the value changes between evaluations. Note this behavior occurs even if the databases is configured to
    * materialize views, since such directives may be ignored for views using nondeterministic functions.
    */
  def getPartitionExpression(tableName: String, numPartitions: Int): ValueExpr = { RowNumber % numPartitions }

  /** Returns the widening factor. This value is used to widen the range of the first and third quartiles obtained in
    * Step #2, in order to arrive at an "effective" interquartile range that results in minimal clipping. Note that the
    * Laplace noise added in Step 5 is proportional to the size of this widened range. Therefore, selection of this
    * value presents a tradeoff: a value too small risks skewing the winsorized mean due to excessive clipping, while a
    * value too large risks more noise added to the winsorized mean, potentially counteracting the benefit of less
    * clipping. The value below, proposed by Smith, was selected to prove optimal asymptotic convergence rates; the best
    * value in practice will depend on the data and query.
    *
    * @see [[https://dl.acm.org/ft_gateway.cfm?id=1993743 Privacy-preserving Statistical Estimation with Optimal Convergence Rates]]
    */
  def getWideningFactor(numPartitions: Int): Double = math.pow(numPartitions, 1.0 / 3 + 1.0 / 10)
}
