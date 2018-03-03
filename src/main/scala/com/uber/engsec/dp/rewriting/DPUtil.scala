package com.uber.engsec.dp.rewriting

import com.uber.engsec.dp.analysis.histogram.AggregationInfo
import com.uber.engsec.dp.dataflow.column.AbstractColumnAnalysis.ColumnFacts
import com.uber.engsec.dp.rewriting.rules.ColumnDefinition._
import com.uber.engsec.dp.rewriting.rules.Expr.{Abs, Case, Ln, Rand, _}
import com.uber.engsec.dp.rewriting.rules.Operations._
import com.uber.engsec.dp.rewriting.rules.{Helpers, ValueExpr}
import com.uber.engsec.dp.schema.Schema
import com.uber.engsec.dp.sql.relational_algebra.Relation
import org.apache.calcite.rel.core._

/** Utilities for differential privacy-based rewriters. */
object DPUtil {
  // Expression to sample a random value from the Laplace distribution.
  val LaplaceSample: ValueExpr = Case((Rand-0.5) < 0, -1.0, 1.0) * Ln(1 - (2 * Abs(Rand-0.5)))

  /** Rewrites the relation to add all values in the domain of the binned column that are not present in the result set
    * if necessary (as determined by the [fillMissingBins] flag). If the provided aggregation contains no grouped
    * columns, returns the original relation.
    *
    * To support this feature the schema must define flag 'domainSet' for any database column usable as a histogram bin.
    * The value of this flag is a fully qualified column in the same database whose records enumerate all values in that
    * column's domain. This flag may point to itself (e.g., if the column's values already span the domain) or it may
    * refer to an auxiliary table. If the flag is not defined, this method throws an error since the rewritten query's
    * result cannot be safely returned; in such cases the mechanism must either perform additional processing on the
    * results or interpose between the results and analyst.
    */
  def addBinsFromDomain(node: Aggregate,
                        histogramResults: ColumnFacts[AggregationInfo],
                        config: DPRewriterConfig): Relation = {
    import scala.collection.JavaConverters._

    if (!config.fillMissingBins)
      return Relation(node)

    val cols = node.getRowType.getFieldList.asScala
    val (groupedCols, aggCols) = cols.splitAt(node.getGroupCount)

    if (groupedCols.length > 1) throw new RewritingException("Multi-column grouping in histograms is not yet supported.")
    val groupedColIdx = groupedCols.head.getIndex
    val groupedColInfo = histogramResults(groupedColIdx)
    val groupedColName = cols(groupedColIdx).getName

    if (aggCols.length > 1) throw new RewritingException("Multi-column aggregations in histograms are not yet supported.")
    val aggColIdx = aggCols.head.getIndex
    val origAggColAlias = cols(aggColIdx).getName

    // Ensure aggregation column has explicit alias in relation, otherwise Calcite will reference it using a derived
    // alias (e.g. EXPR$0), which will fail on the actual database.
    val (withAggColAlias, explicitAggAlias) =
      if (Helpers.isDerivedAlias(origAggColAlias)) {
        val explicitAlias = "_agg"
        val rel = Relation(node).mapCols { col =>
          if (col.idx == aggColIdx)
            EnsureAlias(col.expr) AS explicitAlias
          else col
        }
        (rel, explicitAlias)
      }
      else (Relation(node), origAggColAlias)

    val defaultVal = 0

    // If the value of the histogram bin has been modified prior to grouping, this approach will not work.
    if (groupedColInfo.valueModified) throw new RewritingException(s"Histogram column $groupedColName has modified valued.")

    // If the histogram is not derived from exactly one database column, this approach will not work.
    if (groupedColInfo.references.size != 1) throw new RewritingException(s"Histogram column must derive its values from exactly one database column.")

    // Figure out which database column contains the domain values for to the histogram column.
    val targetCol = groupedColInfo.references.head
    val colProperties = Schema.getSchemaMapForTable(config.database, targetCol.table)(targetCol.column).properties
    val domainSetFlag = colProperties.getOrElse("domainSet", throw new RewritingException(
      s"Column '${targetCol.column}' in table '${targetCol.table}' is used as a histogram bin. " +
       "Please define 'domainSet' parameter specifying a table/column that enumerates all values from this column's domain. " +
       "To disable this check set fillMissingBins = false in rewriter config (if disabled, query results are NOT safe for release)"))

    val (domainSetTable, domainSetCol) = {
      val elems = domainSetFlag.split('.')
      val (tbl, col) = elems.splitAt(elems.length-1)
      (tbl.mkString("."), col.head)
    }

    val domainSetRel = table(domainSetTable, config.database).project(col(domainSetCol) AS "_domain")

    withAggColAlias
      .asAlias("_orig")
      .project(col(groupedColIdx), EnsureAlias(col(explicitAggAlias)))
      .join(domainSetRel, left(0) == right(0), JoinRelType.RIGHT)
      .project(right(0) AS groupedColName,
               Case(IsNull(left(0)), defaultVal, left(1)) AS origAggColAlias)
  }
}
