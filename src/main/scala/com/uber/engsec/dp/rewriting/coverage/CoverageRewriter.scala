package com.uber.engsec.dp.rewriting.coverage

import com.uber.engsec.dp.rewriting.rules.ColumnDefinition._
import com.uber.engsec.dp.rewriting.rules.Expr._
import com.uber.engsec.dp.rewriting.rules.Operations._
import com.uber.engsec.dp.rewriting.{Rewriter, RewriterConfig}
import com.uber.engsec.dp.sql.relational_algebra.{RelUtils, Relation}
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalSort}
import org.apache.calcite.rel.rules.FilterProjectTransposeRule

/**
  * Rewriter that calculates coverage of aggregation queries.
  */
class CoverageRewriter(config: RewriterConfig) extends Rewriter(config) {
  override def rewrite(root: Relation): Relation = {
    /** Find first aggregation node (strip away projections and other post-processing of aggregation column). */
    val rootAggNode = root.collectFirst{ case Relation(l: LogicalAggregate) => l }.get
    val groupedColumns = RelUtils.getGroupedCols(rootAggNode)

    /** Replace aggregation with a count-histogram, grouping by the same bins of original aggregation. */
    val coverageRelation = Relation(rootAggNode.getInput)
      .agg (groupedColumns: _*) (Count(*) AS "coverage")
      .optimize(FilterProjectTransposeRule.INSTANCE)

    /** Reconstruct sort node, if present in original query, to preserve ORDER BY and LIMIT clauses. */
    val newRoot = root.unwrap match {
      case l: LogicalSort => Relation(LogicalSort.create(coverageRelation, l.getCollation, l.offset, l.fetch))
      case _ => coverageRelation
    }

    /** For histogram queries, compute median coverage across all bins */
    val result = if (groupedColumns.nonEmpty)
      newRoot.asAlias("_count")
             .project(Median(col("coverage")) AS "coverage")
             .fetch(1)
    else
      newRoot

    result
  }
}
