package com.uber.engsec.dp.rewriting

import com.uber.engsec.dp.schema.Database
import com.uber.engsec.dp.sql.relational_algebra.{RelUtils, Relation}
import com.uber.engsec.dp.sql.{AbstractAnalysis, QueryParser, TreePrinter}
import org.apache.calcite.plan.{Convention, RelOptAbstractTable}
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.rules.ProjectMergeRule

/** Root class for rewriters.
  *
  * @tparam C Config for rewriter.
  */
abstract class Rewriter[C <: RewriterConfig](config: C) {
  /** Rewrites the given relational algebra tree with the given config. Implemented by subclasses.
    */
  def rewrite(root: Relation): Relation

  /** Entry point for rewriting by callers. Rewrites the given query with this rewriter using the given config.
    */
  def run(query: String): RewriterResult = {
    val root = QueryParser.parseToRelTree(query, config.database)
    run(root)
  }

  /** Rewrites the given relational algebra tree with this rewriter using the given config.
    */
  def run(root: Relation): RewriterResult = {
    if (AbstractAnalysis.DEBUG) {
      println("--- Original query ---")
      TreePrinter.printRelTree(root)
    }

    val rewrittenTree = rewrite(root)

    if (AbstractAnalysis.DEBUG) {
      println("--- Rewritten query (${this.getClass.getSimpleName}) ---")
      printTreeAndSql(rewrittenTree)
    }

    new RewriterResult(rewrittenTree, config)
  }

  /** For debugging. */
  def printTreeAndSql(root: Relation): Unit = {
    val withQueries = root.collect{ case Relation(w: WithTable) => w }.toSet
    withQueries.foreach{ q =>
      println(s"WITH ${q.alias} AS")
      TreePrinter.printRelTree(q.definition)
      println("\n")
    }
    TreePrinter.printRelTree(root)

    println("---")
    println(Rewriter.toSqlWithAliases(root, config.database.dialect))
    println("")
  }

  class RewriterResult(val root: Relation, config: C) {
    /** Emits a SQL query for the given rewritten result.
      */
    def toSql(dialect: String = config.database.dialect): String = Rewriter.toSqlWithAliases(root, dialect)
  }
}

object Rewriter {
  import com.uber.engsec.dp.rewriting.rules.Operations._

  /** Transforms the given relation into SQL, preserving any aliases specified by the rewriter.
    */
  def toSqlWithAliases(root: Relation, dialect: String, aliasRelationsInScope: Set[WithTable] = Set.empty): String = {
    val withQueries = root.collect{ case Relation(w: WithTable) => w }.toList.distinct.filter(!aliasRelationsInScope.contains(_)).sortBy(_.alias)

    val withClauses = withQueries.zipWithIndex.map { case (w, idx) =>
      val queryStr = toSqlWithAliases(w.definition, dialect, aliasRelationsInScope ++ withQueries.take(idx)).replace("\n", "\n  ")
      s"${w.alias} AS (\n  ${queryStr}\n)"
    }

    val withPrefix = if (withClauses.isEmpty) "" else withClauses.mkString("WITH ", ", ", "\n")
    val querySql = RelUtils.relToSql(root.optimize(ProjectMergeRule.INSTANCE), dialect)
    withPrefix + querySql
  }
}

/** Dummy class to store relations that are to be defined as WITH clauses in the rewritten query (the relational
  * algebra tree has no representation for this since it does not admit aliases).
  */
case class WithTable(definition: Relation, alias: String) extends TableScan(
  definition.getCluster,
  definition.getCluster.traitSetOf(Convention.NONE),
  new RelOptAbstractTable(null, alias, definition.getRowType) {}) {
  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case w: WithTable => this.definition.equals(w.definition) && this.alias.equals(w.alias)
      case _ => false
    }
  }
}

/** Flags for all rewriters */
class RewriterConfig(val database: Database)

/** Flags for differential privacy-based rewriters */
class DPRewriterConfig(
    /** The privacy budget allocated to this query. Callers are responsible for tracking the remaining budget. */
    val epsilon: Double,

    /** The database being queried. */
    override val database: Database,

    /** Should rewriter add logic to automatically insert histogram bins from domain? This flag should be true if
      * query results are released directly.
      *
      * This is necessary for histogram queries since a DP mechanism must return a noisy result for all records in the
      * domain - including those not appearing in the output - in order to avoid leaking information via the presence or
      * absence of a bin. If this flag is true, missing bins will be populated with noisy empty results, and query
      * rewriting will fail if this cannot be achieved.
      */
    val fillMissingBins: Boolean)
  extends RewriterConfig(database)


class RewritingException(val msg: String) extends RuntimeException(msg)