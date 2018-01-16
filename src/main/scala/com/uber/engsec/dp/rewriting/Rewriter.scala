package com.uber.engsec.dp.rewriting

import com.uber.engsec.dp.sql.relational_algebra.{RelUtils, Relation, Transformer}
import com.uber.engsec.dp.sql.{AbstractAnalysis, QueryParser, TreePrinter}
import org.apache.calcite.plan.{Convention, RelOptAbstractTable}
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.rules.ProjectMergeRule

/** Root class for rewriters.
  *
  * @tparam C Config for rewriter.
  */
trait Rewriter[C <: Any] {
  /** Rewrites the given relational algebra tree with the given config. Implemented by subclasses.
    */
  def rewrite(root: Relation, config: C): Relation

  /** Entry point for rewriting by callers. Rewrites the given query with this rewriter using the given config.
    */
  def run(query: String, config: C): RewriterResult = {
    val root = QueryParser.parseToRelTree(query)
    run(root, config)
  }

  /** Rewrites the given relational algebra tree with this rewriter using the given config.
    */
  def run(root: Relation, config: C): RewriterResult = {
    if (AbstractAnalysis.DEBUG) {
      println("--- Original query ---")
      TreePrinter.printRelTree(root)
    }

    val rewrittenTree = rewrite(root, config)

    if (AbstractAnalysis.DEBUG) {
      println("--- Rewritten query (${this.getClass.getSimpleName}) ---")
      printTreeAndSql(rewrittenTree)
    }

    new RewriterResult(rewrittenTree)
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
    println(Rewriter.toSqlWithAliases(root))
    println("")
  }
}


object Rewriter {
  import com.uber.engsec.dp.rewriting.rules.Operations._

  /** Transforms the given relation into SQL, preserving any aliases specified by the rewriter.
    */
  def toSqlWithAliases(root: Relation, aliasRelationsInScope: Set[WithTable] = Set.empty): String = {
    val withQueries = root.collect{ case Relation(w: WithTable) => w }.toList.distinct.filter(!aliasRelationsInScope.contains(_)).sortBy(_.alias)

    val withClauses = withQueries.zipWithIndex.map { case (w, idx) =>
      val queryStr = toSqlWithAliases(w.definition, aliasRelationsInScope ++ withQueries.take(idx)).replace("\n", "\n  ")
      s"${w.alias} AS (\n  ${queryStr}\n)"
    }

    val withPrefix = if (withClauses.isEmpty) "" else withClauses.mkString("WITH ", ", ", "\n")
    val querySql = RelUtils.relToSql(root.optimize(ProjectMergeRule.INSTANCE))
    withPrefix + querySql
  }
}

/** Dummy class to store relations that are to be defined as WITH clauses in the rewritten query (the relational
  * algebra tree has no representation for this since it does not admit aliases).
  */
class WithTable(val definition: Relation, val alias: String) extends TableScan(
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

class RewriterResult(val root: Relation) {
  /** Emits a SQL query for the given rewritten result.
    */
  def toSql: String = Rewriter.toSqlWithAliases(root)
}

class RewritingException(val msg: String) extends RuntimeException(msg)