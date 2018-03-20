package com.uber.engsec.dp.rewriting.rules

import com.google.common.collect.{ImmutableList, ImmutableSet}
import com.uber.engsec.dp.dataflow.column.AbstractColumnAnalysis
import com.uber.engsec.dp.dataflow.domain.AbstractDomain
import com.uber.engsec.dp.rewriting.rules.Expr.{ColumnReference, ColumnReferenceByName, ColumnReferenceByOrdinal, EnsureAlias, Predicate, QualifiedColumnReference, RelationQualifier, SimpleValueExpr, rexBuilder}
import com.uber.engsec.dp.rewriting.{RewritingException, WithTable}
import com.uber.engsec.dp.schema.Database
import com.uber.engsec.dp.sql.relational_algebra.{Relation, Transformer}
import com.uber.engsec.dp.sql.{AbstractAnalysis, TreePrinter}
import org.apache.calcite.plan._
import org.apache.calcite.plan.hep.{HepPlanner, HepProgram}
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`._
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.logical._
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlUtil
import org.apache.calcite.util.ImmutableBitSet

/** Primitive rewriting operations on relations. These operations may be called by rewriters; most return a new
  * relation, so rewriting operations can be chained.
  */
object Operations {
  import Helpers._

  import scala.collection.JavaConverters._

  /** Rewriting operations for projection relations.
    */
  implicit class ProjectionRewritingOperations(root: Project) {
    /** Reproject the given columns from this projection's input node. This is different from the .project method,
      * which projects columns from the relation to which it is applied (hence referenced columns must already appear in
      * the relation). Here, the wildcard selector (*) references all original projections for the current node -- not
      * the columns of the input! Note that only column references may be passed to this function; if a new expression
      * must be inserted, the .project method should be used.
      *
      * @param columns Column references from the present relation to project.
      * @return New relation.
      */
    def reproject(columns: ColumnReferenceByName*): Relation = {
      val colNames = root.getRowType.getFieldNames.asScala

      val origProjections = root.getProjects.asScala.zip(colNames).map { case (rex, colName) =>
        val expr = Expr.rex2Expr(rex)
        ColumnDefinitionWithAlias(expr, colName)
      }

      val targetNode = Relation(root.getInput)
      val newColumns = columns
        .flatMap{ newCol => if (newCol eq Expr.*) origProjections else List(ColumnDefinition.columnReferenceToColumnDefinitionWithName(newCol)) }
        .map{ col => (col, col.expr.toRex(targetNode)) }

      val newProject = LogicalProject.create(root.getInput, newColumns.map{ _._2 }.asJava, Helpers.getRecordType(newColumns))
      Relation(newProject)
    }
  }

  /** Rewriting operations for aggregation relations.
    */
  implicit class AggregateRewritingOperations(root: Aggregate) {
    /** Adds a grouped column to the current aggregation node.
      *
      * @param col Column definition for new aggregation.
      * @return New aggregation relation.
      */
    def addGroupedColumn(col: ColumnReference): Relation = addGroupedColumns(col)

    /** Adds grouped column(s) to the current aggregation node.
      *
      * @param cols Column definitions for new aggregation.
      * @return New aggregation relation.
      */
    def addGroupedColumns(cols: ColumnReference*): Relation = {
      val targetColIndices = cols.map{ col => Helpers.lookupColumnOrdinal(root.getInput, col).asInstanceOf[Integer] }
      val newGroupSet = root.getGroupSet.union(ImmutableBitSet.of(targetColIndices.asJava))

      val newAggregate = Relation(LogicalAggregate.create(root.getInput, newGroupSet, root.getGroupSets, root.getAggCallList))

      // Project the grouped columns to the end of the relation so we don't disrupt ordinal references further up the tree
      val oldColumns = root.getRowType.getFieldList.asScala
      val (oldGroupedColumns, oldNonGroupedColumns) = oldColumns.splitAt(root.getGroupCount)

      val newProjections: Seq[(ColumnDefinition[ValueExpr], RexNode)] = oldGroupedColumns.map { col =>
        val rexNode = Expr.rexBuilder.makeInputRef(newAggregate, col.getIndex)
        (ColumnDefinitionWithOrdinal(new SimpleValueExpr(rexNode), col.getName, rexNode.getIndex), rexNode)
      } ++ oldNonGroupedColumns.map { col =>
        val rexNode = Expr.rexBuilder.makeInputRef(newAggregate, col.getIndex + cols.length)
        (ColumnDefinitionWithOrdinal(new SimpleValueExpr(rexNode), col.getName, rexNode.getIndex), rexNode)
      } ++ cols.zipWithIndex.map { case (col, idx) =>
        val colName = lookupColumnType(newAggregate, col).getName
        val rexNode = Expr.rexBuilder.makeInputRef(newAggregate, idx + root.getGroupCount)
        (ColumnDefinitionWithOrdinal(new SimpleValueExpr(rexNode), colName, rexNode.getIndex), rexNode)
      }

      val normalizingProject = LogicalProject.create(newAggregate, newProjections.map{ _._2 }.asJava, Helpers.getRecordType(newProjections))
      Relation(normalizingProject)
    }
  }

  /** Rewriting operations for all relations.
    */
  implicit class RewritingOperations(root: Relation) {
    private def _rewriteRecursive[E](domain: AbstractDomain[E], node: Relation, transformation: (Relation, Relation, E) => (Relation, E)): (Relation, E) = {
      val oldChildren = node.getInputs.asScala.map{ Relation }
      val childResults = oldChildren.map { node => _rewriteRecursive(domain, node, transformation) }
      val newChildren = childResults.map { _._1 }

      var newNode =
        if (oldChildren != newChildren)
        // If any children were modified, clone the current node to reference the new children and fix column references in join condition if necessary.
          remapChildren(node, oldChildren, newChildren)
        else
          node

      val ctx = AbstractColumnAnalysis.joinFacts(domain, childResults.map{ _._2 })

      try {
        val rewrittenNode = transformation(newNode, node, ctx)

        /** Make sure rewriter does not disrupt ordering of columns. Rewriting rules may append new columns but may not
          * modify any of the original columns, otherwise a parent node could reference the wrong column if its ordinal
          * has changed.
          */
        val oldColumns = node.getRowType.getFieldNames.asScala
        val newColumns = rewrittenNode._1.unwrap.getRowType.getFieldNames.asScala

        if (!newColumns.startsWith(oldColumns)) {
          newNode = rewrittenNode._1
          val colString = newColumns.diff(oldColumns).mkString("[", ", ", "]")
          throw new RuntimeException(s"Rewriters must keep original columns in the same order. " +
            s"The following column(s) must be removed or projected to the end of the relation: $colString\n" +
            s"Original schema: $oldColumns\n" +
            s"New schema:      $newColumns")
        }

        rewrittenNode

      } catch {
        case e: Exception =>
          /** If debugging is turned on, print the full tree, including nodes rewritten so far. We need to manually
            * attach the current node to the tree since we are aborting the processing early.
            */
          if (AbstractAnalysis.DEBUG) {
            val currentNode = node.unwrap
            def _attachOrphans(node: Relation): Relation = {
              val oldChildren = node.getInputs.asScala.map {
                Relation
              }
              val childResults = oldChildren.map { n => if (n.unwrap == currentNode) newNode else n }

              if (oldChildren != newChildren)
                remapChildren(node, oldChildren, childResults)
              else
                node
            }
            TreePrinter.printRelTree(_attachOrphans(root), Map.empty, Some(currentNode))
          }

          throw e.initCause(new RuntimeException(s"rewriteRecursive: while processing node [${node.unwrap.getClass.getSimpleName}]"))
      }
    }

    /** Replaces the immediate inputs of the given relation.
      *
      * @param transformation Transformation rule (function) applied to each input of the current node.
      * @return New relation.
      */
    def replaceInputs(transformation: List[Relation] => List[Relation]): Relation = {
      val oldChildren = root.getInputs.asScala.map{ Relation }.toList
      val newChildren = transformation(oldChildren)

      if (oldChildren.length != newChildren.length)
        throw new RewritingException("Input list size mismatch")

      val newRoot =
        if (oldChildren != newChildren)
        // If any children were modified, clone the current node to reference the new children and fix column references in join condition if necessary.
          remapChildren(root, oldChildren, newChildren)
        else
          root

      Relation(newRoot)
    }

    /** Rewrite the relation according to given recursive dataflow/rewriting function.
      *
      * @param domain Abstract domain for dataflow analysis.
      * @param transformation Transformation rule (function) applied to each visited node.
      * @tparam E Dataflow fact type.
      * @return New relation.
      */
    def rewriteRecursive[E](domain: AbstractDomain[E])(transformation: (Relation, Relation, E) => (Relation, E)): Relation = {
      val newRoot = _rewriteRecursive(domain, root, transformation)._1
      Relation(newRoot)
    }

    /** Joins the current relation (left) with the given relation (right) using the provided condition expression.
      *
      * @param other Relation with which to join the current relation.
      * @param condition Join condition expression. Expressions may reference columns in either relation, and may be
      *                  qualified using left(col) or right(col) to disambiguate columns with identical names.
      * @param joinType Type of join.
      * @return Join relation.
      */
    def join(other: Relation, condition: Predicate, joinType: JoinRelType = JoinRelType.INNER): Relation = {
      val schema = (root.getRowType.getFieldList.asScala ++ other.getRowType.getFieldList.asScala).asJava
      val newVariablesSet = (root.getVariablesSet.asScala ++ other.getVariablesSet.asScala).asJava

      val newSchema = ImmutableList.copyOf(schema: java.lang.Iterable[RelDataTypeField])
      val systemFieldList: ImmutableList[RelDataTypeField] = ImmutableList.of()

      /** Create a temporary join node to resolve qualified column references in the condition expression. We need to
        * do this before constructing the final node since the condition expression must be provided in the constructor.
        */
      val dummyJoin = Relation(LogicalJoin.create(root, other, rexBuilder.makeLiteral(true), newVariablesSet, joinType, false, systemFieldList))
      val joinCondition = condition.toRex(dummyJoin)

      val result = LogicalJoin.create(root, other, joinCondition, newVariablesSet, joinType, false, systemFieldList)
      Relation(result)
    }

    def mapCols(transformationRule: ColumnDefinitionWithOrdinal[ValueExpr] => ColumnDefinition[ValueExpr]): Relation = {
      _rewriteColumns( _.map{ transformationRule } )
    }

    /** Creates a union of the current relation with the provided relation.
      *
      * @param other Relation with which to union current relation. Must have same number of columns as present
      *              relation, with compatible data types (i.e., each pair of columns must share a common supertype).
      * @param all Keep all values (true) or remove duplicate values (false)
      * @return Union relation.
      */
    def union(other: Relation, all: Boolean = true): Relation = {
      val result = LogicalUnion.create(List(root.unwrap, other.unwrap).asJava, all)
      Relation(result)
    }

    /** Create a new aggregation of the current relation.
      *
      * @param groups Grouped columns in the aggregation. Must reference only columns from the current relation.
      * @param aggregations Aggregation function to be applied to each group.
      * @return Aggregation relation.
      */
    def agg(groups: ColumnReference*)(aggregations: ColumnDefinition[AggExpr]*): Relation = {
      val targetColIndices = groups.map{ col => lookupColumnOrdinal(root, col).asInstanceOf[Integer] }
      val groupSet = ImmutableBitSet.of(targetColIndices.asJava)

      val aggCalls = aggregations.map{ agg =>
        val colOrdinal = agg.expr.col.map{ lookupColumnOrdinal(root, _).asInstanceOf[Integer] }.toList.asJava
        val colName = getColumnAliasFromDefinition(agg, targetColIndices.length)
        AggregateCall.create(agg.expr.aggFunction, false, colOrdinal, -1, groups.length, root, null, colName)
      }

      val result = LogicalAggregate.create(root.unwrap, groupSet, null, aggCalls.asJava)
      Relation(result)
    }

    /** Retrieves the desired number of records from the relation (e.g., using Sql's LIMIT/FETCH clause).
      *
      * @param count Number of records to retrieve.
      * @return Relation which returns only the first [count] records.
      */
    def fetch(count: Int): Relation = {
      val fetchExpr = rexBuilder.makeExactLiteral(java.math.BigDecimal.valueOf(count))
      val result = LogicalSort.create(root, RelCollations.EMPTY, null, fetchExpr)
      Relation(result)
    }

    /** Filters the relation according to the provided predicate (e.g., using Sql's SELECT _ WHERE ... clause).
      *
      * @param predicate Filter condition, which may reference columns in the current relation.
      * @return Filtered relation.
      */
    def filter(predicate: Predicate): Relation = {
      val condition = predicate.toRex(root)
      val result = LogicalFilter.create(root, condition)
      Relation(result)
    }

    /** Preserve only the given column(s) in the relation, discarding all the other columns.
      *
      * @param columns Column references for the current relation.
      * @return New relation preserving only referenced columns.
      */
    def keep(columns: ColumnReference*): Relation = {
      val indicesToKeep = columns.map{ lookupColumnOrdinal(root, _) }.toSet
      _rewriteColumns(_.filter{ col => indicesToKeep.contains(col.idx) } )
    }

    /** Discard the given column(s) from the relation, preserving all other columns.
      *
      * @param columns Columns to remove from relation schema. For column references by name, only the first matching
      *                column is removed. To remove multiple columns with the same name, use the [removeAll] method.
      * @return New relation excluding discarded columns.
      */
    def remove(columns: ColumnReference*): Relation = {
      val indicesToRemove = columns.map{ lookupColumnOrdinal(root, _) }.toSet
      _rewriteColumns(_.filter{ col => !indicesToRemove.contains(col.idx) } )
    }

    /** Discard all columns matching the given column names.
      *
      * @param columns Columns to remove from relation schema.
      * @return New relation excluding discarded columns.
      */
    def removeAll(columns: String*): Relation = {
      val colNamesToRemove = columns.toSet
      _rewriteColumns(_.filter{ col => !colNamesToRemove.contains(col.alias) } )
    }

    /** Renames the given column with the new alias.
      *
      * @param column Column to rename. The alias provided in the column definition replaces this column's original alias.
      * @return New relation with column renamed.
      */
    def rename(column: ColumnDefinitionWithAlias[ColumnReference]): Relation = {
      val colIdxToRename = lookupColumnOrdinal(root, column.expr)
      _rewriteColumns({ cols =>
        val colDefToRename = cols(colIdxToRename)
        val newColDefinition = ColumnDefinitionWithAlias(EnsureAlias(colDefToRename.expr), column.alias)
        cols.updated(colIdxToRename, newColDefinition)
      })
    }

    /** Join a relation with itself according to the provided join condition.
      *
      * @param alias Name for current relation. Self joins require aliased relations so that relation definition will
      *              not be duplicated in output query. If the current relation is already an alias-defined relation,
      *              this parameter value is ignored and the existing alias is used instead.
      * @param condition Join condition. See join method for information about join condition expressions.
      * @return Join relation.
      */
    def joinSelf(alias: String, condition: Predicate): Relation = {
      val selfAsAlias = root match {
        case Relation(w: WithTable) => root
        case _ => root.asAlias(alias)
      }
      selfAsAlias.join(selfAsAlias, condition)
    }

    /** Create the given column projections in the relation.
      *
      * @param columns Columns to project from the current relation.
      * @return New relation.
      */
    def project(columns: ColumnDefinition[ValueExpr]*): Relation ={
      _rewriteColumns(_ => columns)
    }

    /** Sort the relation according to the given column.
      *
      * @param col Column from current relation by which to sort
      * @param ascending Sort direction
      * @return New relation.
      */
    def sort(col: ColumnReference, ascending: Boolean = true): Relation = {
      val targetColIdx = lookupColumnOrdinal(root, col)
      val direction = if (ascending) RelFieldCollation.Direction.ASCENDING else RelFieldCollation.Direction.DESCENDING
      val collation = new RelFieldCollation(targetColIdx, direction)

      val newNode = root.unwrap match {
        case s: LogicalSort =>
          // If the current node is already a sort node, simply add the column to the set of sorted columns.
          val oldCollations = s.getCollation.getFieldCollations.asScala
          val newCollations = oldCollations :+ collation
          LogicalSort.create(s.getInput(), RelCollationImpl.of(newCollations.asJava), s.offset, s.fetch)

        case _ =>
          // All other node types -> add a new Sort node
          LogicalSort.create(root, RelCollationImpl.of(collation), null, null)
      }

      Relation(newNode)
    }

    /** Ensures the current relation is defined with an explicit alias, if necessary embedding its definition in a view
      * (WITH clause) in emitted SQL. If the relation already has an alias, the [alias] argument is ignored and this
      * method returns the current relation unmodified.
      *
      * This method can be used to make the output query more concise when a relation is referenced multiple times
      * in rewriting operations by ensuring the relation's definition appears only once in the output. For certain
      * databases this can also improve the performance of rewritten queries (note: the database may need to be
      * configured to materialize views in order to realize this performance benefit).
      *
      * @param alias Name for the relation, used only if relation does not already have an alias.
      * @return New relation, which will automatically insert an alias definition clause when converted to SQL.
      */
    def asAlias(alias: String): Relation = {
      root.unwrap match {
        case w: WithTable => root  // asAlias() was already called on this relation
        case t: TableScan => root  // tables always have a name
        case _ => Relation(WithTable(root, alias))
      }
    }

    /** Optimizes the current relation, transforming it into a semantically equivalent relation, according to the given
      * optimization rule.
      *
      * @param rule Optimization rule. For more information see [[org.apache.calcite.rel.rules]].
      * @return New relation.
      */
    def optimize(rule: RelOptRule): Relation = runQueryOptimizerRule(root, rule)

    /** Common method for column-based transformations, called by rewriting operations above. Should not be called
      * by rewriters.
      */
    def _rewriteColumns(transformationRule: Seq[ColumnDefinitionWithOrdinal[ValueExpr]] => Seq[ColumnDefinition[ValueExpr]]): Relation = {
      val colNames = root.getRowType.getFieldNames.asScala
      val columns = colNames.zipWithIndex.map { case (colName, colIdx) =>
        val expr = Expr.rex2Expr(Expr.rexBuilder.makeInputRef(root, colIdx))
        ColumnDefinitionWithOrdinal(expr, colName, colIdx)
      }

      val targetNode = Relation(root)
      val newColumns = transformationRule(columns)

      // If columns did not change, return same relation.
      if (newColumns == columns)
        root
      else {
        // replace wildcard reference (*) with original columns
        val newColExprs =
          newColumns.flatMap{ newCol => if (newCol.expr eq Expr.*) columns else List(newCol) }
                    .map{ col => (col, col.expr.toRex(targetNode)) }

        val newProject = LogicalProject.create(root, newColExprs.map{ _._2 }.asJava, getRecordType(newColExprs))
        Relation(newProject)
      }
    }
  }

  /** Creates a relation defined by the given database table. */
  def table(name: String, database: Database): Relation = {
    val transformer = Transformer.create(database)
    val table = transformer.catalogReader.getTable(name.split('.').toList.asJava)
    if (table == null) throw new RewritingException(s"Cannot find table '${name}' in schema.")
    Relation(transformer.sqlToRelConverter.toRel(table))
  }

  def rewriteProjections(node: LogicalProject, newProjects: Seq[RexNode]): LogicalProject =
    LogicalProject.create(node.getInput, newProjects.asJava, node.getRowType)
}

/** Helper functions for rewriting operations. These methods should not be called directly by rewriters. */
object Helpers {
  import scala.collection.JavaConverters._

  def lookupColumnType(node: RelNode, column: ColumnReference): RelDataTypeField = {
    val colIdx = lookupColumnOrdinal(node, column)
    node.getRowType.getFieldList.asScala(colIdx)
  }

  /** Returns the ordinal of the given column reference from the relation. */
  def lookupColumnOrdinal(node: RelNode, column: ColumnReference): Int = {
    val qualifier: Option[RelationQualifier] = column match {
      case q: QualifiedColumnReference => Some(q.qualifier)
      case _ => None
    }

    val (targetRelation, idxOffset) =
      if (qualifier.isDefined) {
        val targetJoinNode = node match {
          case WithTable(Relation(j: LogicalJoin), _) => j
          case j: LogicalJoin => j
          case _ => throw new RewritingException(s"${qualifier.get}(${column.toString}): Qualified column references may only be used on Join nodes")
        }

        qualifier.get match {
          case Expr.left => (targetJoinNode.getInput(0), 0)
          case Expr.right => (targetJoinNode.getInput(1), targetJoinNode.getInput(0).getRowType.getFieldCount)
        }

      } else (node, 0)

    column match {
      case ColumnReferenceByOrdinal(idx) => idx + idxOffset
      case ColumnReferenceByName(name) =>
        val cols = targetRelation.getRowType.getFieldList.asScala
        cols.find(_.getName == name).getOrElse(throw new RewritingException(s"projectColumns: Cannot find column $name in input ${node.getClass.getSimpleName}")).getIndex + idxOffset
    }
  }

  /** Clones the given node, replacing its original children (inputs) with the provided nodes and updating column
    * references in join conditions based on potentially new ordinals of children columns. */
  def remapChildren(node: Relation, oldChildren: Seq[Relation], newChildren: Seq[Relation]): Relation = {
    val newNode = node.unwrap match {
      case p: LogicalProject => LogicalProject.create(newChildren.head.unwrap, p.getProjects, p.getRowType)
      case f: LogicalFilter => LogicalFilter.create(newChildren.head.unwrap, f.getCondition, f.getVariablesSet.asInstanceOf[ImmutableSet[CorrelationId]])
      case j: LogicalJoin =>
        val left = newChildren.head.unwrap
        val right = newChildren(1).unwrap

        val oldColsInLeftRelation = oldChildren.head.unwrap.getRowType.getFieldCount
        val addedColsInLeftRelation = newChildren.head.unwrap.getRowType.getFieldCount - oldColsInLeftRelation
        val newCondition = if (addedColsInLeftRelation > 0) rewriteJoinCondition(j.getCondition, oldColsInLeftRelation, addedColsInLeftRelation) else j.getCondition

        val newVariablesSet = (left.getVariablesSet.asScala ++ right.getVariablesSet.asScala).asJava
        val systemFieldList: ImmutableList[RelDataTypeField] = ImmutableList.of()

        LogicalJoin.create(left, right, newCondition, newVariablesSet, j.getJoinType, false, systemFieldList)

      case a: LogicalAggregate => LogicalAggregate.create(newChildren.head.unwrap, a.indicator, a.getGroupSet, a.getGroupSets, a.getAggCallList)
      case s: LogicalSort => LogicalSort.create(newChildren.head.unwrap, s.collation, s.offset, s.fetch) // standard constructor for LogicalSort is marked private
      case u: LogicalUnion => LogicalUnion.create(newChildren.map{ _.unwrap }.toList.asJava, u.all)
      case m: LogicalMinus => LogicalMinus.create(newChildren.map{ _.unwrap }.toList.asJava, m.all)
      case i: LogicalIntersect => LogicalIntersect.create(newChildren.map{ _.unwrap }.toList.asJava, i.all)
      case c: LogicalCorrelate => LogicalCorrelate.create(newChildren.head.unwrap, newChildren(1).unwrap, c.getCorrelationId, c.getRequiredColumns, c.getJoinType)
    }

    Relation(newNode)
  }

  /** Rewrites the join condition, replacing column references with corrected ordinals based on the number of columns
    * added by the rewriter to the left relation.
    */
  def rewriteJoinCondition(node: RexNode, oldColsInLeftRelation: Int, addedColsInLeftRelation: Int): RexNode = {
    def _rewriteRecursive(node: RexNode): RexNode = node match {
      case c: RexCall =>
        val origChildren = c.operands.asScala
        val newChildren = origChildren.map{ _rewriteRecursive }
        if (newChildren != origChildren)
          c.clone(c.getType, newChildren.asJava)
        else
          c

      case i: RexInputRef =>
        if (i.getIndex >= oldColsInLeftRelation)
          new RexInputRef(i.getIndex + addedColsInLeftRelation, i.getType)
        else
          i

      case l: RexLiteral => l
    }

    _rewriteRecursive(node)
  }

  /** Construct a RelRecordType object indicating the Calcite data type of the given set of columns. */
  def getRecordType(cols: Seq[(ColumnDefinition[Expr], RexNode)]): RelRecordType = {
    val newFields: Seq[RelDataTypeField] = cols.zipWithIndex.map{ case ((col, rex), idx) =>
      val colName: String = getColumnAliasFromDefinition(col, idx)
      new RelDataTypeFieldImpl(colName, idx, rex.getType)
    }
    new RelRecordType(StructKind.FULLY_QUALIFIED, newFields.asJava)
  }

  def isDerivedAlias(alias: String): Boolean = alias.startsWith("EXPR$") // Unfortunately Calcite doesn't provide a method for detecting this. See calcite.sql.SqlUtil.deriveAliasFromOrdinal().

  def getColumnAliasFromDefinition(col: ColumnDefinition[Expr], ordinal: Int): String = {
    // If column has no specified name, generate a derived name (e.g. EXPR$0), which Calcite omits when emitting SQL
    (col, col.expr) match {
      // Only keep the original alias if it wasn't an auto-generated name. Otherwise generate a new name based on the (possibly new) column ordinal
      case (ColumnDefinitionWithOrdinal(_, alias, _), _) if !isDerivedAlias(alias) => alias
      case (ColumnDefinitionWithAlias(_, alias), _) => alias
      case (_, ColumnReferenceByName(alias)) => alias
      case _ => SqlUtil.deriveAliasFromOrdinal(ordinal)
    }
  }

  def runQueryOptimizerRule(root: Relation, rule: RelOptRule): Relation = {
    val program = HepProgram.builder.addRuleInstance(rule).build
    val optPlanner = new HepPlanner(program)
    optPlanner.setRoot(root.unwrap)
    Relation(optPlanner.findBestExp)
  }
}