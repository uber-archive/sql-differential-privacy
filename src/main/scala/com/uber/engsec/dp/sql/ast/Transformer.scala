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

package com.uber.engsec.dp.sql.ast

import com.facebook.presto.sql.tree.{AliasedRelation, AllColumns, ArithmeticBinaryExpression, ArithmeticUnaryExpression, AtTimeZone, BetweenPredicate, Cast, CoalesceExpression, ComparisonExpression, CurrentTime, DereferenceExpression, ExistsPredicate, Expression, Extract, FunctionCall, InListExpression, InPredicate, IsNotNullPredicate, IsNullPredicate, JoinOn, LikePredicate, Literal, LogicalBinaryExpression, LongLiteral, NotExpression, NullIfExpression, QualifiedNameReference, Query, QuerySpecification, Row, SearchedCaseExpression, SimpleCaseExpression, SingleColumn, SubqueryExpression, Table, TableSubquery, WhenClause, Except => PrestoExcept, Join => PrestoJoin, Node => PrestoNode, SelectItem => PrestoSelectItem, Union => PrestoUnion}
import com.uber.engsec.dp.analysis.name_resolution.{NameResolution, NameResolutionAnalysis, ReferenceInfo}
import com.uber.engsec.dp.exception._
import com.uber.engsec.dp.schema.{Database, DatabaseModel, Schema}
import com.uber.engsec.dp.sql.dataflow_graph.reference.{ColumnReference, Function, Reference, UnstructuredReference}
import com.uber.engsec.dp.sql.dataflow_graph.relation._
import com.uber.engsec.dp.sql.dataflow_graph.{Node => DFGNode}
import com.uber.engsec.dp.sql.{AbstractAnalysis, TreePrinter}
import com.uber.engsec.dp.util.IdentityHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Transforms a parsed AST (Presto tree) into a dataflow graph.
  */
class Transformer(database: Database) {
  private val prestoReferences: IdentityHashMap[PrestoNode, ReferenceInfo] = IdentityHashMap.empty
  private val prestoToDFGNode: IdentityHashMap[PrestoNode, DFGNode] = IdentityHashMap.empty
  private val inferredSchemaForTables: mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String] = new mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]

  def convertToDataflowGraph(statement: PrestoNode): DFGNode = {

    if (!statement.isInstanceOf[Query])
      throw new IllegalArgumentException("convertToDataflowGraph can only be called on an AST (Presto) tree (Query node), found type " + statement.getClass().getSimpleName)

    val nameResolutionResults: NameResolution = new NameResolutionAnalysis().run(statement, database)

    prestoToDFGNode.clear()
    prestoReferences.clear()
    inferredSchemaForTables.clear()

    // Pre-processing step: infer the schema for each table from the query structure using results of name resolution
    // analysis. The inferred schema will be merged with the config schema during tree transformation, subject to the
    // schemaMode flag.
    nameResolutionResults.getColumnRefs.foreach { case (node, refInfo) =>
      prestoReferences += (node -> refInfo)

      val targetRelation =
        if (refInfo.innerRelation.isDefined)
          refInfo.innerRelation
        else if (refInfo.ref.isUnique)
          Some(refInfo.ref.getOnly)
        else
          None

      targetRelation match {
        case Some(table: Table) =>
          val tableName = DatabaseModel.normalizeTableName(table.getName.toString)
          node match {
            case q: QualifiedNameReference => inferredSchemaForTables.addBinding(tableName, q.getName.toString)
            case d: DereferenceExpression => inferredSchemaForTables.addBinding(tableName, d.getFieldName)
            case _ => ()
          }
        case _ => ()
      }
    }

    if (AbstractAnalysis.DEBUG) {
      // Print name resolution results
      TreePrinter.printTreePresto(statement, prestoReferences)

      // Print inferred schema
      println("Inferred schema: ")
      println(inferredSchemaForTables.toString)
    }

    // Validate results (i.e., make sure we've resolved all the columns) before starting transformation.
    nameResolutionResults.validate()
    val stackDepth: Array[Integer] = Array(0)

    // Transform root node (recursively)
    val result: DFGNode = toGraphNode(statement)

    if (AbstractAnalysis.DEBUG) {
      TreePrinter.printTree(result)
    }

    result
  }

  def wrapInReference(node: DFGNode): Reference = node match {
    case r : Reference => r  // it's already a Reference type, no need to do anything
    case _ => UnstructuredReference("wrappedRelation", List(node))(node.prestoSource)
  }

  def processSelectItem(prestoNode: PrestoSelectItem): Seq[SelectItem] = prestoNode match {
    case singleColumn: SingleColumn =>
      // Select column by name: Create a SelectItem node pointing to the target column
      val expr = singleColumn.getExpression
      val alias =
        if (singleColumn.getAlias.isPresent)
          singleColumn.getAlias.get
        else
          DatabaseModel.getImplicitColumnName(expr)

      val targetRelation = wrapInReference(toGraphNode(expr))

      List(SelectItem(alias, targetRelation, Some(singleColumn)))

    case all : AllColumns =>
      // SELECT * : Create a SelectItem for every column in the target relation.
      val targetRelation = getReferencedRelationNode(all)
      if (targetRelation.numCols == 0) throw new AmbiguousWildcardException("Empty schema for SELECT * from table " + targetRelation.toString)
      targetRelation.columnNames.zipWithIndex.map{ case (colName, idx) => SelectItem(colName, ColumnReference(idx, targetRelation)) }

    case _ => throw new RuntimeException("Unsupported SelectItem type: " + prestoNode.getClass.getSimpleName + " : " + prestoNode.toString)
  }

  private def toGraphNode(prestoNode: PrestoNode): DFGNode = {

    if (prestoToDFGNode.contains(prestoNode)) {
      // If we've already processed this node, return it immediately.
      prestoToDFGNode(prestoNode)

    } else {
      // Otherwise convert the Presto node into a dataflow graph node and store the result before returning.

      implicit val prestoSource = Some(prestoNode)  // automatically assign current node as prestoSource for constructed dataflow graph nodes

      val DFGNode: DFGNode = prestoNode match {
        case query: Query => toGraphNode(query.getQueryBody)

        case spec: QuerySpecification =>
          val items = spec.getSelect.getSelectItems.asScala.map { processSelectItem(_) }.toList.flatten
          val where = if (!spec.getWhere.isPresent) None else Some(toGraphNode(spec.getWhere.get).asInstanceOf[Reference])
          val groupBy = if (!spec.getGroupBy.isPresent) Nil else
            spec.getGroupBy.get.getGroupingElements.asScala.flatMap {
              _.enumerateGroupingSets.asScala.flatMap {
                _.asScala.map {
                    case l: LongLiteral => l.getValue.toInt - 1 // SQL indices start at 1, dataflow graph indices at 0

                    case q: QualifiedNameReference =>
                      val colName = q.getName.toString
                      val matchedItems = items.filter { _.as == colName }
                      val groupedItem = matchedItems match {
                        case Nil => throw new UnknownColumnException(colName)
                        case List(x) => x
                        case _ => throw new AmbiguousColumnReference(colName)
                      }
                      items.indexOf(groupedItem)

                    case d: DereferenceExpression =>
                      // We call toGraphNode to benefit from our handling of DereferenceExpression, which performs the
                      // same logic we would otherwise do here (i.e., normalizing col index in joins, column name
                      // uniqueness checks, etc.)
                      val targetRef = toGraphNode(d).asInstanceOf[ColumnReference]

                      // This gives us the column index into the dereferenced relation, but we need to convert that
                      // into the column of the Select relation we're about to create.
                      items.map{ _.ref }.indexWhere{
                        case ColumnReference(colIndex, of) => colIndex == targetRef.colIndex && of == targetRef.of
                        case _ => false
                      }

                    case e: Expression => throw new UnsupportedConstructException("Unsupported grouping element type: " + e.toString)
                }
              }
            }.toList

          Select(items, where, groupBy)

        case func: FunctionCall =>
          val funcName = func.getName.toString
          val args =
            if (funcName == "count") {
              // Count has special semantics for its arguments that we need to handle separately from other functions.
              val arg = if (func.getArguments.isEmpty) None else Some(func.getArguments.get(0))
              if (arg.isDefined && (arg.get.isInstanceOf[QualifiedNameReference] || arg.get.isInstanceOf[DereferenceExpression])) {
                // A count of a specific column. These cases include COUNT(column_name) and COUNT(table.column_name).
                // Process as a regular column reference
                List(wrapInReference(toGraphNode(arg.get)))
              } else {
                // Otherwise, it's COUNT(*), COUNT(1), etc., i.e., a use of COUNT() that does not reference a
                // specific column. Create an unstructured reference node named 'countAll' that points to the
                // target relation in order to preserve the data dependence.
                val targetRelation = getReferencedRelationNode(func)
                List(UnstructuredReference("countAll", targetRelation))
              }

            } else {
              // Regular function. Create dataflow graph nodes for each of the function's arguments.
              func.getArguments.asScala.zipWithIndex.map { case (arg, idx) =>
                if (DatabaseModel.isFunctionArgumentLiteral(funcName, idx))
                  // Skip processing of nodes that we know by context are literals to avoid incorrectly processing them
                  // as column references even when they aren't escaped (this would otherwise result in UnknownColumnException)
                  UnstructuredReference(arg.toString)
                else
                  wrapInReference(toGraphNode(arg))
              }.toList
            }

          Function(funcName, args)

        case ref: QualifiedNameReference =>
          val name = ref.getName.toString
          if (DatabaseModel.isBuiltInFunction(name))
            Function(name)
          else {
            val targetRelation = getReferencedRelationNode(ref)
            val targetIndex =
              targetRelation.getColumnIndexes(name) match {
                case Nil => throw new UnknownColumnException(name)
                case x :: Nil => x
                case _ => throw new AmbiguousColumnReference(name)
              }

            ColumnReference(targetIndex, targetRelation)
          }

        case table: Table =>
          if (prestoReferences.contains(table)) {
            // This is an aliased table (not a database table), so resolve the alias and return the graph node for it.
            val aliasedRelation = prestoReferences(table).ref.getOnly
            toGraphNode(aliasedRelation)
          } else {
            // It's not an aliased table, create a new DataTable node.
            val tableName = DatabaseModel.normalizeTableName(table.getName.toString)
            if (Schema.getSchemaMapForTable(database, tableName).isEmpty && Transformer.isStrictMode())
              throw new UndefinedSchemaException(tableName)

            val configSchema = Schema.getSchemaForTable(database, tableName).map{ _.name }.toList
            val effectiveSchema =
              if (Transformer.isBestEffortMode())
                mergeSchemas(configSchema, inferredSchemaForTables.getOrElse(tableName, Set()).toSet)
              else
                configSchema

            DataTable(tableName, database, effectiveSchema.toIndexedSeq)
          }

        case join: PrestoJoin =>
          val leftRelation = toGraphNode(join.getLeft).asInstanceOf[Relation]
          val rightRelation = toGraphNode(join.getRight).asInstanceOf[Relation]

          val joinType = JoinType.parse(join.getType.toString)
          val joinCondition =
            if (!join.getCriteria.isPresent) None else {
              join.getCriteria.get match {
                case jo : JoinOn => Some(toGraphNode(jo.getExpression).asInstanceOf[Reference])
                case _ => throw new UnsupportedOperationException("Unsupported join criteria type: " + join.getCriteria.get.getClass.getSimpleName)
              }
            }

          Join(leftRelation, rightRelation, joinType, joinCondition)

        case deref: DereferenceExpression =>
          val colName = deref.getFieldName
          val targetRelation = getReferencedRelationNode(deref)

          val targetColIndex = targetRelation match {
            // If this dereferences into a JOIN-created relation, calculate the correct index by figuring out which
            // inner relation's column is being referenced.
            case join : Join =>
              val innerRelationNode = toGraphNode(prestoReferences(deref).innerRelation.get).asInstanceOf[Relation]
              join.getColumnIndexForInnerRelation(colName, innerRelationNode) match {
                case Some(idx) => idx
                case None => throw new JoinException("No column " + colName + " found in inner relation " + innerRelationNode.toString + ", referenced from " + deref.toString)
              }

            // For all other relation node types, ask the graph node for the named column's index.
            case _ => targetRelation.getColumnIndexes(colName) match {
              case Nil => throw new UnknownColumnException(colName)
              case x :: Nil => x
              case _ => throw new AmbiguousColumnReference(colName)
            }
          }

          ColumnReference(targetColIndex, targetRelation)

        case union: PrestoUnion =>
          val children = union.getRelations.asScala.map{ toGraphNode(_).asInstanceOf[Relation] }.toList
          // In SQL, all relations in a UNION must have identical schema
          if (!children.forall(_.columnNames == children.head.columnNames)) throw new InvalidQueryException("Schema mismatch in UNION")
          Union(children)

        case except: PrestoExcept =>
          val (left, right) = (toGraphNode(except.getLeft).asInstanceOf[Relation], toGraphNode(except.getRight).asInstanceOf[Relation])
          // In SQL, all relations in a EXCEPT must have identical schema
          if (left.columnNames != right.columnNames) throw new InvalidQueryException("Schema mismatch in EXCEPT")
          Except(left, right)

        // We represent comparison expressions as Function nodes
        case c: ComparisonExpression       => Function(c.getType.toString, wrapInReference(toGraphNode(c.getLeft)), wrapInReference(toGraphNode(c.getRight)))

        // Pass-through nodes (i.e., nodes for which we follow the data dependence chain but aren't represented explicitly in dataflow graphs)
        case t: TableSubquery              => toGraphNode(t.getQuery)
        case a: AliasedRelation            => toGraphNode(a.getRelation)
        case s: SubqueryExpression         => toGraphNode(s.getQuery)
        case c: Cast                       => toGraphNode(c.getExpression)

        // Nodes represented as UnstructuredReference
        case l: Literal                    => UnstructuredReference(s"literal(${l.toString})")
        case a: ArithmeticBinaryExpression => UnstructuredReference("arithmeticBinary", toGraphNodes(a.getLeft, a.getRight))
        case t: CurrentTime                => UnstructuredReference("currentTime")
        case n: NullIfExpression           => UnstructuredReference("nullIf", toGraphNodes(n.getFirst, n.getSecond))
        case a: ArithmeticUnaryExpression  => UnstructuredReference("arithmeticUnary", toGraphNodes(a.getValue))
        case c: CoalesceExpression         => UnstructuredReference("coalesce", toGraphNodes(c.getOperands.asScala: _*))
        case a: AtTimeZone                 => UnstructuredReference("atTimeZone", toGraphNodes(a.getValue, a.getTimeZone))
        case i: IsNullPredicate            => UnstructuredReference("isNull", toGraphNodes(i.getValue))
        case b: BetweenPredicate           => UnstructuredReference("between", toGraphNodes(b.getValue, b.getMax, b.getMin))
        case l: LikePredicate              => UnstructuredReference("like", toGraphNodes(l.getValue, l.getEscape))
        case n: NotExpression              => UnstructuredReference("not", toGraphNodes(n.getValue))
        case i: InListExpression           => UnstructuredReference("in", toGraphNodes(i.getValues.asScala: _*))
        case i: InPredicate                => UnstructuredReference("in", toGraphNodes(i.getValue, i.getValueList))
        case i: IsNotNullPredicate         => UnstructuredReference("notNull", toGraphNodes(i.getValue))
        case l: LogicalBinaryExpression    => UnstructuredReference(s"logicalBinary-${l.getType}", toGraphNodes(l.getLeft, l.getRight))
        case w: WhenClause                 => UnstructuredReference("whenClause", toGraphNodes(w.getOperand, w.getResult))
        case s: SearchedCaseExpression     => UnstructuredReference("case", toGraphNodes(s.getWhenClauses.asScala ++ List(s.getDefaultValue.orElse(null)): _*))
        case e: Extract                    => UnstructuredReference("extract", toGraphNodes(e.getExpression))
        case r: Row                        => UnstructuredReference("row", toGraphNodes(r.getItems.asScala: _*))
        case s: SimpleCaseExpression       => UnstructuredReference("simpleCase", toGraphNodes(s.getWhenClauses.asScala ++ List(s.getDefaultValue.orElse(null)) ++ List(s.getOperand): _* ))
        case e: ExistsPredicate            => UnstructuredReference("exists", toGraphNodes(e.getSubquery))
        case _ => throw new RuntimeException("Unsupported presto type: " + prestoNode.getClass.getSimpleName + " : " + prestoNode.toString)
      }

      prestoToDFGNode += (prestoNode -> DFGNode)
      DFGNode
    }
  }

  // Helper function to convert a variable number of Presto nodes into a list of dataflow graph nodes (this is
  // a common pattern when processing functions and unstructured references)
  private def toGraphNodes(prestoNodes: PrestoNode*): List[DFGNode] = prestoNodes.filter{ _ != null }.map{ toGraphNode }.toList

  private def getReferencedRelationNode(node: PrestoNode): Relation = {
    val referenceInfo = prestoReferences(node).ref

    val result =
      if (!referenceInfo.hasTwoRelations)
        toGraphNode(referenceInfo.getOnly)

      else {
        // This node may refer to either of multiple relations. We use the schema information to figure
        // out which one, throwing AmbiguousColumnReference if both relations have the target column, and
        // UnknownColumnException if neither has it.
        if (!node.isInstanceOf[QualifiedNameReference]) throw new RuntimeException("Unsupported node type for ambiguous reference: " + node.getClass.getSimpleName)
        val referencedColumnName = node.asInstanceOf[QualifiedNameReference].getName.toString

        val firstRelation = toGraphNode(referenceInfo.first).asInstanceOf[Relation]
        val secondRelation = toGraphNode(referenceInfo.second.get).asInstanceOf[Relation]

        val foundInFirst = firstRelation.getColumnIndexes(referencedColumnName).nonEmpty
        val foundInSecond = secondRelation.getColumnIndexes(referencedColumnName).nonEmpty

        (foundInFirst, foundInSecond) match {
          case (true, true)   => throw new AmbiguousColumnReference(referencedColumnName)
          case (false, false) => throw new UnknownColumnException(referencedColumnName)
          case (true, _)      => firstRelation
          case (_, true)      => secondRelation
        }
      }

    result.asInstanceOf[Relation]
  }

  /** Adds the schema elements from schema2 which are not already present in schema1 to the end of schema1.
    */
  def mergeSchemas(schema1: List[String], schema2: Set[String]): List[String] = {
    schema1 ++ (schema2 -- schema1.toSet).toList
  }

  def nodeToStr(node: DFGNode): String = {
    if (node == null) "[null]"
    else node.getClass.getSimpleName + "[" + node.toString + "]"
  }
}


object Transformer {

  object SCHEMA_MODE extends Enumeration {
    /** Strict schema mode: only predefined schema information is used. Queries that reference a table with an undefined
      * schema, or conflict with the known schema are determined to be invalid, raising an exception. This mode is useful
      * for guaranteeing correct semantics of dataflow graphs (including validation of the query) when the complete
      * schema is known.
      */
    val STRICT = Value("Strict")

    /** Best-effort mode: use schema information, if available, to resolve ambiguous semantics (e.g., wildcard selection
      * and left/right resolution for column selection of joined tables), while using the query structure to infer
      * schema everywhere else. This mode is useful if the query is known to be valid but schema information is not
      * necessarily up-to-date, since the schema is only consulted as a hint when necessary and missing elements of the
      * schema are filled in, where possible, by inspection of the query.
      *
      * IMPORTANT: this mode assumes the query is valid and therefore certain types of errors can be ruled out. This can
      * significantly improve transformation success rate when schema information is not available. However, this mode
      * may not preserve semantics if the query is invalid (in which case we may incorrectly generate a valid graph) or
      * uses wildcard selection (in which case we may generate a query with incorrect/incomplete column names).
      *
      * For example, consider the query:
      *
      *   SELECT z FROM known_table
      *
      * If the config schema defines columns [a, b, c] in table 'known_table', this query suggests that column z
      * must also exist in 'known_table', so it is added automatically. In STRICT mode this query would raise
      * an UnknownColumnException error. While this has no effect for typical analyses (which care only about columns
      * referenced by the query, i.e., column "z"), it can cause subtle issues with wildcard selection. For example:
      *
      *   WITH t1 AS (SELECT z FROM known_table) SELECT * FROM known_table
      *
      * will produce a dataflow graph with SelectItems: "a", "b", "c", "z" -- where the first 3 columns are taken from
      * the config schema and the latter column added automatically based on inspection of the WITH clause.
      * This is the best guess for the query's semantics given available information, however it may be incorrect
      * if the schema is wrong or incomplete. As a general rule, wildcard handling is only guaranteed to be correct in
      * strict mode. Wildcard selection will only fail in best-effort mode if the combined (config and/or inferred)
      * schema is completely empty.
      *
      * As another example, consider the query:
      *
      *    SELECT col1 from known_table JOIN unknown_table
      *
      * If the schema for table 'known_table' contains a column named col1, then the dataflow path for the selected
      * column is assumed to flow from that table even if we don't have any schema information for 'unknown_table'.
      * In reality, this may not be true: if 'unknown_table' also contains a column named 'col1', this query would
      * result in an ambiguous column runtime error.
      */
    val BEST_EFFORT = Value("Best effort")
  }

  var schemaMode: SCHEMA_MODE.Value = SCHEMA_MODE.BEST_EFFORT

  def isStrictMode() = { schemaMode == SCHEMA_MODE.STRICT }
  def isBestEffortMode() = { schemaMode == SCHEMA_MODE.BEST_EFFORT }
}