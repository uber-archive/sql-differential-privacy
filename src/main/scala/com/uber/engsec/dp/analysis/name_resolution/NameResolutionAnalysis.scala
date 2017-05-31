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

package com.uber.engsec.dp.analysis.name_resolution

import com.facebook.presto.sql.tree._
import com.uber.engsec.dp.dataflow.node.ASTDataflowAnalysis
import com.uber.engsec.dp.sql.ast.ASTFunctions

/** Dataflow analysis on ASTs to resolve identifiers in the query. Used internally in the tree transformation
  * process to transform ASTs into dataflow graphs.
  */
class NameResolutionAnalysis extends ASTDataflowAnalysis(NameResolutionDomain) {

  override def transferNode(node: Node, state: NameResolution): NameResolution = node match {
    case table: Table =>
      // Assume every table node refers to an alias until proven otherwise (i.e., unless we don't find an alias
      // of this name within the scope of the current subquery namespace).
      val newState = state.copy()
      newState.addReference(table)
      newState.setTargetRelation(table, true)
      newState

    case alias: AliasedRelation =>
      val newState = state.copy()
      newState.setTargetRelation(alias.getRelation, true)
      newState.addRelationToScope(alias)
      newState

    case table: TableSubquery =>
      val newState = state.copy()
      newState.setTargetRelation(table, true)
      newState

    case join: Join =>
      val newState = state.copy()
      newState.setTargetRelation(join, false)
      // Select items can reference the left and right relations by name, e.g., to disambiguate columns of the same name.
      newState.addRelationToScope(join.getLeft)
      newState.addRelationToScope(join.getRight)
      newState

    case withQuery: WithQuery =>
      // a WITH node adds a new relation in the global scope. We will match Table alias references to this relation
      // in the transferQuery method.
      val newState = state.copy()
      newState.addRelationToScope(withQuery)
      newState

    case deref: DereferenceExpression =>
      val newState = state.copy()
      newState.addReference(deref)
      // getBase will be a QualifiedNameReference, which we will look at and handle when matching the dereference
      // expression, so no point keeping the orphan child. We only do this for QualifiedNameReference because we may
      // see other types of base expressions in the future.
      if (deref.getBase.isInstanceOf[QualifiedNameReference])
        newState.removeOrphanReference(deref.getBase)
      newState

    case func: FunctionCall =>
      val newState = state.copy()
      if ((func.getArguments.size == 0) || (func.getName.toString == "count"))
        newState.addReference(func)
      newState

    case all: AllColumns =>
      val newState = state.copy()
      newState.addReference(all)
      newState

    case qual: QualifiedNameReference =>
      val newState = state.copy()
      newState.addReference(qual)
      newState

    case query: Query =>
      val newState = state.copy()
      newState.matchOrphanReferences()
      newState.clearScope()
      if (query eq treeRoot.get) {
        // Any remaining table nodes must be database tables, so remove them from the orphans list or else we'll get
        // errors about unresolved columns/tables.
        newState.removeTableOrphanReferences()
      }
      newState

    case withNode: With =>
      val newState = state.copy()
      newState.matchOrphanReferences()
      newState

    case spec: QuerySpecification =>
      val newState = state.copy()
      newState.matchOrphanReferences()
      newState.clearScope() // no target relation persists above a query specification node
      newState

    case _ => state
  }

  override def joinNode(node: Node, children: Iterable[Node]): NameResolution = node match {
    case join: Join =>
      val leftState = resultMap(join.getLeft).copy()
      val rightState = resultMap(join.getRight).copy()
      val result = new NameResolution()

      val children = List(join.getLeft, join.getRight) ++ ASTFunctions.stripOption(join.getCriteria).collect { case c: JoinOn => c.getExpression }

      children.foreach { child =>
        val childState = resultMap(child)
        result.addReferencesFromState(childState)
        childState.namedRelationsInScope.foreach {
          result.namedRelationsInScope += _
        }
      }

      // Each relation within a join may be exposed in scope of SELECT as a possible inner relation if the relation has a name.
      result.addRelationToScope(leftState.targetRelation.get)
      result.addRelationToScope(rightState.targetRelation.get)

      // Add the named relations of the JOIN left/right so the matchOrphanReferences method can disambiguate
      // deference expressions in the join condition between the left or right based on which relation names are in
      // scope of each.
      leftState.addRelationToScope(leftState.targetRelation.get)
      rightState.addRelationToScope(rightState.targetRelation.get)

      // This is our only chance to match column references in the join condition to the respective joined table.
      // We need to do this before set the target relation. We temporarily set the target relation to None so that
      // references inside the join condition don't resolve to the overall join. The target relation will be set
      // to this node in the transferJoin() method.
      result.targetRelation = None
      result.matchOrphanReferences(Some(join.getLeft), Some(join.getRight), leftState.namedRelationsInScope.keySet, rightState.namedRelationsInScope.keySet)

      result

    case _ => super.joinNode(node, children)
  }
}
