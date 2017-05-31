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
import com.uber.engsec.dp.dataflow.domain.AbstractDomain
import com.uber.engsec.dp.exception.{AmbiguousColumnReference, TransformationException, UnknownColumnException}
import com.uber.engsec.dp.util.IdentityHashMap

import scala.collection.{SetLike, mutable}

class NameResolutionException(message: String) extends TransformationException(message) {}

object NameResolutionDomain extends AbstractDomain[NameResolution] {

  override val bottom: NameResolution = new NameResolution()

  override def leastUpperBound(first: NameResolution, second: NameResolution): NameResolution = {
    val newReferences = first.references ++ second.references
    val newOrphanReferences = first.orphanReferences ++ second.orphanReferences
    val newNamedRelations = first.namedRelationsInScope ++ second.namedRelationsInScope
    val newRelation = (first.targetRelation, second.targetRelation) match {
      case (_, None) => first.targetRelation
      case (None, _) => second.targetRelation
      case (Some(a), Some(b)) if a == b => first.targetRelation
      case _ => throw new NameResolutionException("Multiple different relations in NameResolutionDomain join.")
    }
    new NameResolution(newReferences, newOrphanReferences, newRelation, newNamedRelations)
  }
}

/** Abstract domain for the name resolution analysis
  *
  * @param references A list of all resolved references below a given point in the parse tree. Here, "reference" refers to both
  * column-reference type nodes (DeferenceExpression, QualifiedNameExpression, etc.) as well as data tables
  * references (Table nodes). We compute the same type of information about each type of reference, namely, which
  * relation it's actually referring to. In the case of a Table node, it may reference a WITH-created relation using
  * an alias, in which case the Table node would be included in this map (conversely, if a Table node is not in this
  * map after analysis completes, that node must refer to an original database table).
  * IMPORTANT: we must use IdentityHashMap because presto nodes define equal methods based on field values and we
  * require reference equality.
  *
  * @param orphanReferences An orphan reference is a reference that hasn't (yet) been matched to its respective relation but will be once
  * we go further up the tree. For efficiency we maintain these in a separate list so we can process them without
  * the need to iterate through all the other already-processed references. We use an IdentityHashMap instead of
  * HashSet for the reason noted above.
  *
  * @param targetRelation This field stores the (singleton) relation in the FROM clause of the current subtree.
  * In other words, this is the relation that will referenced by the nearest SELECT items, absent any overriding table dereference.
  *
  * @param namedRelationsInScope A list of all named relations in scope, i.e., tables that may be referenced by name/alias at the current point in
  * the parse tree. This includes referenceable (is that a word?) inner relations such as tables used in a JOIN, as
  * well as globally referenceable relations such as those created using WITH. This list is used to resolve the
  * target relation whenever a deference node is processed. The analysis will clear this scope, or add new items to
  * it, as it processes relevant nodes in the tree.
  */
class NameResolution (
  val references: mutable.Map[Node, ReferenceInfo] = new IdentityHashMap(),
  val orphanReferences: mutable.Map[Node, Null] = new IdentityHashMap(),
  var targetRelation: Option[Node] = None,
  val namedRelationsInScope: mutable.Map[String, Node] = new mutable.HashMap()) {

  def copy(): NameResolution = {
    val newReferences = new IdentityHashMap[Node, ReferenceInfo]()
    val newOrphanReferences = new IdentityHashMap[Node, Null]()
    val newNamedRelationInScope = new mutable.HashMap[String, Node]()

    references.foreach { newReferences += _ }
    orphanReferences.foreach { newOrphanReferences += _ }
    namedRelationsInScope.foreach { newNamedRelationInScope += _ }

    new NameResolution(newReferences, newOrphanReferences, targetRelation, newNamedRelationInScope)
  }


  def addReferencesFromState(other: NameResolution): Unit = {
    other.references.foreach { references += _ }
    other.orphanReferences.foreach { orphanReferences += _ }
  }

  def validate() = {
    /*
    if (!orphanReferences.isEmpty) {
      val unknownCols = orphanReferences.keys.map { orphan => orphan.getClass.getSimpleName + "[" + orphan.toString + "] " }
      throw new UnknownColumnException("Unknown/unresolved column(s): " + unknownCols.mkString(", "))
    }
    */
  }

  /** Helper function to update the target relation and optionally clear all other relations in scope. */
  def setTargetRelation(targetRelation: Node, clearScope: Boolean): Unit = {
    if (clearScope)
      this.clearScope()

    this.targetRelation = Some(targetRelation)
  }

  /** Helper function to add a named relation to the current scope. */
  def addRelationToScope(relation: Node): Unit = {

    val (name, targetRelation) = relation match {
      case t: Table =>
        (relation.asInstanceOf[Table].getName.toString, relation)

      case a: AliasedRelation =>
        // Dereference expression table names are parsed to lowercase by Presto, so we convert relation aliases to
        // lowercase so we can match.
        (a.getAlias.toLowerCase, a.getRelation)

      case w: WithQuery =>
        // We want to point to the actual query part of the WITH node.
        (w.getName, w.getQuery)

      case t: TableSubquery =>
        ("", t.getQuery)

      case j: Join =>
        // Joins are not named, so outer clauses have no way to reference it so we can just return.
        return

      case _ =>
        throw new TransformationException("Unsupported named relation node type: " + relation.getClass.getSimpleName)
    }

    // A new relation in scope shadows any relation with the same name beneath it in the tree. We model this by
    // removing any orphan reference that matches the alias name. To be safe we only do this for WithQuery and
    // AliasedRelation nodes, and only remove orphan Table nodes, but this logic should apply to any relation type.
    // See TreeTransformationTest.testAliasCollision() for an example query that requires this.
    if (relation.isInstanceOf[AliasedRelation] || relation.isInstanceOf[WithQuery]) {
      val shadowedTable = orphanReferences
        .keys
        .find( { node => node.isInstanceOf[Table] && node.asInstanceOf[Table].getName.toString == name } )
        .foreach{ orphanReferences.remove }
    }

    namedRelationsInScope += name -> targetRelation
  }

  def addReference(column: Node): Unit = {
    // When new reference nodes are initially added, they are orphans; the matchOrphanReferences method, invoked
    // by certain transfer functions during analysis, will associate the references with corresponding relations.
    // We use an IdentityHashMap to act as a Set with reference equality (see comment above); the map value is ignored.
    orphanReferences += column -> null
  }

  def matchOrphanReferences(leftRelation: Option[Node] = None,
                            rightRelation: Option[Node] = None,
                            leftRelationsInScope: SetLike[String,_] = Set.empty,
                            rightRelationsInScope: SetLike[String,_] = Set.empty): Unit = {

    val matchedColumns = new mutable.ArrayBuffer[Node]()

    orphanReferences.keys.foreach { node =>
      val matchedReferenceInfo: Option[ReferenceInfo] = node match {
        case _ : QualifiedNameReference | _ : AllColumns | _ : FunctionCall =>
          // If leftRelation and rightRelation are defined, it means we are resolving orphan references in a JOIN
          // node and must (later) resolve between the left and right relations using schema information. This will
          // happen during tree transformation.
          if (leftRelation.isDefined && rightRelation.isDefined) {
            Some(new ReferenceInfo(leftRelation.get, rightRelation, None))

          } else {
            // Otherwise, we can point the reference directly to the current targetRelation. If the column doesn't
            // exist in this relation, we will detect it during tree transformation (when schema information is
            // available) and throw an error.
            targetRelation.map { new ReferenceInfo(_) }
          }

        case d: DereferenceExpression =>
          // If it's a dereference expression, it may refer either to the FROM relation, an inner relation,
          // or a different aliased relation (e.g., a relation created in a WITH clause) so we must find the
          // right one. This uses toString, which puts extra quotes in, then uses replaceAll to remove them.
          // We split on the first dot, assuming that everything after it is the column name (which may itself have dots in it)
          // TODO: figure out a better way to handle this; sometimes table names have dots in them. We should probably try to match all possible decompositions of dereferences with more than one dot.
          val tableName = d.getBase.toString.replaceAll("\"", "").split("\\.")(0)
          if (leftRelation.isDefined && rightRelation.isDefined) {
            // Dereference inside of a join condition. In contrast to the QualifiedNameReference case above,
            // we can uniquely disambiguate between the left and right relations right now because only one will
            // have the referenced table in its scope.
            val isRelationInLeftScope = leftRelationsInScope.contains(tableName)
            val isRelationInRightScope = rightRelationsInScope.contains(tableName)

            val newTargetRelation = (isRelationInLeftScope, isRelationInRightScope) match {
                case (true, true) => throw new AmbiguousColumnReference(s"Deferenced table '${tableName}' is in scope of both left and right JOIN relation.")
                case (false, false) => throw new UnknownColumnException(s"Dereferenced table '${tableName}' not found in JOIN left or right relation.")
                case (true, _) => leftRelation.get
                case (_, true) => rightRelation.get
              }

            Some(new ReferenceInfo(newTargetRelation, None, namedRelationsInScope.get(tableName)))

          } else {
            // A dereference outside a join condition. There are two subcases we need to handle:
            if (targetRelation.isDefined) {
              // The target relation is defined (i.e. unique and unambiguous) but the node still dereferences
              // a named relation. This should only occur for references of columns in an inner relation of
              // a JOIN from an encompassing SELECT clause, e.g.:
              //
              //    SELECT ft.uuid FROM fact_trip ft JOIN dim_city dc ON ...
              //
              // We match the reference to the targetRelation (i.e. the Join node) and set the inner relation
              // as the node with the referenced name in our scope (which will be either the left or right
              // field of the Join).
              Some(new ReferenceInfo(targetRelation.get, None, namedRelationsInScope.get(tableName)))
            }
            else {
              // No target relation is currently defined (i.e. this reference does not de facto refer to a
              // specific relation) but it's a dereference of a named relation. This case could happen for a
              // dereference of, for example, a WITH-created relation.
              namedRelationsInScope.get(tableName).map{ new ReferenceInfo(_) }
            }
          }

        case t: Table =>
          // If it's a Table node, check if the name matches a named relation in scope, in which case it's an
          // alias reference. Never match a table to itself.
          val tableName = t.getName.toString
          namedRelationsInScope.get(tableName).filter { _ ne node } map { new ReferenceInfo(_) }

        case _ => throw new NameResolutionException("Unsupported reference type: " + node.getClass.getSimpleName)
      }

      // If we found the correct relation, move this to the matched columns set.
      matchedReferenceInfo.foreach { info: ReferenceInfo =>
        references.put(node, info)
        matchedColumns += node
      }
    }

    matchedColumns.foreach { orphanReferences.remove }
  }

  def removeTableOrphanReferences() = orphanReferences.keys.filter { _.isInstanceOf[Table] } foreach { orphanReferences.remove }

  def removeOrphanReference(column: Node): Unit = orphanReferences.remove(column)

  def clearScope() = {
    targetRelation = None
    namedRelationsInScope.clear()
  }

  override def toString: String = {
    def map2Str[A <: AnyRef, B <: AnyRef](map: mutable.Map[A, B]): String = {
      "[" + map.map { case (key,value) =>
        val keyStr = key match {
          case _: Query => "Query"
          case _ => key.toString
        }
        val valStr = value match {
          case _ : Query => "Query"
          case _ => value.toString
        }
        keyStr + " -> " + valStr
      }.mkString(", ") + "]"
    }

    val targetRelations = if (targetRelation.isEmpty) Nil else List("TargetRelation: " + targetRelation)
    val namedRelations = if (namedRelationsInScope.isEmpty) Nil else List("NamedRelationsInScope: " + map2Str(namedRelationsInScope))
    val refs = if (references.isEmpty) Nil else List("References: " + map2Str(references))
    val orphans = if (orphanReferences.isEmpty) Nil else List("OrphanReferences: " + orphanReferences.keySet.toString)
    (targetRelations ++ namedRelations ++ refs ++ orphans).mkString(" | ")
  }

  def getColumnRefs: mutable.Map[Node, ReferenceInfo] = references

}