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

package com.uber.engsec.dp.sql

import com.facebook.presto.sql.tree.{AliasedRelation, AllColumns, DereferenceExpression, FunctionCall, GroupBy, QualifiedNameReference, Query, QueryBody, SimpleGroupBy, SingleColumn, SortItem, StringLiteral, Table, With, WithQuery, Expression => PrestoExpression, Join => PrestoJoin, Node => PrestoNode, Select => PrestoSelect}
import com.uber.engsec.dp.dataflow.column.NodeColumnFacts
import com.uber.engsec.dp.sql.ast.ASTFunctions
import com.uber.engsec.dp.sql.dataflow_graph.reference.{ColumnReference, Function, UnstructuredReference}
import com.uber.engsec.dp.sql.dataflow_graph.relation.{Relation => DFGRelation, _}
import com.uber.engsec.dp.sql.dataflow_graph.{Node => DFGNode}
import com.uber.engsec.dp.sql.relational_algebra.{Expression, RelOrExpr, RelUtils, Relation}
import com.uber.engsec.dp.util.IdentityHashMap
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Aggregate, Correlate, Filter, Project, Sort, TableScan, Values, Join => RelJoin, Union => RelUnion}
import org.apache.calcite.rex._

import scala.collection.{MapLike, mutable}

/** A class to pretty-print parse trees with analysis results. Useful for debugging.
  */
object TreePrinter {
  def printTreeJava(root: DFGNode, resultMap: java.util.Map[DFGNode, _]): Unit = {
    import scala.collection.JavaConverters._
    printTree(root, resultMap.asScala, None)
  }

  /** Prints the given Presto tree.
    */
  def printTreePresto(root: PrestoNode,
                      resultMap: MapLike[PrestoNode,_,_] = mutable.Map.empty,
                      currentNode: Option[PrestoNode] = None): Unit = {

    def nodePrintInfo(node: PrestoNode): (String, Seq[LabeledNode[PrestoNode]]) = {

      val className = node.getClass.getSimpleName

      val printStr = node match {
        case t: Table => "Table[" + t.getName + "]"
        case w: With => "With"
        case w: WithQuery => "Name[" + w.getName + "]"
        case j: PrestoJoin => "Join [" + j.getType.toString + "]"
        case s: SortItem => "OrderBy[" + s.toString + "]"
        case s: PrestoSelect => "Select" + (if (s.isDistinct) " [DISTINCT]" else "")
        case q: Query => "Query" + (if (q.getLimit.isPresent) (" [LIMIT " + q.getLimit.get() + "]") else "")
        case a: AllColumns => if (a.getPrefix.isPresent) (a.getPrefix.toString + ".*") else "*"
        case s: SingleColumn => className + (if (s.getAlias.isPresent) (" [ALIAS: " + s.getAlias.get() + "]") else "")
        case l: StringLiteral => className + "[" + l.toString + "]"
        case q: QualifiedNameReference => "QualifiedNameReference[" + q.getName + "]"
        case d: DereferenceExpression => "DereferenceExpression[" + d.toString + "]"
        case f: FunctionCall => "FunctionCall[" + f.getName + "]"
        case a: AliasedRelation => "Alias[" + a.getAlias + "]"
        case e: PrestoExpression => className + "[" + e.toString + "]"
        case q: QueryBody => className
        case g: GroupBy => "GroupBy" + (if (g.isDistinct) " [DISTINCT]" else "")
        case s: SimpleGroupBy => "SimpleGroupBy"
        case _ => "??? [" + className + "]"
      }

      val namedChildren = ASTFunctions.getChildren(node).map { LabeledNode(None, _) }.toSeq

      (printStr, namedChildren)
    }

    new TreePrinter[PrestoNode]().printTree(root, resultMap, false, nodePrintInfo, currentNode)
  }

  /** Prints the given relational alegebra tree.
    */
  def printRelTree(root: RelOrExpr,
                   resultMap: MapLike[RelOrExpr,_,_] = mutable.Map.empty,
                   currentNode: Option[RelOrExpr] = None): Unit = {

    import scala.collection.JavaConverters._

    var currentInputNode: Option[RelNode] = None

    def nodePrintInfo(node: RelOrExpr): (String, Seq[LabeledNode[RelOrExpr]]) = node match {
      case Relation(rel) =>
        val nodeStr = node.getClass.getSimpleName
        val colNames = rel.getRowType.getFieldNames.asScala.toIndexedSeq

        rel match {
          case p: Project =>
            currentInputNode = Some(p.getInput)
            ("Project", p.getProjects.asScala.zipWithIndex.map{ case (node, idx) => LabeledNode(Some(s"$idx [as ${colNames(idx)}]"), Expression(node)) }.toList ++ List(LabeledNode(Some("input"), Relation(p.getInput))))

          case t: TableScan =>
            ("TableScan[" + RelUtils.getQualifiedTableName(t) + "]", Nil)

          case a: Aggregate =>
            ("Aggregate[" + a.getAggCallList.asScala.mkString(",") + "] grouped:" + a.getGroupSet.asList.asScala.mkString(",") + " groupSets:" + a.groupSets.asScala.map{ _.toList.toString }.mkString(", "), List(LabeledNode(Some("input"), Relation(a.getInput))))

          case f: Filter =>
            currentInputNode = Some(f.getInput)
            ("Filter",
              LabeledNode(Some("condition"), Expression(f.getCondition))
              :: LabeledNode(Some("input"), Relation(f.getInput()))
              :: Nil)

          case j: RelJoin =>
            currentInputNode = Some(j)
            ("Join[" + j.getJoinType.toString + "]",
              LabeledNode(Some("condition"), Expression(j.getCondition))
              :: LabeledNode(Some("left"), Relation(j.getLeft))
              :: LabeledNode(Some("right"), Relation(j.getRight))
              :: Nil)

          case s: Sort =>
            ("Sort[" + s.collation.getFieldCollations.asScala.map{ col => s"${col.getFieldIndex} ${col.direction.shortString}" }.mkString(", ") + "]", List(LabeledNode(Some("input"), Relation(s.getInput))))

          case u: RelUnion =>
            ("Union", u.getInputs.asScala.zipWithIndex.map { case (rel,idx) => LabeledNode(Some(s"input${idx}"), Relation(rel)) })

          case v: Values =>
            ("Values" + v.tuples.asScala.mkString, Nil)

          case c: Correlate =>
            ("Correlate[" + c.getJoinType.toString + "]",
              LabeledNode(Some("left"), Relation(c.getLeft))
              :: LabeledNode(Some("right"), Relation(c.getRight))
              :: Nil)

          case _ =>
            // ("??? (" + rel.getClass.getSimpleName + ")", rel.getChildExps.asScala.map { x => ("? expr", Expression(x) ) } ++ rel.getInputs.asScala.map { x => ("? rel", Relation(x) ) })
            throw new RuntimeException(s"Unrecognized relational node type: ${rel.getClass.toString})")
        }

      case Expression(ex) =>
        ex match {
          case rexCall: RexCall =>
            (rexCall.getClass.getSimpleName.substring(3) + "[" + rexCall.op.toString + "]", rexCall.operands.asScala.map{ ex => LabeledNode(None, Expression(ex)) })
          case inputRef: RexInputRef =>
            ("Ref[input." + inputRef.getIndex.toString + "] (" + currentInputNode.get.getRowType.getFieldNames.get(inputRef.getIndex) + ")", Nil)
          case rexSlot: RexSlot =>
            (rexSlot.getClass.getSimpleName.substring(3) + ": " + rexSlot.getIndex.toString, Nil)
          case rexLiteral: RexLiteral =>
            ("Literal[" + rexLiteral.getTypeName.toString + "]: " + (if (rexLiteral.isNull) "null" else rexLiteral.getValue.toString), Nil)
          case _ =>
            ("UNIMPLEMENTED: " + ex.getClass.getSimpleName.toString + " (" + ex.getKind.toString + ")" , Nil)
        }
    }

    new TreePrinter[RelOrExpr]().printTree(root, resultMap, false, nodePrintInfo, currentNode)
  }

  /** Prints the given dataflow graph as a tree.
    */
  def printTree(root: DFGNode,
                resultMap: MapLike[DFGNode, _, _] = mutable.Map.empty,
                currentNode: Option[DFGNode] = None): Unit = {

    def nodePrintInfo(node: DFGNode): (String, Seq[LabeledNode[DFGNode]]) = {
      val nodeStr = node.getClass.getSimpleName + (if (node.nodeStr.isEmpty) "" else "[" + node.nodeStr + "]")
      val namedChildren: Seq[LabeledNode[DFGNode]] = node match {
        case c: ColumnReference => List(("of", c.of))
        case f: Function => f.args.view.zipWithIndex.map{ case (node, idx) => (s"arg${idx}", node) }
        case d: DataTable => Nil
        case s: Select => s.items.view.zipWithIndex.map{ case (node, idx) => (s"${idx} [as ${node.as}]", node.ref) } ++ List(s.where).flatten.map{ ("where", _) } ++ s.groupBy.map{ idx => ("groupBy", s.items(idx).ref) }
        case u: UnstructuredReference => u.children.view.zipWithIndex.map{ case (node, idx) => (s"arg${idx}", node) }
        case j: Join => List(("left", j.left), ("right", j.right)) ++ List(j.condition).flatten.map{ ("condition", _) }
        case u: Union => u.children.view.zipWithIndex.map{ case (node, idx) => (s"r${idx}", node) }
        case e: Except => List(("left", e.left), ("right", e.right))
      }
      (nodeStr, namedChildren)
    }

    new TreePrinter[DFGNode]().printTree(root, resultMap, true, nodePrintInfo, currentNode)
  }
}

class TreePrinter[T <: AnyRef] {

  /* Truncate node strings longer than this for easier readability */
  val MAX_NODE_STRING_LENGTH = 64

  var maxIndent = 0
  var nodeNum = 0
  val nodeMap = new IdentityHashMap[T, PrintedNode[T]]()
  val printedNodes = scala.collection.mutable.Set[T]()

  def printTree(root: T,
                resultMap: MapLike[T, _, _] = mutable.Map.empty,
                printNodeNumbers: Boolean,
                nodePrintInfo: T => (String, Seq[LabeledNode[T]]),
                currentNode: Option[T]): Unit = {
    import scala.collection.JavaConverters._
    def _printTree(node: T, prefix: String, isRoot: Boolean = false): Unit = {
      val nodeInfo = nodeMap(node)
      val schemaStr = node match {
        // Dataflow graph nodes
        case r: DFGRelation => " | Columns: " + r.columnNames.mkString(", ")
        case c: ColumnReference => " | Column: "  + c.of.getColumnName(c.colIndex)

        // RelRex nodes
        case Relation(r: RelNode)    => " | Columns: " + r.getRowType.getFieldNames.asScala.mkString(", ")
        case _ => ""
      }

      val alreadyPrinted = node.isInstanceOf[DFGNode] && printedNodes.contains(node)
      val isTail = alreadyPrinted || nodeInfo.isTail

      // Generate the node print string
      val nodePrintStr =
        if (alreadyPrinted)
          "..."
        else if (nodeInfo.printStr.length < MAX_NODE_STRING_LENGTH)
          nodeInfo.printStr
        else
          nodeInfo.printStr.substring(0, MAX_NODE_STRING_LENGTH) + " ..."

      def printNodeAndColFacts(node: T, nodeFact: Option[_], colFacts: IndexedSeq[_]): String = {
        val colNames = node match {
          // Dataflow graph nodes
          case r: DFGRelation => r.columnNames
          case c: ColumnReference => List(c.of.getColumnName(c.colIndex))

          // RelRex nodes
          case Relation(r) => r.getRowType.getFieldNames.asScala

          case _ => Nil // All other node types have no formal column names
        }

        // Only print node result if current node is a relation
        val nodeResult =
          if (colNames.isEmpty) None
          else nodeFact.map{ x => s"{ $x }" }

        // Print all column results, one per line
        val colResults =
          if (colNames.isEmpty) {
            assert (colFacts.size == 1)
            List(colFacts.head.toString)
          }
          else {
            assert (colNames.size == colFacts.size)
            val maxColName = colNames.map(_.length).max
            colNames.zip(colFacts).map { x => s"%-${maxColName}s : %s".format(x._1, x._2.toString) }
          }

        val printLines = nodeResult ++ colResults

        val linePrefix = prefix + (if (isTail) "     " else "│    ") + (if (nodeInfo.namedChildren.isEmpty) "" else "│")
        val formattedResult = printLines.mkString(
          s"\n%-${maxIndent}s   ${if (printNodeNumbers) "     " else ""}".format(linePrefix)
        )
        formattedResult
      }

      // Generate the "state" print string
      val statePrintStr =
        if (alreadyPrinted)
          "..."
        else if (currentNode.contains(node))
          "<== Current node"
        else {
          resultMap.get(node) match {
            // Print column fact analysis results with aligned formatting to help readability
            case Some(NodeColumnFacts(nodeFact, colFacts)) => printNodeAndColFacts(node, Some(nodeFact), colFacts)
            case Some(colFacts: IndexedSeq[_]) => printNodeAndColFacts(node, None, colFacts)
            case Some(x) => x.toString + schemaStr
            case None if resultMap.nonEmpty => "" // if the result map is non-empty but current node isn't found, we must have aborted analysis early; don't print schema for these unprocessed nodes
            case _ => "- " + schemaStr
          }
        }

      // Figure out what should be printed as a prefix for this and next lines
      val prefixStr = prefix + (if (isRoot) "─> " else if (isTail) "└──> " else "├──> ")
      val nextPrefix = prefix + (if (isTail) "     " else "│    ")
      val childPrefix = prefix + (if (isTail) " " else "│")

      // Print the node!
      if (printNodeNumbers)
        System.out.format(s"%-${maxIndent}s   %-2s   %s\n", prefixStr + nodePrintStr, nodeInfo.nodeNum.toString, statePrintStr)
      else
        System.out.format(s"%-${maxIndent}s   %s\n", prefixStr + nodePrintStr, statePrintStr)

      printedNodes += node

      if (alreadyPrinted || (nodeInfo.namedChildren.isEmpty && isTail)) {
        // If we already printed this node earlier in the graph, or the node has no children and is a tail node, print a
        // spacer row (this improves readability of the graph)
        System.out.format("%-" + (maxIndent + 11) + "s\n", nextPrefix)

      } else {
        // Otherwise print all the children (recursively)
        nodeInfo.namedChildren.foreach { child =>
          if (child.label.isDefined) {
            System.out.println(s"${childPrefix}    │")
            System.out.println(s"${childPrefix}    ." + child.label.get)
            System.out.println(s"${childPrefix}    │")
          }
          _printTree(child.node, nextPrefix)
        }
      }
    }

    // Scan through all the nodes to construct the node string and assign unique identifiers to each node. This
    // allows us to calculate the required indentation level before printing the first node.
    def preprocess(node: T, isTail: Boolean, depth: Int): Unit = {
      if (nodeMap.contains(node)) return

      val (nodeStr: String, namedChildren: Seq[LabeledNode[T]]) = nodePrintInfo(node)

      maxIndent = math.max(maxIndent, 5*depth + math.min(nodeStr.length, MAX_NODE_STRING_LENGTH+4))
      val lastIdx = namedChildren.length - 1
      nodeMap += (node -> new PrintedNode[T](nodeStr, nodeNum, isTail, namedChildren))
      nodeNum = nodeNum + 1

      namedChildren.zipWithIndex.foreach{ case (node, idx) => preprocess(node.node, idx == lastIdx, depth+1) }
    }

    maxIndent = 0
    nodeNum = 0
    nodeMap.clear()
    printedNodes.clear()

    preprocess(root, true, 1)

    if (printNodeNumbers) {
      System.out.format(s"%-${maxIndent}s   %s   %s\n", "", "# ", "State")
      System.out.format(s"%-${maxIndent}s   %s   %s\n", "", "--", "-------")
    }
    else {
      System.out.format(s"%-${maxIndent}s   State\n", "")
      System.out.format(s"%-${maxIndent}s   -------\n", "")
    }

    _printTree(root, "", true)
  }
}

case class LabeledNode[+T](label: Option[String], node: T)

object LabeledNode {
  import scala.language.implicitConversions
  implicit def tuple2LabeledNode[T]( arg: (String, T) ): LabeledNode[T] = LabeledNode(Some(arg._1), arg._2)
  implicit def list2LabeledNode[T]( args: Seq[(String,T)] ): Seq[LabeledNode[T]] = args.map{ arg => LabeledNode[T](Some(arg._1), arg._2) }
  implicit def node2LabeledNode[T]( arg: T ): LabeledNode[T] = LabeledNode(None, arg)
}

class PrintedNode[T](val printStr: String, val nodeNum: Int, val isTail: Boolean, val namedChildren: Seq[LabeledNode[T]])
