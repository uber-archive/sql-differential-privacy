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

import com.facebook.presto.sql.tree._
import com.uber.engsec.dp.exception.{TransformationException, UnsupportedConstructException}
import com.uber.engsec.dp.schema.Database
import com.uber.engsec.dp.sql.{AbstractAnalysis, QueryParser, TreeFunctions, TreePrinter}

trait ASTFunctions extends TreeFunctions[Node] {
  this: AbstractAnalysis[Node, _] =>
  override def getNodeChildren(node: Node): Iterable[Node] = ASTFunctions.getChildren(node)
  override def parseQueryToTree(query: String, database: Database): Node = QueryParser.parseToPrestoTree(query)
  override def printTree(node: Node) = TreePrinter.printTreePresto(node, resultMap, currentNode)
}

object ASTFunctions {
  /** Returns the children of the given AST node. This is used to traverse ASTs in lieu of Presto's Java visitor interface.
    */
  def getChildren(node: Node): Iterable[Node] = {
    import scala.collection.JavaConverters._
    val result = node match {
      case _: IntervalLiteral => Nil
      case _: Literal => Nil
      case e: Explain => List(e.getStatement) ++ e.getOptions.asScala
      case e: ExistsPredicate => List(e.getSubquery)
      case e: Extract => List(e.getExpression)
      case c: Cast => List(c.getExpression)
      case a: ArithmeticBinaryExpression => List(a.getLeft, a.getRight)
      case b: BetweenPredicate => List(b.getMin, b.getMax, b.getValue)
      case c: CoalesceExpression => c.getOperands.asScala
      case a: AtTimeZone => List(a.getValue, a.getTimeZone)
      case a: ArrayConstructor => a.getValues.asScala
      case s: SubscriptExpression => List(s.getBase, s.getIndex)
      case c: ComparisonExpression => List(c.getLeft, c.getRight)
      case q: QualifiedNameReference => Nil
      case q: Query => stripOption(q.getWith) ++ List(q.getQueryBody) ++ q.getOrderBy.asScala
      case w: With => w.getQueries.asScala
      case w: WithQuery => List(w.getQuery)
      case s: Select => s.getSelectItems.asScala
      case s: SingleColumn => List(s.getExpression)
      case w: WhenClause => List(w.getOperand, w.getResult)
      case i: InPredicate => List(i.getValue, i.getValueList)
      case f: FunctionCall => f.getArguments.asScala ++ stripOption(f.getWindow)
      case d: DereferenceExpression => List(d.getBase)
      case w: Window => w.getOrderBy.asScala ++ w.getPartitionBy.asScala ++ stripOption(w.getFrame)
      case w: WindowFrame => List(w.getStart) ++  stripOption(w.getEnd)
      case f: FrameBound => if (f.getValue.isPresent) List(f.getValue.get) else Nil
      case s: SimpleCaseExpression => s.getWhenClauses.asScala ++ List(s.getOperand) ++ stripOption(s.getDefaultValue)
      case i: InListExpression => i.getValues.asScala
      case n: NullIfExpression => List(n.getFirst, n.getSecond)
      case i: IfExpression => List(i.getCondition, i.getTrueValue) ++ stripOption(i.getFalseValue)
      case t: TryExpression => List(t.getInnerExpression)
      case a: ArithmeticUnaryExpression => List(a.getValue)
      case n: NotExpression => List(n.getValue)
      case s: SearchedCaseExpression => s.getWhenClauses.asScala ++ stripOption(s.getDefaultValue)
      case l: LikePredicate => List(l.getValue, l.getPattern, l.getEscape)
      case i: IsNotNullPredicate => List(i.getValue)
      case i: IsNullPredicate => List(i.getValue)
      case l: LogicalBinaryExpression => List(l.getRight, l.getLeft)
      case s: SubqueryExpression => List(s.getQuery)
      case s: SortItem => List(s.getSortKey)
      case q: QuerySpecification => List(q.getSelect) ++ stripOption(q.getFrom) ++ stripOption(q.getWhere) ++ stripOption(q.getGroupBy) ++ stripOption(q.getHaving)
      case s: SetOperation => s.getRelations.asScala
      case v: Values => v.getRows.asScala
      case r: Row => r.getItems.asScala
      case t: Table => Nil
      case t: TableSubquery => List(t.getQuery)
      case a: AliasedRelation => List(a.getRelation)
      case s: SampledRelation => List(s.getRelation, s.getSamplePercentage)
      case j: Join => List(j.getLeft, j.getRight) ++ stripOption(j.getCriteria).collect{ case c: JoinOn => c.getExpression }
      case u: Unnest => u.getExpressions.asScala
      case g: GroupBy => g.getGroupingElements.asScala
      case s: SimpleGroupBy => s.getColumnExpressions.asScala
      case g: GroupingElement => g.enumerateGroupingSets.asScala.flatMap{ _.asScala }
      case i: Insert => illegalOperation(i)
      case d: Delete => illegalOperation(d)
      case c: CreateTableAsSelect => illegalOperation(c)
      case c: AllColumns => Nil
      case c: CurrentTime => Nil
      case _ => throw new UnsupportedConstructException("getChildren on unsupported AST node type: " + node.getClass.getSimpleName)
    }
    result.filter{ _ != null }
  }

  private[ast] def illegalOperation(node: Node): Nothing = throw new TransformationException("Found illegal/unsupported operation in query: " + node.getClass.toString)
  def stripOption[T](node: java.util.Optional[T]): List[T] = { if (node.isPresent) List(node.get) else Nil }
}
