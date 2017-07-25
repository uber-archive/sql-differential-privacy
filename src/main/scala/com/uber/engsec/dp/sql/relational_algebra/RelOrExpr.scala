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

package com.uber.engsec.dp.sql.relational_algebra

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexNode

/** Wrapper for union type RelNode | RexNode, the root node type for relational algebra trees.
  */
sealed abstract class RelOrExpr extends Traversable[RelOrExpr] {

  /** Implementing the foreach method from Traversable gives access to many useful higher-order functions on relational
    * algebra trees fold*, reduce*, exists, collect, etc.
    */
  override def foreach[U](f: RelOrExpr => U): Unit = {
    f(this)
    RelTreeFunctions.getChildren(this).foreach { _.foreach(f) }
  }

  /** Optimized version of some traversable methods.
    */
  override def isEmpty: Boolean = false
  override def head: RelOrExpr = this
  // tail is inherited from TraversableLike

  /** Returns the underlying node element.
    */
  def unwrap: AnyRef
}

case class Relation(node: RelNode) extends RelOrExpr {
  override def hashCode: Int = System.identityHashCode(node)
  override def equals(other: Any): Boolean = other match {
    case other: Relation => other.node eq node
    case _ => false
  }
  override def unwrap: RelNode = node
  override def toString: String = node.toString
}

case class Expression(node: RexNode) extends RelOrExpr {
  override def hashCode: Int = System.identityHashCode(node)
  override def equals(other: Any): Boolean = other match {
    case other: Expression => other.node eq node
    case _ => false
  }
  override def unwrap: RexNode = node
  override def toString: String = node.toString
}

/** Conversions to and from RelOrExpr */
object RelOrExpr {
  import scala.language.implicitConversions
  implicit def rel2Sum(node: RelNode): RelOrExpr = Relation(node)
  implicit def rex2Sum(node: RexNode): RelOrExpr = Expression(node)

  implicit def relIterable2Sum(nodes: Iterable[RelNode]): Iterable[RelOrExpr] = nodes.map{Relation}
  implicit def rexIterable2Sum(nodes: Iterable[RexNode]): Iterable[RelOrExpr] = nodes.map{Expression}

  implicit def sum2Rel(rel: Relation): RelNode = rel.node
  implicit def sum2Rex(rex: Expression): RexNode = rex.node
}
