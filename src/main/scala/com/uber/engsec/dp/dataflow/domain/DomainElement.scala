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

package com.uber.engsec.dp.dataflow.domain

/** A monad for lattice values represented by type E augmented with (type-less) top and bottom elements.
  */
abstract class DomainElem[+E] {
  /** Returns the element value, or throws java.util.NoSuchElementException if the lattice element is Top or Bottom
    */
  def get: E
  def isTop: Boolean
  def isBottom: Boolean

  /** Returns true if the lattice value contains the given element. Always returns false if the lattice value is Top or Bottom. */
  def contains[F >: E](elem: F): Boolean

  /** Retrieves the lattice value as an option, with Bottom returning None and element type E returning Some(e).
    * Should only be used on semi-bounded lattices which are guaranteed never to have value Top (e.g., SetLattice) since
    * this will raise an exception.
    */
  def asOption: Option[E]
}

case object Top extends DomainElem[Nothing] {
  override def isTop: Boolean = true
  override def isBottom: Boolean = false
  override def contains[F >: Nothing](elem: F): Boolean = false
  override def asOption: Option[Nothing] = throw new java.util.NoSuchElementException("Top.asOption")
  override def get = throw new java.util.NoSuchElementException("Top.get")
}

case object Bottom extends DomainElem[Nothing] {
  override def isTop: Boolean = false
  override def isBottom: Boolean = true
  override def contains[F >: Nothing](elem: F): Boolean = false
  override def asOption: Option[Nothing] = None
  override def get = throw new java.util.NoSuchElementException("Bottom.get")
}

/** External code shouldn't need to interact directly with this class; the implicit definitions below automatically
  * convert to and from this wrapper and the underlying element type.
  */
case class Mid[E](value: E) extends DomainElem[E] {
  override def isTop: Boolean = false
  override def isBottom: Boolean = false
  override def get: E = value
  override def contains[F >: E](elem: F): Boolean = elem.equals(value)
  override def asOption: Option[E] = Some(value)
  override def toString: String = value.toString
}

object DomainElem {
  import scala.language.implicitConversions
  implicit def val2DomainElem[E](value: E): DomainElem[E] = Mid(value)
  implicit def elem2Val[E](value: Mid[E]): E = value.get

  /** Convert from Option[E] to lattice element, with Option.None mapped to Bottom. */
  implicit def option2DomainElem[E](value: Option[E]): DomainElem[E] = value.fold[DomainElem[E]](Bottom)(Mid(_))
}



