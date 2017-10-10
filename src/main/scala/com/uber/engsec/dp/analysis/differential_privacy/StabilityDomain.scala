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

package com.uber.engsec.dp.analysis.differential_privacy

import com.uber.engsec.dp.dataflow.domain.AbstractDomain

/** Abstract domain for relations in elastic sensitivity analysis.
  *
  * @param stability Stability of the relation as defined by elastic sensitivity.
  * @param isPublic  Does this relation contain only publicly-derived data (as determined by the isPublic table flag)?
  *                  When public tables are joined with a protected table the entire relation becomes non-public.
  * @param ancestors Set of this node's ancestor tables, used to detect self-joins.
  */
case class RelStability(stability: Double,
                        isPublic: Boolean,
                        ancestors: Set[String]) {
  override def toString: String = s"stability: $stability, isPublic: $isPublic, ancestors: $ancestors"
}

/** The abstract domain is a product lattice with pointwise ordering of the element types defined above.
  */
object StabilityDomain extends AbstractDomain[RelStability] {
  override val bottom: RelStability =
    RelStability(
      stability = 1.0,
      isPublic = false,
      ancestors = Set.empty)

  override def leastUpperBound(first: RelStability, second: RelStability): RelStability =
    RelStability(
      stability = math.max(first.stability, second.stability),
      isPublic = first.isPublic && second.isPublic,
      ancestors = first.ancestors ++ second.ancestors)
}