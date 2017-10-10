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

/** Abstract domain for columns in elastic sensitivity analysis.
  *
  * @param sensitivity Elastic sensitivity for this column. Always an upper bound of local sensitivity.
  *                    This is a floating point lattice with bottom (undefined sensitivity) represented by Option.None,
  *                    top (unbounded sensitivity) represented by Some(Infinity), and partial order defined by max.
  * @param maxFreq Max frequency of the column. The lattice is defined by the natural ordering.
  * @param aggregationApplied Has an aggregation already been applied to this column?
  * @param postAggregationArithmeticApplied Was a function/operation applied to post-aggregated result? We track this
  *                                         only to print a helpful error message since this results in infinite
  *                                         sensitivity.
  * @param canRelease  Can the values of this column be released without adding noise? This is true for columns
  *                    of public tables and columns in private tables explicitly marked with canRelease=true
  *                    (as well as values derived therefrom). This is used to determine whether histogram bin
  *                    columns are safe for release. Boolean lattice with bottom = true and top = false
  */
case class ColSensitivity(sensitivity: Option[Double],
                          maxFreq: Double,
                          aggregationApplied: Boolean,
                          postAggregationArithmeticApplied: Boolean,
                          canRelease: Boolean) {
  override def toString: String = s"sensitivity: $sensitivity, maxFreq: $maxFreq, aggregationApplied: $aggregationApplied, postAggregationArithmeticApplied: $postAggregationArithmeticApplied, canRelease: $canRelease"
}

/** The abstract domain is a product lattice with pointwise ordering of the element types defined above.
  */
object SensitivityDomain extends AbstractDomain[ColSensitivity] {
  override val bottom: ColSensitivity =
    ColSensitivity(
      sensitivity = None,  // sensitivity is undefined until aggregations are applied
      maxFreq = 0.0,
      aggregationApplied = false,
      postAggregationArithmeticApplied = false,
      canRelease = true)

  override def leastUpperBound(first: ColSensitivity, second: ColSensitivity): ColSensitivity =
    ColSensitivity(
      sensitivity = (first.sensitivity ++ second.sensitivity).reduceLeftOption(math.max),
      maxFreq = math.max(first.maxFreq, second.maxFreq),
      aggregationApplied = first.aggregationApplied || second.aggregationApplied,
      postAggregationArithmeticApplied = first.postAggregationArithmeticApplied || second.postAggregationArithmeticApplied,
      canRelease = first.canRelease && second.canRelease)
}
