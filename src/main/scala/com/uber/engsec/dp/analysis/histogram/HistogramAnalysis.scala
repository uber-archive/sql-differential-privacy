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

package com.uber.engsec.dp.analysis.histogram

import com.uber.engsec.dp.dataflow.AggFunctions._
import com.uber.engsec.dp.dataflow.column.RelColumnAnalysis
import com.uber.engsec.dp.dataflow.domain._
import com.uber.engsec.dp.dataflow.domain.lattice.FlatLatticeDomain
import com.uber.engsec.dp.sql.relational_algebra.RelUtils
import org.apache.calcite.rel.core.{Aggregate, TableScan}

/** Returns the aggregation status of each output column of a query. The results of this analysis are used to classify
  * queries as statistical or raw data and to determine which columns contain aggregations.
  */
class HistogramAnalysis extends RelColumnAnalysis(AggregationDomain) {

  override def transferAggregate(node: Aggregate, idx: Int, aggFunction: Option[AggFunction], state: AggregationInfo): AggregationInfo = {
    if (aggFunction.isEmpty) // grouped column
      state.copy(isGroupBy=true)
    else {
      val newReferences: Set[String] = aggFunction.get match {
        case COUNT => Set.empty
        case _ => state.references
      }

      AggregationInfo(
        isAggregation = true,
        outermostAggregation = aggFunction,
        references = newReferences,
        isGroupBy = false
      )
    }
  }

  override def transferTableScan(node: TableScan, idx: Int, state: AggregationInfo): AggregationInfo = {
    val qualifiedColName = RelUtils.getQualifiedColumnName(node, idx)
    state.copy(references=Set(qualifiedColName))
  }
}

/** Information about the aggregation status of a column
  *
  * @param isAggregation Is this column any type of aggregation?
  * @param outermostAggregation Outermost aggregation function applied to references
  * @param references Fully qualified table and column names
  * @param isGroupBy Is this column grouped?
  */
case class AggregationInfo(isAggregation: Boolean,
                           outermostAggregation: DomainElem[AggFunction],
                           references: Set[String],
                           isGroupBy: Boolean)


object AggregationDomain extends AbstractDomain[AggregationInfo] {
  override val bottom: AggregationInfo = AggregationInfo(false, FlatLatticeDomain.bottom, Set.empty, false)

  override def leastUpperBound(first: AggregationInfo, second: AggregationInfo): AggregationInfo = {
    AggregationInfo(
      isAggregation=first.isAggregation || second.isAggregation,
      outermostAggregation=FlatLatticeDomain.leastUpperBound(first.outermostAggregation, second.outermostAggregation),
      references=first.references ++ second.references,
      isGroupBy=first.isGroupBy || second.isGroupBy
    )
  }
}

