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

import com.uber.engsec.dp.schema.Database
import org.apache.calcite.rel.RelNode

/** Classification of queries: histogram, non-histogram statistical, and raw data. */
object QueryType extends Enumeration {
  type QueryType = Value
  val HISTOGRAM, NON_HISTOGRAM_STATISTICAL, RAW_DATA = Value

  /** Inspects results of HistogramAnalysis to categorize the query as statistical (histogram or non-histogram)
    * or raw data.
    *
    * @param results A set of column facts representing the results of a histogram analysis
    * @return The type of the query: histogram, non-histogram statistical, or raw data
    */
  def getQueryType(results: HistogramAnalysis#ResultType): QueryType = {
    var groupedColumns = 0
    var nonGroupedAggregations = 0
    var rawColumns = 0

    results.colFacts.foreach { info =>
      if (info.isGroupBy)
        groupedColumns += 1

      if (info.isAggregation && !info.isGroupBy)
        nonGroupedAggregations += 1

      if (!info.isAggregation && !info.isGroupBy)
        rawColumns += 1
    }

    // A histogram is a query with one or more (non-grouped) aggregations, and all remaining columns grouped.
    if ((groupedColumns > 0) && (nonGroupedAggregations > 0) && (rawColumns == 0))
      QueryType.HISTOGRAM

    // A statistical query has every column aggregated.
    else if (nonGroupedAggregations == results.colFacts.size)
      QueryType.NON_HISTOGRAM_STATISTICAL

    // Everything else is "raw data"
    else
      QueryType.RAW_DATA
  }

  /** Categorize the query using an already parsed tree. */
  def getQueryType(root: RelNode, database: Database): QueryType = {
    val results = new HistogramAnalysis().run(root, database)
    getQueryType(results)
  }
}
