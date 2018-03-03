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
import com.uber.engsec.dp.dataflow.domain.{Bottom, Top}
import com.uber.engsec.dp.schema.Schema
import junit.framework.TestCase

class HistogramAnalysisTest extends TestCase {
  val database = Schema.getDatabase("test")

  private def getResults(query: String) = {
    val h = new HistogramAnalysis
    val results = h.analyzeQuery(query, database)
    results.colFacts.toList
  }

  def assertHistogramFailure(queryStr: String, errorMsg: String) = {
    try {
      getResults(queryStr)
      TestCase.fail("Unexpected successful transformation (was expecting exception)")
    }
    catch {
      case e: Exception => TestCase.assertEquals(errorMsg, e.getMessage)
    }
  }

  def testSimpleHistogram() = {
    val query = "SELECT product_id, COUNT(*) FROM orders GROUP BY product_id"
    val actualResult = getResults(query)

    val expectedResult = List(
      AggregationInfo(false, Bottom, Set(QualifiedColumnName("public.orders", "product_id")), false, true),
      AggregationInfo(true, Some(COUNT), Set(QualifiedColumnName("public.orders", "*")), true, false)
    )

    TestCase.assertEquals(expectedResult, actualResult)
  }
  
  def testAliasHistogram() = {
    val query = "SELECT order_date as bin, COUNT(*) FROM orders GROUP BY bin"
    val actualResult = getResults(query)

    val expectedResult = List(
      AggregationInfo(false, Bottom, Set(QualifiedColumnName("public.orders", "order_date")), false, true),
      AggregationInfo(true, Some(COUNT), Set(QualifiedColumnName("public.orders", "*")), true, false)
    )

    TestCase.assertEquals(expectedResult, actualResult)
  }

  def testModifiedHistogramBin() = {
    val query = "SELECT order_date+1 as bin, COUNT(*) FROM orders GROUP BY bin"
    val actualResult = getResults(query)

    val expectedResult = List(
      AggregationInfo(false, Bottom, Set(QualifiedColumnName("public.orders", "order_date")), true, true),
      AggregationInfo(true, Some(COUNT), Set(QualifiedColumnName("public.orders", "*")), true, false)
    )

    TestCase.assertEquals(expectedResult, actualResult)
  }

  def testRoundFunction() = {
    // functions that input only aggregates should return true for isAggregation
    val query = "SELECT product_id, ROUND(COUNT(*), 0) FROM orders GROUP BY 1"
    val actualResult = getResults(query)

    val expectedResult = List(
      AggregationInfo(false, Bottom, Set(QualifiedColumnName("public.orders", "product_id")), false, true),
      AggregationInfo(true, Some(COUNT), Set(QualifiedColumnName("public.orders", "*")), true, false)
    )

    TestCase.assertEquals(expectedResult, actualResult)
  }
  
  def testColumnReference() = {
    val query = "WITH t1 as (SELECT order_id as a FROM orders) SELECT a FROM t1"
    val actualResult = getResults(query)

    val expectedResult = List(
      AggregationInfo(false, Bottom, Set(QualifiedColumnName("public.orders", "order_id")), false, false)
    )

    TestCase.assertEquals(expectedResult, actualResult)
  }

  def testDivision() = {
    // arithmetic of aggregations is still an aggregation, but outermost aggregation is Top since more than one
    // aggregation function was applied.
    val query = "SELECT (AVG(price) / COUNT(*)) as \"result\" FROM products"
    val actualResult = getResults(query)

    val expectedResult = List(
      AggregationInfo(true, Top, Set(QualifiedColumnName("public.products", "price"), QualifiedColumnName("public.products", "*")), true, false)
    )

    TestCase.assertEquals(expectedResult, actualResult)
  }

  def testCountStar() = {
    val query = "SELECT COUNT(*) FROM orders"
    val actualResult = getResults(query)

    val expectedResult = List(
      AggregationInfo(true, COUNT, Set.empty, true, false)
    )

    TestCase.assertEquals(expectedResult, actualResult)
  }

  // Test that statistics analysis correctly returns applied aggregation functions for simple query.
  def testStatistics() = {
    val query = "SELECT COUNT(*) as my_count, SUM(price) as my_sum, AVG(price) as my_avg FROM products"
    val actualResult = getResults(query)

    val expectedResult = List(
      AggregationInfo(true, COUNT, Set(QualifiedColumnName("public.products", "*")), true, false),
      AggregationInfo(true, SUM, Set(QualifiedColumnName("public.products", "price")), true, false),
      AggregationInfo(true, AVG, Set(QualifiedColumnName("public.products", "price")), true, false)
    )

    TestCase.assertEquals(expectedResult, actualResult)
  }
}