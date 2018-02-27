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

package com.uber.engsec.dp.rewriting

import com.uber.engsec.dp.rewriting.coverage.CoverageRewriter
import com.uber.engsec.dp.schema.Schema
import com.uber.engsec.dp.sql.QueryParser
import junit.framework.TestCase

class CoverageRewriterTest extends TestCase {
  val database = Schema.getDatabase("test")

  def checkResult(query: String, expected: String): Unit = {
    val root = QueryParser.parseToRelTree(query, database)
    val config = new RewriterConfig(database)
    val result = new CoverageRewriter(config).run(root)
    TestCase.assertEquals(expected.stripMargin.stripPrefix("\n"), result.toSql())
  }

  def testStatisticalQuery() = {
    val query = "SELECT COUNT(*), AVG(order_id) FROM orders"

    checkResult(query, """
      |SELECT COUNT(*) coverage
      |FROM public.orders"""
    )
  }

  def testHistogramQuery() = {
    val query = "SELECT order_id, AVG(order_id) FROM orders WHERE order_id < 10 GROUP BY order_id"

    checkResult(query, """
      |WITH _count AS (
      |  SELECT order_id, COUNT(*) coverage
      |  FROM public.orders
      |  WHERE order_id < 10
      |  GROUP BY order_id
      |)
      |SELECT MEDIAN(coverage) coverage
      |FROM _count
      |LIMIT 1"""
    )
  }
}