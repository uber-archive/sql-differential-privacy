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

package com.uber.engsec.dp.analysis.columns_used

import com.uber.engsec.dp.sql.QueryParser
import junit.framework.TestCase

class ColumnsUsedAnalysisTest extends TestCase {

  def checkResult(queryStr: String, expected: List[Set[String]]): Unit = {
    val root = QueryParser.parseToDataflowGraph(queryStr)
    val results = (new ColumnsUsedAnalysis).run(root)
    TestCase.assertEquals(expected, results.toList)
  }

  def testSelectAll() = {
    val query = "SELECT * FROM orders"
    checkResult(query, List(Set("orders.order_id"), Set("orders.order_date"), Set("orders.customer_id"), Set("orders.product_id"), Set("orders.quantity")))
  }

  def testCountAll() = {
    val query = "SELECT count(*) FROM orders"
    checkResult(query, List(Set("orders.order_id", "orders.product_id", "orders.order_date", "orders.customer_id", "orders.quantity")))
  }

  def testWithoutWhere() = {
    val query = "SELECT order_id FROM orders"
    checkResult(query, List(Set("orders.order_id")))
  }

  def testWithWhere() = {
    val query = "SELECT order_id FROM orders WHERE product_id = 1"
    checkResult(query, List(Set("orders.order_id")))
  }

  def testJoin() = {
    val query = "SELECT order_date FROM orders JOIN products ON orders.product_id = products.product_id"
    checkResult(query, List(Set("orders.order_date", "orders.product_id", "products.product_id")))
  }
}