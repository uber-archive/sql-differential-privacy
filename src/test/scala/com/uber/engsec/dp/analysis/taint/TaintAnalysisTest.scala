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

package com.uber.engsec.dp.analysis.taint

import com.uber.engsec.dp.schema.Schema
import com.uber.engsec.dp.sql.QueryParser
import junit.framework.TestCase

/** Note: the following columns care marked tainted in schema config:
  *   customers.name
  *   customers.address
  *
  * All other columns are untainted.
  */
class TaintAnalysisTest extends TestCase {
  val database = Schema.getDatabase("test")

  private def getResults(query: String) = {
    val root = QueryParser.parseToRelTree(query, database)
    new TaintAnalysis().run(root, database).colFacts.toList
  }

  def testSimple() = {
    val query = "SELECT customer_id, name, address FROM customers"
    val actualResult = getResults(query)
    val expectedResult = List(false, true, true)

    TestCase.assertEquals(expectedResult, actualResult)
  }

  def testAggregation() = {
    val query = """
        SELECT customers.name as name, count(*) as "count"
        FROM orders JOIN customers ON orders.customer_id = customers.customer_id
        GROUP BY 1
      """

    val actualResult = getResults(query)
    // "customers.name" is tainted, so output column 'name' should be tainted
    val expectedResult = List(true, true)

    TestCase.assertEquals(expectedResult, actualResult)
  }

  def testWith() = {
    val query = """
        WITH t1 as (SELECT * FROM products),
             t2 as (SELECT * FROM customers)
        SELECT t1.name as product_name, t2.name as customer_name from t1, t2
      """

    val actualResult = getResults(query)
    val expectedResult = List(false, true)

    TestCase.assertEquals(expectedResult, actualResult)
  }

}