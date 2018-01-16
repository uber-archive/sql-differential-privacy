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

package com.uber.engsec.dp.sql.ast

import com.uber.engsec.dp.exception._
import com.uber.engsec.dp.sql.QueryParser
import com.uber.engsec.dp.sql.dataflow_graph.Node
import com.uber.engsec.dp.sql.dataflow_graph.reference.{ColumnReference, Function, UnstructuredReference}
import com.uber.engsec.dp.sql.dataflow_graph.relation._
import junit.framework.TestCase

/** These tests verify that certain types of queries can be parsed and transformed to dataflow graphs.
  */
class TreeTransformationTest extends TestCase {

  /** Compares a parsed dataflow graph against an expected result (represented by a string encoding the tree structure
    * in a basic syntax inspired by the TreePrinter's output -- see tests below for examples). This method is used by
    * the tests below to ensure that the query is transformed successfully and the transformed graph is correct.
    */
  def validateDataflowGraph(queryStr: String,
                            treeStructure: String,
                            schemaMode: Transformer.SCHEMA_MODE.Value = Transformer.SCHEMA_MODE.STRICT) {
    System.out.println("Query: " + queryStr)
    Transformer.schemaMode = schemaMode
    val root = QueryParser.parseToDataflowGraph(queryStr)
    TestCase.assertEquals(treeStructure.trim().replaceAll("\n      ", "\n"), CompactTreePrinter.printTree(root))
    println("Test passed!\n")
  }

  /** Used by tests to ensure that certain illegal queries and unsupported conditions are correctly raised as errors.
    */
  def assertException(queryStr: String,
                      expectedException: Class[_ <: Exception],
                      schemaMode: Transformer.SCHEMA_MODE.Value = Transformer.SCHEMA_MODE.BEST_EFFORT) {
    try {
      System.out.println("Query: " + queryStr)
      Transformer.schemaMode = schemaMode
      val tree = QueryParser.parseToDataflowGraph(queryStr)
      TestCase.fail("Unexpected successful transformation (was expecting exception " + expectedException.getSimpleName + ")")
    }
    catch {
      case e: Exception if e.getClass == expectedException =>
        System.out.println("Expected exception thrown: " + expectedException.getSimpleName + "\nTest passed!\n")

      case e: Exception =>
        e.printStackTrace()
        TestCase.failNotEquals("Exception type mismatch. Stack trace printed above.", expectedException, e)
    }
  }

  def testParseError() {
    val query = "SELECT b SELECT c"
    assertException(query, classOf[ParsingException])
  }

  def testSelectSubqueryWithAlias() {
    val query = "SELECT b FROM (SELECT product_id AS b FROM orders) AS ft"
    val tree = """
      ─> Select
          .0 [as b]
          └─> ColumnReference[0]
              .of
              └─> Select
                  .0 [as b]
                  └─> ColumnReference[3]
                      .of
                      └─> DataTable["orders"]
      """

    validateDataflowGraph(query, tree)
  }

  def testSelectSubqueryWithoutAlias1() {
    val query = "SELECT ft.uuid FROM (SELECT uuid AS b FROM orders) AS ft"

    assertException(query, classOf[UnknownColumnException])
  }

  def testSelectSubqueryWithoutAlias2() {
    val query = "SELECT order_id FROM (SELECT order_id AS b FROM orders) AS ft"

    assertException(query, classOf[UnknownColumnException])
  }

  def testSelectCase() {
    val query = "SELECT CASE WHEN product_id = 1 THEN 'one' ELSE 'other' END AS caseResult FROM orders"
    val tree = """
      ─> Select
          .0 [as caseResult]
          └─> UnstructuredReference[case]
              .arg0
              ├─> UnstructuredReference[whenClause]
              │   .arg0
              │   ├─> Function[EQUAL]
              │   │   .arg0
              │   │   ├─> ColumnReference[3]
              │   │   │   .of
              │   │   │   └─> DataTable["orders"]
              │   │   .arg1
              │   │   └─> UnstructuredReference[literal(1)]
              │   .arg1
              │   └─> UnstructuredReference[literal('one')]
              .arg1
              └─> UnstructuredReference[literal('other')]
      """

    validateDataflowGraph(query, tree)
  }

  def testQueryDateFeatures1() {
    val query = "SELECT date_trunc('DAY', order_date) AS my_time FROM orders"
    val tree = """
      ─> Select
          .0 [as my_time]
          └─> Function[date_trunc]
              .arg0
              ├─> UnstructuredReference[literal('DAY')]
              .arg1
              └─> ColumnReference[1]
                  .of
                  └─> DataTable["orders"]
      """

    validateDataflowGraph(query, tree)
  }

  def testQueryDateFeaturesBestEffort() {
    // In best effort mode, non-existent column "fake_column" should be added to schema for orders
    val query = "SELECT date_trunc('DAY', fake_column) AS my_time FROM orders"
    val tree =
      """
      ─> Select
          .0 [as my_time]
          └─> Function[date_trunc]
              .arg0
              ├─> UnstructuredReference[literal('DAY')]
              .arg1
              └─> ColumnReference[6]
                  .of
                  └─> DataTable["orders"]
      """

    validateDataflowGraph(query, tree, Transformer.SCHEMA_MODE.BEST_EFFORT)
  }

  def testQueryDateFeaturesStrict() {
    // In strict mode, non-existent column fake_column should raise an UnknownColumnException.
    val query = "SELECT date_trunc('DAY', fake_column) AS my_time FROM orders"
    assertException(query, classOf[UnknownColumnException], Transformer.SCHEMA_MODE.STRICT)
  }

  def testTableAliasWithCompoundQuery() {
    val query = """
      SELECT date_trunc('DAY', f.order_date) AS my_time
      FROM orders f JOIN customers c ON f.customer_id = c.customer_id
    """

    val tree = """
      ─> Select
          .0 [as my_time]
          └─> Function[date_trunc]
              .arg0
              ├─> UnstructuredReference[literal('DAY')]
              .arg1
              └─> ColumnReference[1]
                  .of
                  └─> Join[INNER]
                      .left
                      ├─> DataTable["orders"]
                      .right
                      ├─> DataTable["customers"]
                      .condition
                      └─> Function[EQUAL]
                          .arg0
                          ├─> ColumnReference[2]
                          │   .of
                          │   └─> DataTable["orders"]
                          .arg1
                          └─> ColumnReference[0]
                              .of
                              └─> DataTable["customers"]
      """

    validateDataflowGraph(query, tree)
  }

  def testWithClauseSimple1() {
    val query = "with t1 AS (select product_id FROM orders) select product_id FROM t1"
    val tree = """
      ─> Select
          .0 [as product_id]
          └─> ColumnReference[0]
              .of
              └─> Select
                  .0 [as product_id]
                  └─> ColumnReference[3]
                      .of
                      └─> DataTable["orders"]
      """

    validateDataflowGraph(query, tree)
  }

  def testWithClauseSimple2() {
    // Selecting a nonexistent column.
    val query = "with t1 AS (select product_id FROM orders) select fake_column FROM t1"

    assertException(query, classOf[UnknownColumnException])
  }

  def testWithClauseSimple3() {
    // Unknown table but selecting known column.
    val query = "WITH t1 AS (SELECT product_id FROM fake_table) SELECT product_id FROM t1"
    val tree = """
      ─> Select
          .0 [as product_id]
          └─> ColumnReference[0]
              .of
              └─> Select
                  .0 [as product_id]
                  └─> ColumnReference[0]
                      .of
                      └─> DataTable["fake_table"]
      """

    validateDataflowGraph(query, tree, Transformer.SCHEMA_MODE.BEST_EFFORT)
  }

  def testCaseInsensitivityOfColumnReference1() {
    val query = "SELECT OrDER_iD FROM orders"
    val tree = """
      ─> Select
          .0 [as order_id]
          └─> ColumnReference[0]
              .of
              └─> DataTable["orders"]
      """

    validateDataflowGraph(query, tree)
  }

  def testCaseInsensitivityOfColumnReference2() {
    val query = "with t1 AS (select customer_id AS CUSTOMER_ID FROM orders) select CusTOMer_Id FROM t1"
    val tree = """
      ─> Select
          .0 [as customer_id]
          └─> ColumnReference[0]
              .of
              └─> Select
                  .0 [as CUSTOMER_ID]
                  └─> ColumnReference[2]
                      .of
                      └─> DataTable["orders"]
      """

    validateDataflowGraph(query, tree)
  }

  def testMultipleWithAliases() {
    val query = "with t1 AS (select uuid FROM orders), t2 AS (select product_id FROM products) select product_id FROM t2"
    val tree = """
      ─> Select
          .0 [as product_id]
          └─> ColumnReference[0]
              .of
              └─> Select
                  .0 [as product_id]
                  └─> ColumnReference[0]
                      .of
                      └─> DataTable["products"]
      """

    validateDataflowGraph(query, tree)
  }

  def testNestedWith() {
    val query = """
      with t1 AS (select order_id FROM orders),
           t2 AS (with t3 AS (select * FROM t1) select * FROM t3)
           select order_id FROM t2
     """

    val tree = """
      ─> Select
          .0 [as order_id]
          └─> ColumnReference[0]
              .of
              └─> Select
                  .0 [as order_id]
                  └─> ColumnReference[0]
                      .of
                      └─> Select
                          .0 [as order_id]
                          └─> ColumnReference[0]
                              .of
                              └─> Select
                                  .0 [as order_id]
                                  └─> ColumnReference[0]
                                      .of
                                      └─> DataTable["orders"]
      """

    validateDataflowGraph(query, tree)
  }

  def testWithAliasInsideWith1() {
    val query = """
       WITH t1 AS (SELECT product_id FROM orders),
            t2 AS (SELECT * FROM t1)
       SELECT product_id FROM t2
       """

    val tree = """
      ─> Select
          .0 [as product_id]
          └─> ColumnReference[0]
              .of
              └─> Select
                  .0 [as product_id]
                  └─> ColumnReference[0]
                      .of
                      └─> Select
                          .0 [as product_id]
                          └─> ColumnReference[3]
                              .of
                              └─> DataTable["orders"]
      """

    validateDataflowGraph(query, tree)
  }

  def testDeferenceWithCapitalizedName() {
    val query = """
      WITH t1 as
        (SELECT P.name, O.order_date FROM orders AS O left join products AS P
                                                 ON O.product_id = P.product_id)
      SELECT order_date FROM t1
      """

    val tree = """
      ─> Select
          .0 [as order_date]
          └─> ColumnReference[1]
              .of
              └─> Select
                  .0 [as name]
                  ├─> ColumnReference[7]
                  │   .of
                  │   └─> Join[LEFT]
                  │       .left
                  │       ├─> DataTable["orders"]
                  │       .right
                  │       ├─> DataTable["products"]
                  │       .condition
                  │       └─> Function[EQUAL]
                  │           .arg0
                  │           ├─> ColumnReference[3]
                  │           │   .of
                  │           │   └─> DataTable["orders"]
                  │           .arg1
                  │           └─> ColumnReference[0]
                  │               .of
                  │               └─> DataTable["products"]
                  .1 [as order_date]
                  └─> ColumnReference[1]
                      .of
                      └─> ... (Join[LEFT])
      """

    validateDataflowGraph(query, tree)
  }

  // variant of above with unknown table in first WITH clause
  def testWithAliasInsideWith2() {
    val query = "WITH t1 AS (SELECT product_id FROM fake_table), t2 AS (SELECT * FROM t1) SELECT product_id FROM t2"
    val tree = """
      ─> Select
          .0 [as product_id]
          └─> ColumnReference[0]
              .of
              └─> Select
                  .0 [as product_id]
                  └─> ColumnReference[0]
                      .of
                      └─> Select
                          .0 [as product_id]
                          └─> ColumnReference[0]
                              .of
                              └─> DataTable["fake_table"]
      """

    validateDataflowGraph(query, tree, Transformer.SCHEMA_MODE.BEST_EFFORT)
  }

  def testWithAliasInsideWithUnknownColumn() {
    val query = "with t1 AS (select uuid FROM fake_table), t2 AS (select * FROM t1) select product_id FROM t2"

    assertException(query, classOf[UnknownColumnException])
  }

  // Regression: test WITH alias (t1) that is aliased (to "z") inside another WITH (t2). This is a simplified version of
  // a query that previously parsed to an incorrect graph.
  def testWithAliasInsideWithWithAlias() {
    val query = "with t1 AS (select order_id FROM orders), t2 AS (select z.order_id FROM t1 z) select * FROM t2"
    val tree = """
      ─> Select
          .0 [as order_id]
          └─> ColumnReference[0]
              .of
              └─> Select
                  .0 [as order_id]
                  └─> ColumnReference[0]
                      .of
                      └─> Select
                          .0 [as order_id]
                          └─> ColumnReference[0]
                              .of
                              └─> DataTable["orders"]
      """

    validateDataflowGraph(query, tree)
  }

  def testWithAndJoinInnerRelation1() {
    val query = "WITH t1 AS (SELECT product_id FROM orders), t2 AS (SELECT product_id FROM products) SELECT t1.product_id FROM t1, t2"
    val tree = """
      ─> Select
          .0 [as product_id]
          └─> ColumnReference[0]
              .of
              └─> Join[CROSS]
                  .left
                  ├─> Select
                  │   .0 [as product_id]
                  │   └─> ColumnReference[3]
                  │       .of
                  │       └─> DataTable["orders"]
                  .right
                  └─> Select
                      .0 [as product_id]
                      └─> ColumnReference[0]
                          .of
                          └─> DataTable["products"]
      """

    validateDataflowGraph(query, tree)
  }

  def testWithAndJoinInnerRelation2() {
    val query = "WITH t1 AS (SELECT product_id FROM orders), t2 AS (SELECT product_id FROM products) SELECT t2.product_id FROM t1, t2"
    val tree = """
      ─> Select
          .0 [as product_id]
          └─> ColumnReference[1]
              .of
              └─> Join[CROSS]
                  .left
                  ├─> Select
                  │   .0 [as product_id]
                  │   └─> ColumnReference[3]
                  │       .of
                  │       └─> DataTable["orders"]
                  .right
                  └─> Select
                      .0 [as product_id]
                      └─> ColumnReference[0]
                          .of
                          └─> DataTable["products"]
      """

    validateDataflowGraph(query, tree)
  }

  def testWithAndJoinInnerRelation3() {
    val query = "WITH t1 AS (SELECT * FROM orders), t2 AS (SELECT * FROM products) SELECT t2.product_id FROM t1, t2"
    val tree = """
      ─> Select
          .0 [as product_id]
          └─> ColumnReference[6]
              .of
              └─> Join[CROSS]
                  .left
                  ├─> Select
                  │   .0 [as order_id]
                  │   ├─> ColumnReference[0]
                  │   │   .of
                  │   │   └─> DataTable["orders"]
                  │   .1 [as order_date]
                  │   ├─> ColumnReference[1]
                  │   │   .of
                  │   │   └─> DataTable["orders"]
                  │   .2 [as customer_id]
                  │   ├─> ColumnReference[2]
                  │   │   .of
                  │   │   └─> DataTable["orders"]
                  │   .3 [as product_id]
                  │   ├─> ColumnReference[3]
                  │   │   .of
                  │   │   └─> DataTable["orders"]
                  │   .4 [as quantity]
                  │   ├─> ColumnReference[4]
                  │   │   .of
                  │   │   └─> DataTable["orders"]
                  │   .5 [as order_cost]
                  │   └─> ColumnReference[5]
                  │       .of
                  │       └─> DataTable["orders"]
                  .right
                  └─> Select
                      .0 [as product_id]
                      ├─> ColumnReference[0]
                      │   .of
                      │   └─> DataTable["products"]
                      .1 [as name]
                      ├─> ColumnReference[1]
                      │   .of
                      │   └─> DataTable["products"]
                      .2 [as price]
                      └─> ColumnReference[2]
                          .of
                          └─> DataTable["products"]
      """

    validateDataflowGraph(query, tree)
  }

  def testWhereExpression() {
    val query = "SELECT order_id FROM orders where 1=1"
    val tree = """
      ─> Select
          .0 [as order_id]
          ├─> ColumnReference[0]
          │   .of
          │   └─> DataTable["orders"]
          .where
          └─> Function[EQUAL]
              .arg0
              ├─> UnstructuredReference[literal(1)]
              .arg1
              └─> UnstructuredReference[literal(1)]
      """

    validateDataflowGraph(query, tree)
  }

  // Test a wildcard query ON a table for which we don't know the schema, which should fail.
  def testAmbiguousWildcardSelect1() {
    val query = "SELECT * FROM fake_table"
    assertException(query, classOf[AmbiguousWildcardException])
  }

  def testWildcardSelectUsingInferredSchema() {
    // In strict mode, this query should fail. If best effort mode, this query should success and the inferred column
    // 'fake_column' in 'fake_table' is assumed to be the complete schema (see comment in Transformer.SCHEMA_MODE)
    val query = "WITH t1 AS (SELECT fake_column FROM fake_table) SELECT * FROM fake_table"
    val tree = """
      ─> Select
          .0 [as fake_column]
          └─> ColumnReference[0]
              .of
              └─> DataTable["fake_table"]
      """

    assertException(query, classOf[UndefinedSchemaException], Transformer.SCHEMA_MODE.STRICT)
    validateDataflowGraph(query, tree, Transformer.SCHEMA_MODE.BEST_EFFORT)
  }

  def testAugmentedSchema() {
    // In strict mode, this query should fail due to unknown column. In best-effort mode, this query should produce
    // a dataflow graph with two new columns, 'new_column1' and 'new_column2', based on the structure of the query.
    val query = """
      WITH t1 AS (SELECT products.new_column1 FROM products JOIN orders ON products.new_column2 = orders.product_id)
      SELECT * FROM t1
      """

    val tree = """
      ─> Select
          .0 [as new_column1]
          └─> ColumnReference[0]
              .of
              └─> Select
                  .0 [as new_column1]
                  └─> ColumnReference[4]
                      .of
                      └─> Join[INNER]
                          .left
                          ├─> DataTable["products"]
                          .right
                          ├─> DataTable["orders"]
                          .condition
                          └─> Function[EQUAL]
                              .arg0
                              ├─> ColumnReference[3]
                              │   .of
                              │   └─> DataTable["products"]
                              .arg1
                              └─> ColumnReference[3]
                                  .of
                                  └─> DataTable["orders"]
      """

    assertException(query, classOf[UnknownColumnException], Transformer.SCHEMA_MODE.STRICT)
    validateDataflowGraph(query, tree, Transformer.SCHEMA_MODE.BEST_EFFORT)
  }

  def testWildcardSelect() {
    val query = "WITH t1 AS (SELECT uuid FROM fake_table) SELECT * FROM t1"
    val tree = """
      ─> Select
          .0 [as uuid]
          └─> ColumnReference[0]
              .of
              └─> Select
                  .0 [as uuid]
                  └─> ColumnReference[0]
                      .of
                      └─> DataTable["fake_table"]
      """

    validateDataflowGraph(query, tree, Transformer.SCHEMA_MODE.BEST_EFFORT)
  }

  def testKnownColumnUnknownTableStrictMode() {
    // In strict mode, selecting a column from a table with unknown schema should throw an UndefinedSchemaException
    val query = "SELECT fake_column FROM fake_table"

    assertException(query, classOf[UndefinedSchemaException], Transformer.SCHEMA_MODE.STRICT)
  }

  def testKnownColumnUnknownTable() {
    val query = "SELECT fake_column FROM fake_table"
    val tree = """
      ─> Select
          .0 [as fake_column]
          └─> ColumnReference[0]
              .of
              └─> DataTable["fake_table"]
      """

    validateDataflowGraph(query, tree, Transformer.SCHEMA_MODE.BEST_EFFORT)
  }

  def testCountStar() {
    val query = "SELECT count(*) FROM orders"
    val tree = """
      ─> Select
          .0 [as count]
          └─> Function[count]
              .arg0
              └─> UnstructuredReference[countAll]
                  .arg0
                  └─> DataTable["orders"]
      """

    validateDataflowGraph(query, tree)
  }

  def testCountLiteral() {
    val query = "SELECT count(1) FROM orders"
    val tree = """
      ─> Select
          .0 [as count]
          └─> Function[count]
              .arg0
              └─> UnstructuredReference[countAll]
                  .arg0
                  └─> DataTable["orders"]
      """

    validateDataflowGraph(query, tree)
  }

  def testColumnResolutionUsingSchemaAmbiguous() {
    val query = "" +
      "WITH t1 as\n" +
      "  (SELECT dd.driver_uuid FROM\n" +
      "     (fact_trip JOIN dim_city      ON fact_trip.product_id = dim_city.product_id) t \n" +
      "                JOIN dim_driver dd ON t.product_id     = dd.driver_uuid \n" +
      "   )\n" +
      "   SELECT count(*) FROM t1 where driver_uuid = 'abcdefg'"

    // t.product_id is ambiguous (it could refer to either dim_city.city or fact_trip.product_id)
    assertException(
      query,
      classOf[AmbiguousColumnReference]
    )
  }

  def testJoin() {
    val query = "SELECT order_id FROM orders join products ON orders.product_id = products.product_id"
    val tree = """
      ─> Select
          .0 [as order_id]
          └─> ColumnReference[0]
              .of
              └─> Join[INNER]
                  .left
                  ├─> DataTable["orders"]
                  .right
                  ├─> DataTable["products"]
                  .condition
                  └─> Function[EQUAL]
                      .arg0
                      ├─> ColumnReference[3]
                      │   .of
                      │   └─> DataTable["orders"]
                      .arg1
                      └─> ColumnReference[0]
                          .of
                          └─> DataTable["products"]
      """

    validateDataflowGraph(query, tree)
  }

  def testJoinOnQualifiedName() {
    val query = "SELECT order_id FROM orders JOIN products ON orders.product_id = products.product_id"
    val tree = """
      ─> Select
          .0 [as order_id]
          └─> ColumnReference[0]
              .of
              └─> Join[INNER]
                  .left
                  ├─> DataTable["orders"]
                  .right
                  ├─> DataTable["products"]
                  .condition
                  └─> Function[EQUAL]
                      .arg0
                      ├─> ColumnReference[3]
                      │   .of
                      │   └─> DataTable["orders"]
                      .arg1
                      └─> ColumnReference[0]
                          .of
                          └─> DataTable["products"]
      """

    validateDataflowGraph(query, tree)
  }

  def testJoinOnQualifiedNameAmbiguous() {
    // "name" could refer to either products.name or customers.name
    val query = "SELECT name FROM products JOIN customers ON products.product_id = customers.customer_id"

    assertException(query, classOf[AmbiguousColumnReference])
  }

  def testJoinOnQualifiedNameNonexistent() {
    val query = "SELECT uuid FROM orders join products ON nonexistent_column = product_id"

    assertException(query, classOf[UnknownColumnException])
  }

  def testMultipleJoins1() {
    val query = "SELECT t1.product_id FROM (orders t1 JOIN products t2 ON t1.product_id = t2.product_id) JOIN customers t3 ON t1.product_id = t3.customer_id"
    val tree = """
      ─> Select
          .0 [as product_id]
          └─> ColumnReference[3]
              .of
              └─> Join[INNER]
                  .left
                  ├─> Join[INNER]
                  │   .left
                  │   ├─> DataTable["orders"]
                  │   .right
                  │   ├─> DataTable["products"]
                  │   .condition
                  │   └─> Function[EQUAL]
                  │       .arg0
                  │       ├─> ColumnReference[3]
                  │       │   .of
                  │       │   └─> DataTable["orders"]
                  │       .arg1
                  │       └─> ColumnReference[0]
                  │           .of
                  │           └─> DataTable["products"]
                  .right
                  ├─> DataTable["customers"]
                  .condition
                  └─> Function[EQUAL]
                      .arg0
                      ├─> ColumnReference[3]
                      │   .of
                      │   └─> ... (Join[INNER])
                      .arg1
                      └─> ColumnReference[0]
                          .of
                          └─> DataTable["customers"]
      """

    validateDataflowGraph(query, tree)
  }

  def testMultipleJoins2() {
    val query = "SELECT t2.product_id FROM (orders t1 JOIN products t2 ON t1.product_id = t2.product_id) JOIN customers t3 ON t1.product_id = t3.customer_id"
    val tree = """
      ─> Select
          .0 [as product_id]
          └─> ColumnReference[6]
              .of
              └─> Join[INNER]
                  .left
                  ├─> Join[INNER]
                  │   .left
                  │   ├─> DataTable["orders"]
                  │   .right
                  │   ├─> DataTable["products"]
                  │   .condition
                  │   └─> Function[EQUAL]
                  │       .arg0
                  │       ├─> ColumnReference[3]
                  │       │   .of
                  │       │   └─> DataTable["orders"]
                  │       .arg1
                  │       └─> ColumnReference[0]
                  │           .of
                  │           └─> DataTable["products"]
                  .right
                  ├─> DataTable["customers"]
                  .condition
                  └─> Function[EQUAL]
                      .arg0
                      ├─> ColumnReference[3]
                      │   .of
                      │   └─> ... (Join[INNER])
                      .arg1
                      └─> ColumnReference[0]
                          .of
                          └─> DataTable["customers"]
      """

    validateDataflowGraph(query, tree)
  }

  def testMultipleJoins3() {
    val query = "SELECT orders.product_id FROM orders, products, customers"
    val tree = """
      ─> Select
          .0 [as product_id]
          └─> ColumnReference[3]
              .of
              └─> Join[CROSS]
                  .left
                  ├─> Join[CROSS]
                  │   .left
                  │   ├─> DataTable["orders"]
                  │   .right
                  │   └─> DataTable["products"]
                  .right
                  └─> DataTable["customers"]
      """

    validateDataflowGraph(query, tree)
  }

  def testMultipleJoins4() {
    val query = "SELECT products.product_id FROM orders t1, products t2, customers t3"
    val tree = """
      ─> Select
          .0 [as product_id]
          └─> ColumnReference[6]
              .of
              └─> Join[CROSS]
                  .left
                  ├─> Join[CROSS]
                  │   .left
                  │   ├─> DataTable["orders"]
                  │   .right
                  │   └─> DataTable["products"]
                  .right
                  └─> DataTable["customers"]
      """

    validateDataflowGraph(query, tree)
  }

  def testMultipleJoins5() {
    val query = "SELECT customers.customer_id FROM orders t1, products t2, customers t3"
    val tree = """
      ─> Select
          .0 [as customer_id]
          └─> ColumnReference[9]
              .of
              └─> Join[CROSS]
                  .left
                  ├─> Join[CROSS]
                  │   .left
                  │   ├─> DataTable["orders"]
                  │   .right
                  │   └─> DataTable["products"]
                  .right
                  └─> DataTable["customers"]
      """

    validateDataflowGraph(query, tree)
  }

  def testMultipleJoins6() {
    val query = "SELECT t1.product_id FROM orders t1, products t2, customers t3"
    val tree = """
      ─> Select
          .0 [as product_id]
          └─> ColumnReference[3]
              .of
              └─> Join[CROSS]
                  .left
                  ├─> Join[CROSS]
                  │   .left
                  │   ├─> DataTable["orders"]
                  │   .right
                  │   └─> DataTable["products"]
                  .right
                  └─> DataTable["customers"]
      """

    validateDataflowGraph(query, tree)
  }

  def testMultipleJoins7() {
    val query = "SELECT t2.product_id FROM orders t1, products t2, customers t3"
    val tree = """
      ─> Select
          .0 [as product_id]
          └─> ColumnReference[6]
              .of
              └─> Join[CROSS]
                  .left
                  ├─> Join[CROSS]
                  │   .left
                  │   ├─> DataTable["orders"]
                  │   .right
                  │   └─> DataTable["products"]
                  .right
                  └─> DataTable["customers"]
      """

    validateDataflowGraph(query, tree)
  }

  def testMultipleJoins8() {
    val query = "SELECT t3.customer_id FROM orders t1, products t2, customers t3"
    val tree = """
      ─> Select
          .0 [as customer_id]
          └─> ColumnReference[9]
              .of
              └─> Join[CROSS]
                  .left
                  ├─> Join[CROSS]
                  │   .left
                  │   ├─> DataTable["orders"]
                  │   .right
                  │   └─> DataTable["products"]
                  .right
                  └─> DataTable["customers"]
      """

    validateDataflowGraph(query, tree)
  }

  def testAmbiguousColumName() {
    val query = "SELECT product_id FROM orders, products"

    assertException(query, classOf[AmbiguousColumnReference], Transformer.SCHEMA_MODE.STRICT)
    assertException(query, classOf[AmbiguousColumnReference], Transformer.SCHEMA_MODE.BEST_EFFORT)
  }

  def testSelectImplicitJoin() {
    val query = "SELECT order_date FROM orders, products"
    val tree = """
      ─> Select
          .0 [as order_date]
          └─> ColumnReference[1]
              .of
              └─> Join[CROSS]
                  .left
                  ├─> DataTable["orders"]
                  .right
                  └─> DataTable["products"]
      """

    validateDataflowGraph(query, tree)
  }

  def testSelectNonexistentColumnOnJoinWithKnownSchemas() {
    val query = "SELECT fake_column FROM orders JOIN products ON orders.product_id = products.product_id"

    // In strict mode this query should result in an error because we know column fake_column does not exist in the JOINed relation.
    assertException(query, classOf[UnknownColumnException], Transformer.SCHEMA_MODE.STRICT)
  }

  def testSelectNonexistentColumnOnJoinWithUnknownSchemas() {
    val query = "SELECT fake_column FROM fake_table1 JOIN fake_table2 ON fake_table1.product_id = fake_table2.product_id"

    assertException(query, classOf[UndefinedSchemaException], Transformer.SCHEMA_MODE.STRICT)

    // In best-effort mode, this query should raise UnknownColumnException because we cannot determine
    // which joined relation contains column 'fake_column'

    assertException(query,  classOf[UnknownColumnException],
      Transformer.SCHEMA_MODE.BEST_EFFORT
    )
  }

  def testSelectColumnInferredFromJoinCondition() {
    // In best-effort mode, this query should succeed because we know from the join condition that fake_table_2
    // contains a column named 'fake_column', therefore can assume that the column referenced in
    // the outermost select belongs to fake_table_2.
    val query = "SELECT fake_column FROM fake_table1 JOIN fake_table2 ON fake_table1.blah = fake_table2.fake_column"
    val tree = """
      ─> Select
          .0 [as fake_column]
          └─> ColumnReference[1]
              .of
              └─> Join[INNER]
                  .left
                  ├─> DataTable["fake_table1"]
                  .right
                  ├─> DataTable["fake_table2"]
                  .condition
                  └─> Function[EQUAL]
                      .arg0
                      ├─> ColumnReference[0]
                      │   .of
                      │   └─> DataTable["fake_table1"]
                      .arg1
                      └─> ColumnReference[0]
                          .of
                          └─> DataTable["fake_table2"]
      """

    validateDataflowGraph(query, tree, Transformer.SCHEMA_MODE.BEST_EFFORT)
  }

  def testSelectColumnInferredFromJoinCondition2() {
    // Same test as above using table aliases.
    val query = "SELECT fake_column FROM fake_table1 ft1 JOIN fake_table2 ft2 ON ft1.fake_column = ft2.product_id"
    val tree = """
      ─> Select
          .0 [as fake_column]
          └─> ColumnReference[0]
              .of
              └─> Join[INNER]
                  .left
                  ├─> DataTable["fake_table1"]
                  .right
                  ├─> DataTable["fake_table2"]
                  .condition
                  └─> Function[EQUAL]
                      .arg0
                      ├─> ColumnReference[0]
                      │   .of
                      │   └─> DataTable["fake_table1"]
                      .arg1
                      └─> ColumnReference[0]
                          .of
                          └─> DataTable["fake_table2"]
      """

    validateDataflowGraph(query, tree, Transformer.SCHEMA_MODE.BEST_EFFORT)
  }

  def testAmbiguousColumnFromInferredFromJoinConditions() {
    val query = "SELECT fake_column FROM orders JOIN products ON orders.fake_column = products.fake_column"

    // In strict mode, this is a schema error because "fake_column" is not defined.
    assertException(query, classOf[UnknownColumnException], Transformer.SCHEMA_MODE.STRICT)

    // In best-effort mode, we infer that fake_column appears in both fake_table1 and fake_table2, therefore this is an
    // ambiguous column reference
    assertException(query, classOf[AmbiguousColumnReference], Transformer.SCHEMA_MODE.BEST_EFFORT)
  }

  def testCountStarImplicitJoin() {
    val query = "SELECT count(*) FROM orders, products"
    val tree = """
      ─> Select
          .0 [as count]
          └─> Function[count]
              .arg0
              └─> UnstructuredReference[countAll]
                  .arg0
                  └─> Join[CROSS]
                      .left
                      ├─> DataTable["orders"]
                      .right
                      └─> DataTable["products"]
      """

    validateDataflowGraph(query, tree)
  }

  def testImplicitJoinWithAlias() {
    val query = "SELECT a.product_id, b.product_id FROM orders a, products b"
    val tree = """
      ─> Select
          .0 [as product_id]
          ├─> ColumnReference[3]
          │   .of
          │   └─> Join[CROSS]
          │       .left
          │       ├─> DataTable["orders"]
          │       .right
          │       └─> DataTable["products"]
          .1 [as product_id]
          └─> ColumnReference[6]
              .of
              └─> ... (Join[CROSS])
      """

    validateDataflowGraph(query, tree)
  }

  def testImplicitJoinWithoutAlias() {
    val query = "SELECT orders.product_id, products.product_id FROM orders, products"
    val tree =
      """
      ─> Select
          .0 [as product_id]
          ├─> ColumnReference[3]
          │   .of
          │   └─> Join[CROSS]
          │       .left
          │       ├─> DataTable["orders"]
          │       .right
          │       └─> DataTable["products"]
          .1 [as product_id]
          └─> ColumnReference[6]
              .of
              └─> ... (Join[CROSS])
      """

    validateDataflowGraph(query, tree)
  }

  def testLeftJoin() {
    val query = "SELECT name FROM orders LEFT JOIN products ON orders.product_id = products.product_id"
    val tree = """
      ─> Select
          .0 [as name]
          └─> ColumnReference[7]
              .of
              └─> Join[LEFT]
                  .left
                  ├─> DataTable["orders"]
                  .right
                  ├─> DataTable["products"]
                  .condition
                  └─> Function[EQUAL]
                      .arg0
                      ├─> ColumnReference[3]
                      │   .of
                      │   └─> DataTable["orders"]
                      .arg1
                      └─> ColumnReference[0]
                          .of
                          └─> DataTable["products"]
      """

    validateDataflowGraph(query, tree)
  }

  def testRightJoin() {
    val query = "SELECT products.product_id FROM orders RIGHT JOIN products ON orders.product_id = products.product_id"
    val tree = """
      ─> Select
          .0 [as product_id]
          └─> ColumnReference[6]
              .of
              └─> Join[RIGHT]
                  .left
                  ├─> DataTable["orders"]
                  .right
                  ├─> DataTable["products"]
                  .condition
                  └─> Function[EQUAL]
                      .arg0
                      ├─> ColumnReference[3]
                      │   .of
                      │   └─> DataTable["orders"]
                      .arg1
                      └─> ColumnReference[0]
                          .of
                          └─> DataTable["products"]
      """

    validateDataflowGraph(query, tree)
  }

  def testGroupBy1() {
    val query = "SELECT product_id, name FROM products GROUP BY name"
    val tree = """
      ─> Select
          .0 [as product_id]
          ├─> ColumnReference[0]
          │   .of
          │   └─> DataTable["products"]
          .1 [as name]
          ├─> ColumnReference[1]
          │   .of
          │   └─> DataTable["products"]
          .groupBy
          └─> ... (ColumnReference[1])
      """

    validateDataflowGraph(query, tree)
  }

  def testGroupBy2() {
    val query = "SELECT product_id, name FROM products GROUP BY 2"
    val tree = """
      ─> Select
          .0 [as product_id]
          ├─> ColumnReference[0]
          │   .of
          │   └─> DataTable["products"]
          .1 [as name]
          ├─> ColumnReference[1]
          │   .of
          │   └─> DataTable["products"]
          .groupBy
          └─> ... (ColumnReference[1])
      """

    validateDataflowGraph(query, tree)
  }

  def testGroupBy3() {
    val query = "SELECT product_id, name AS a FROM products GROUP BY a"
    val tree = """
      ─> Select
          .0 [as product_id]
          ├─> ColumnReference[0]
          │   .of
          │   └─> DataTable["products"]
          .1 [as a]
          ├─> ColumnReference[1]
          │   .of
          │   └─> DataTable["products"]
          .groupBy
          └─> ... (ColumnReference[1])
      """

    validateDataflowGraph(query, tree)
  }

  def testGroupBy4() {
    val query = "SELECT product_id AS a, name AS b FROM products GROUP BY b, a"
    val tree = """
      ─> Select
          .0 [as a]
          ├─> ColumnReference[0]
          │   .of
          │   └─> DataTable["products"]
          .1 [as b]
          ├─> ColumnReference[1]
          │   .of
          │   └─> DataTable["products"]
          .groupBy
          ├─> ... (ColumnReference[1])
          .groupBy
          └─> ... (ColumnReference[0])
      """

    validateDataflowGraph(query, tree)
  }

  def testGroupBy5() {
    val query = "SELECT product_id, name FROM products GROUP BY 1, 2"
    val tree = """
      ─> Select
          .0 [as product_id]
          ├─> ColumnReference[0]
          │   .of
          │   └─> DataTable["products"]
          .1 [as name]
          ├─> ColumnReference[1]
          │   .of
          │   └─> DataTable["products"]
          .groupBy
          ├─> ... (ColumnReference[0])
          .groupBy
          └─> ... (ColumnReference[1])
      """

    validateDataflowGraph(query, tree)
  }

  def testQualifiedTableName() {
    val query = "SELECT order_id FROM public.orders"
    val tree = """
      ─> Select
          .0 [as order_id]
          └─> ColumnReference[0]
              .of
              └─> DataTable["orders"]
      """

    validateDataflowGraph(query, tree, Transformer.SCHEMA_MODE.STRICT)
  }

  def testSysdatePrimitive1() {
    // Make sure the 'sysdate' macro is interpreted as a function and not a column reference, even when it is not
    // escaped by quotes.
    val query = "SELECT to_char(sysdate,'yyyy-mm-dd HH12:MI') AS my_date FROM orders"
    val tree = """
      ─> Select
          .0 [as my_date]
          └─> Function[to_char]
              .arg0
              ├─> Function[sysdate]
              .arg1
              └─> UnstructuredReference[literal('yyyy-mm-dd HH12:MI')]
      """

    validateDataflowGraph(query, tree)
  }

  def testSysdatePrimitive2() {
    val query = "SELECT order_id FROM orders WHERE order_date > sysdate - 5"
    val tree = """
      ─> Select
          .0 [as order_id]
          ├─> ColumnReference[0]
          │   .of
          │   └─> DataTable["orders"]
          .where
          └─> Function[GREATER_THAN]
              .arg0
              ├─> ColumnReference[1]
              │   .of
              │   └─> DataTable["orders"]
              .arg1
              └─> UnstructuredReference[arithmeticBinary]
                  .arg0
                  ├─> Function[sysdate]
                  .arg1
                  └─> UnstructuredReference[literal(5)]
      """

    validateDataflowGraph(query, tree)
  }

  def testGroupByQualifiedName() {
    val query = "SELECT order_id, count(*) FROM orders GROUP BY orders.order_id"
    val tree = """
      ─> Select
          .0 [as order_id]
          ├─> ColumnReference[0]
          │   .of
          │   └─> DataTable["orders"]
          .1 [as count]
          ├─> Function[count]
          │   .arg0
          │   └─> UnstructuredReference[countAll]
          │       .arg0
          │       └─> DataTable["orders"]
          .groupBy
          └─> ... (ColumnReference[0])
      """

    validateDataflowGraph(query, tree)
  }

  def testGroupByQualifiedNameInnerRelation() {
    val query = "SELECT orders.order_id, customers.customer_id, count(*) FROM orders INNER JOIN customers ON orders.customer_id = customers.customer_id GROUP BY customers.customer_id, orders.order_id"
    val tree = """
      ─> Select
          .0 [as order_id]
          ├─> ColumnReference[0]
          │   .of
          │   └─> Join[INNER]
          │       .left
          │       ├─> DataTable["orders"]
          │       .right
          │       ├─> DataTable["customers"]
          │       .condition
          │       └─> Function[EQUAL]
          │           .arg0
          │           ├─> ColumnReference[2]
          │           │   .of
          │           │   └─> DataTable["orders"]
          │           .arg1
          │           └─> ColumnReference[0]
          │               .of
          │               └─> DataTable["customers"]
          .1 [as customer_id]
          ├─> ColumnReference[6]
          │   .of
          │   └─> ... (Join[INNER])
          .2 [as count]
          ├─> Function[count]
          │   .arg0
          │   └─> UnstructuredReference[countAll]
          │       .arg0
          │       └─> ... (Join[INNER])
          .groupBy
          ├─> ... (ColumnReference[6])
          .groupBy
          └─> ... (ColumnReference[0])
      """

    validateDataflowGraph(query, tree)
  }

  def testCountWithColumnReference() {
    val query = "SELECT count(order_id) FROM orders"
    val tree = """
      ─> Select
          .0 [as count]
          └─> Function[count]
              .arg0
              └─> ColumnReference[0]
                  .of
                  └─> DataTable["orders"]
      """

    validateDataflowGraph(query, tree)
  }

  def testCountWithColumnReferenceDistinct() {
    val query = "SELECT count (DISTINCT order_id) FROM orders"
    val tree = """
      ─> Select
          .0 [as count]
          └─> Function[count]
              .arg0
              └─> ColumnReference[0]
                  .of
                  └─> DataTable["orders"]
      """

    validateDataflowGraph(query, tree)
  }

  def testSelectInWhere() {
    val query = """
       SELECT COUNT(*) FROM orders
       WHERE product_id = (SELECT product_id FROM products WHERE product_id = 1)
     """

    val tree = """
      ─> Select
          .0 [as count]
          ├─> Function[count]
          │   .arg0
          │   └─> UnstructuredReference[countAll]
          │       .arg0
          │       └─> DataTable["orders"]
          .where
          └─> Function[EQUAL]
              .arg0
              ├─> ColumnReference[3]
              │   .of
              │   └─> DataTable["orders"]
              .arg1
              └─> UnstructuredReference[wrappedRelation]
                  .arg0
                  └─> Select
                      .0 [as product_id]
                      ├─> ColumnReference[0]
                      │   .of
                      │   └─> DataTable["products"]
                      .where
                      └─> Function[EQUAL]
                          .arg0
                          ├─> ColumnReference[0]
                          │   .of
                          │   └─> DataTable["products"]
                          .arg1
                          └─> UnstructuredReference[literal(1)]
      """

    validateDataflowGraph(query, tree)
  }


  def testAliasCollisionInWith() {
    // This pattern caused an InfiniteLoopException during tree transformation. Many queries use this
    // pattern: creating a subrelation and giving it the same name as the table from where the data came. The
    // query-defined alias should shadow the database table, not vice-versa (that is, the SELECT * below
    // should return just a UUID column, not all the columns in database table orders).
    val query = "with orders AS (" +
      "    SELECT order_id FROM orders" +
      ") " +
      "SELECT * FROM orders"

    val tree = """
      ─> Select
          .0 [as order_id]
          └─> ColumnReference[0]
              .of
              └─> Select
                  .0 [as order_id]
                  └─> ColumnReference[0]
                      .of
                      └─> DataTable["orders"]
      """

    validateDataflowGraph(query, tree)
  }

  def testAliasCollisionInAliasedRelation() {
    val query = "SELECT orders.product_id FROM (SELECT order_id, product_id FROM orders) orders"
    val tree = """
      ─> Select
          .0 [as product_id]
          └─> ColumnReference[1]
              .of
              └─> Select
                  .0 [as order_id]
                  ├─> ColumnReference[0]
                  │   .of
                  │   └─> DataTable["orders"]
                  .1 [as product_id]
                  └─> ColumnReference[3]
                      .of
                      └─> DataTable["orders"]
      """

    validateDataflowGraph(query, tree)
  }

  def testUnion() {
    val query = "SELECT name FROM products UNION SELECT name FROM customers"
    val tree = """
      ─> Union["UNION"]
          .r0
          ├─> Select
          │   .0 [as name]
          │   └─> ColumnReference[1]
          │       .of
          │       └─> DataTable["products"]
          .r1
          └─> Select
              .0 [as name]
              └─> ColumnReference[1]
                  .of
                  └─> DataTable["customers"]
      """

    validateDataflowGraph(query, tree)
  }

  def testUnionSchemaMismatch() {
    val query1 = "SELECT name FROM products UNION SELECT customer_id FROM customers"
    val query2 = "SELECT name FROM products UNION SELECT name AS xyz FROM customers"
    val query3 = "SELECT name FROM products UNION SELECT name, customer_id FROM products"
    assertException(query1, classOf[InvalidQueryException])
    assertException(query2, classOf[InvalidQueryException])
    assertException(query3, classOf[InvalidQueryException])
  }

  def testExcept() {
    val query = "SELECT name FROM products EXCEPT SELECT name FROM customers"
    val tree = """
      ─> Except["EXCEPT"]
          .left
          ├─> Select
          │   .0 [as name]
          │   └─> ColumnReference[1]
          │       .of
          │       └─> DataTable["products"]
          .right
          └─> Select
              .0 [as name]
              └─> ColumnReference[1]
                  .of
                  └─> DataTable["customers"]
      """

    validateDataflowGraph(query, tree)
  }

  def testExceptSchemaMismatch() {
    val query1 = "SELECT name FROM products EXCEPT SELECT customer_id FROM customers"
    val query2 = "SELECT name FROM products EXCEPT SELECT name AS xyz FROM customers"
    val query3 = "SELECT name FROM products EXCEPT SELECT name, customer_id FROM products"
    assertException(query1, classOf[InvalidQueryException])
    assertException(query2, classOf[InvalidQueryException])
    assertException(query3, classOf[InvalidQueryException])
  }
}

/** Dataflow graph printer for tests. Similar to [TreePrinter.scala] but uses a more compact format.
 */
object CompactTreePrinter {

  def printTree(root: Node): String = {
    val printedNodes = scala.collection.mutable.Set[Node]()
    val builder: StringBuilder = new StringBuilder()

    def process(node: Node, prefix: String, isTail: Boolean, isRoot: Boolean): Unit = {
      val alreadyPrinted = printedNodes.contains(node)
      val nodeStr = node.getClass.getSimpleName + (if (node.nodeStr.isEmpty) "" else "[" + node.nodeStr + "]")

      val nodePrintStr = if (alreadyPrinted && (node.children.size > 0)) s"... (${nodeStr})" else nodeStr
      
      val (thisPrefix, nextPrefix) =
        if (isRoot)
          ("─> ", "    ")
        else if (isTail)
          ("└─> ", "    ")
        else
          ("├─> ", "│   ")

      val childPrefix = prefix + (if (isTail) " " else "│")

      if (!isRoot)
        builder.append("\n")
      builder.append(prefix + thisPrefix + nodePrintStr)
      printedNodes += node

      if (!alreadyPrinted) {
        val namedChildren: Seq[Tuple2[String,Node]] =
          node match {
            case c: ColumnReference => List(("of", c.of))
            case f: Function => f.args.view.zipWithIndex.map{ case (node, idx) => (s"arg${idx}", node) }
            case d: DataTable => Nil
            case s: Select => s.items.view.zipWithIndex.map{ case (node, idx) => (s"${idx} [as ${node.as}]", node.ref) } ++ List(s.where).flatten.map{ ("where", _) } ++ s.groupBy.map{ idx => ("groupBy", s.items(idx).ref) }
            case u: UnstructuredReference => u.children.view.zipWithIndex.map{ case (node, idx) => (s"arg${idx}", node) }
            case j: Join => List(("left", j.left), ("right", j.right)) ++ List(j.condition).flatten.map{ ("condition", _) }
            case u: Union => u.children.view.zipWithIndex.map{ case (node, idx) => (s"r${idx}", node) }
            case e: Except => List(("left", e.left), ("right", e.right))
          }

        namedChildren.view.zipWithIndex.foreach { case ((label, child), idx) =>
          val isLeaf = idx == namedChildren.size - 1
          builder.append(s"\n${childPrefix}   .")
          builder.append(label)
          process(child, prefix + nextPrefix, isLeaf, false)
        }
      }
    }

    process(root, "", true, true)
    builder.toString
  }
}
