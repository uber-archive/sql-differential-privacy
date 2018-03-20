package com.uber.engsec.dp.core

import com.uber.engsec.dp.schema.Schema
import com.uber.engsec.dp.sql.QueryParser
import junit.framework.TestCase

class SchemaTest extends TestCase {
  val db1 = Schema.getDatabase("test")
  val db2 = Schema.getDatabase("test2")

  def testMultiDatabase(): Unit = {
    // Query on db1
    val query1 = "SELECT COUNT(*) FROM orders WHERE product_id = 1"

    // Query on db2
    val query2 = "SELECT COUNT(*) from my_table WHERE my_col = 1"

    QueryParser.parseToRelTree(query1, db1)
    QueryParser.parseToRelTree(query2, db2)

    try {
      QueryParser.parseToRelTree(query1, db2)
      TestCase.fail()
    } catch {
      case _: Exception =>
    }

    try {
      Schema.getDatabase("nonexistDb")
      TestCase.fail()
    } catch {
      case _: Exception =>
    }
  }

  def testSchemaWithEmptyNamespace(): Unit = {
    QueryParser.parseToRelTree("SELECT my_col FROM my_table", db2)
    QueryParser.parseToRelTree("SELECT col FROM subschema.tbl", db2)
  }

  def testStructuredColumn(): Unit = {
    QueryParser.parseToRelTree("SELECT structured_col FROM my_table", db2)
    QueryParser.parseToRelTree("SELECT structured_col.field1 FROM my_table", db2)
    QueryParser.parseToRelTree("SELECT structured_col.field2.subfield1 FROM my_table", db2)
  }
}
