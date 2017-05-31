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

package com.uber.engsec.dp.sql

import java.util.Properties

import com.facebook.presto.sql.parser.{SqlParser => PrestoSqlParser}
import com.facebook.presto.sql.tree.{Query, Statement}
import com.uber.engsec.dp.schema.{CalciteSchemaFromConfig, Schema}
import com.uber.engsec.dp.sql.relational_algebra.RelOrExpr
import com.uber.engsec.dp.exception.ParsingException
import com.uber.engsec.dp.sql.ast.Transformer
import com.uber.engsec.dp.sql.dataflow_graph.Node
import org.apache.calcite.avatica.util.{Casing, Quoting}
import org.apache.calcite.config.{CalciteConnectionConfig, CalciteConnectionConfigImpl}
import org.apache.calcite.plan.Context
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.validate.SqlConformanceEnum
import org.apache.calcite.tools.Frameworks

/** Utility class for parsing SQL queries into different representations.
  */
object QueryParser {
  private val prestoParser: PrestoSqlParser = new PrestoSqlParser

  def printQuery(query: String, treeType: String): Unit = {
    if (AbstractAnalysis.DEBUG) {
      println(s">>>>>>>>>>>>>>>>>>>>>>>>>>>> Parsing query to ${treeType}:")
      println(query)
      println("<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    }
  }

  /** Parse a SQL query into an AST (represented by a Presto tree)
    * @param query The SQL query to be parsed
    * @return The AST root node representing the query
    */
  def parseToPrestoTree(query: String): Query = {
    printQuery(query, "presto tree")

    try {
      return prestoParser.createStatement(query).asInstanceOf[Query]
    }
    catch {
      case e: Exception => {
        // Catch all exceptions that occur during presto parsing and wrap them in our ParsingException exception type.
        throw new ParsingException(e.getMessage)
      }
    }
  }

  /** Parse a SQL query and transform it into a dataflow graph
    * @param query The SQL query to be parsed
    * @return The dataflow graph root node
    */
  def parseToDataflowGraph(query: String): Node = {
    printQuery(query, "dataflow graph")

    val prestoRoot: Statement = parseToPrestoTree(query)
    val transform = new Transformer
    transform.convertToDataflowGraph(prestoRoot)
  }

  /** Parse a SQL query and transform it into a relational algebra representation.
    * @param query The SQL query to be parsed
    * @return The relational algebra tree root node
    */
  def parseToRelTree(query: String): RelOrExpr = {
    printQuery(query, "relational algebra tree")

    val schema = new CalciteSchemaFromConfig
    val planner = {
      val parserConfig = SqlParser.configBuilder
        .setQuoting(Quoting.DOUBLE_QUOTE)
        .setConformance(SqlConformanceEnum.LENIENT)
        .setUnquotedCasing(Casing.UNCHANGED)
        .setQuotedCasing(Casing.UNCHANGED)
        .setCaseSensitive(false)
        .build

      val rootSchema = Frameworks.createRootSchema(true)

      val conformanceProperties = new Properties()
      conformanceProperties.setProperty("conformance", "LENIENT")

      val config = Frameworks.newConfigBuilder
        .defaultSchema(rootSchema.add(Schema.currentDb.namespace, schema))
        .parserConfig(parserConfig)
        .context(new Context() {
          override def unwrap[C](aClass: Class[C]): C = {
            if (aClass.isAssignableFrom(classOf[CalciteConnectionConfig]))
              new CalciteConnectionConfigImpl(conformanceProperties).asInstanceOf[C]
            else
              null.asInstanceOf[C] // ugly but required to return null from Java generic method
          }
          })
        .build

      val planner = Frameworks.getPlanner(config)
      planner
    }

    val parse = planner.parse(query)
    val validate = planner.validate(parse)
    val rel = planner.rel(validate)

    rel.rel
  }
}