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

import com.facebook.presto.sql.parser.{SqlParser => PrestoSqlParser}
import com.facebook.presto.sql.tree.{Query, Statement}
import com.uber.engsec.dp.exception.ParsingException
import com.uber.engsec.dp.schema.Database
import com.uber.engsec.dp.sql.ast.{Transformer => ASTTransformer}
import com.uber.engsec.dp.sql.dataflow_graph.Node
import com.uber.engsec.dp.sql.relational_algebra.{Relation, Transformer => RelTransformer}

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
  def parseToDataflowGraph(query: String, database: Database): Node = {
    printQuery(query, "dataflow graph")

    val prestoRoot: Statement = parseToPrestoTree(query)
    val transform = new ASTTransformer(database)
    transform.convertToDataflowGraph(prestoRoot)
  }

  /** Parse a SQL query and transform it into a relational algebra representation.
    * @param query The SQL query to be parsed
    * @return The relational algebra tree root node
    */
  def parseToRelTree(query: String, database: Database): Relation = {
    printQuery(query, "relational algebra tree")

    val transformer = RelTransformer.create(database)
    val root = transformer.convertToRelTree(query)
    Relation(root)
  }
}