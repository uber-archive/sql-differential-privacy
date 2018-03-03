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

import com.uber.engsec.dp.exception.DPException
import com.uber.engsec.dp.schema.Database
import com.uber.engsec.dp.util.IdentityHashMap

import scala.collection.mutable

/** Abstract class for all analyses on parsed SQL queries.
  *
  * @tparam N The node type for the tree (AST, dataflow graph, or relational algebra)
  * @tparam T The return type of the analysis. For column fact analysis, [T] derives from ColumnFacts[_]. For visitor
  *           analyses, T is any object reference type. For abstract interpretation-based dataflow analyses, [T]
  *           derives from AbstractDomain.
  */
abstract class AbstractAnalysis[N <: AnyRef, T] extends TreeFunctions[N] {

  /** Allows code to symbolically reference return type of an analysis (e.g., HistogramAnalysis#ResultType) */
  type ResultType = T

  /******************************************************************************************************************
   * Public methods for analysis callers.
   *******************************************************************************************************************/

  /** Runs the analysis on the given query and returns the abstract results at tree root.
    */
  final def analyzeQuery(query: String, database: Database): T = {
    try {
      val treeRoot = parseQueryToTree(query, database)
      run(treeRoot, database)
    }
    catch {
      case e: Exception =>
        // Catch all exceptions that may occur during query parsing and analysis, and wrap in DPException type.
        throw new DPException("Error during query analysis", e)
    }
  }

  /** Runs the analysis on the given parsed representation of the query.
    */
  final def analyzeQuery(root: N, database: Database): T = {
    try {
      run(root, database)
    }
    catch {
      case e: Exception =>
        // Catch all exceptions that may occur during query parsing and analysis, and wrap in DPException type.
        throw new DPException("Error during query analysis", e)
    }
  }

  /** Runs the analysis on the tree and returns the abstract result at the tree root. Subclases may override this
    * method to pre-process the query before analysis begins, but must call super.run().
    */
  def run(root: N, database: Database): T = {
    try {
      treeRoot = Some(root)
      currentDb = Some(database)
      resultMap.clear()
      this.process(root)
      currentNode = None
    }
    finally { // Print the tree even if analysis throws an exception
      if (AbstractAnalysis.DEBUG) {
        System.out.println("\n********** " + this.getClass.getSimpleName + " **********")
        printTree(treeRoot.get)
      }
    }
    resultMap(root)
  }

  /** Returns the current database being queried. */
  def getDatabase: Database = currentDb.get

  /** Runs the analysis on the tree and returns a map from nodes in the tree to analysis state at that node. */
  def runAll(root: N, database: Database): mutable.HashMap[N, T] = {
    run(root, database)
    resultMap
  }

  /******************************************************************************************************************
   * Analysis engine internals.
   ******************************************************************************************************************/

  /** Map from each node in the tree to analysis results at that node. May be inspected by analysis implementations in
    * their transfer/join functions; results are guaranteed to exist for all nodes *below* the current node in the tree.
    */
  val resultMap: mutable.HashMap[N, T] = new IdentityHashMap[N, T]()

  /** The root node of the tree under analysis. */
  final var treeRoot: Option[N] = None

  /** The database being queried. */
  final var currentDb: Option[Database] = None

  /** The current node being processed. Subclasses should update this variable as tree is traversed to enable helpful
    * debugging when analysis throws an exception.
    */
  var currentNode: Option[N] = None

  /** Analysis entry point. Runs analysis and stores results in resultMap. */
  def process(root: N): Unit
}

object AbstractAnalysis {
  // Set this argument to print all analysis trees along with result state.
  val DEBUG: Boolean = System.getProperty("query.debug", "false").toBoolean
}
