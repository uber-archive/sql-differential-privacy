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

package com.uber.engsec.dp.schema

import com.facebook.presto.sql.tree._

/** A class to model differences between SQL database dialects and vendors that are material to query analysis. This
  * will be updated over time
  */
object DatabaseModel {
  /** Returns the column name assigned implicitly by the database for the given expression, i.e.,
    * the name of the output column in the absence of an explicit alias. Note this is highly
    * database-specific. Logic below is for Vertica.
    */
  def getImplicitColumnName(expr: Expression) = expr match {
    case q: QualifiedNameReference => q.getName.toString
    case d: DereferenceExpression => d.getFieldName
    case f: FunctionCall => f.getName.toString
    case s: StringLiteral => s.getValue
    case _ : ArithmeticBinaryExpression => "?column?"
    case _ : AtTimeZone => "timezone"
    case _ : SearchedCaseExpression => "case"
    case _ : Extract => "date_part"
    case _ : CurrentTime => "?column?"
    case _ : InPredicate => "?column?"
    case _ : CoalesceExpression => "coalesce"
    case _ : LongLiteral => "?column?"
    case _ : ComparisonExpression => "?column?"
    case _ => "?column?"  // unknown/default
  }

  /** Is the given name a built-in function? If so, all QualifiedNameReference nodes with this value will be interpreted
    * as functions rather than column references.
    */
  def isBuiltInFunction(name: String): Boolean = {
    name == "sysdate"
  }

  /** Returns true if the given function's ordinal argument (0-indexed) is known to be a literal value, in which case
    * it should be interpreted as a literal value even if parsed as a QualifiedName reference because it may not
    * be quoted/escaped in the original query.
    */
  def isFunctionArgumentLiteral(functionName: String, argNum: Int): Boolean = {
    // TODO: extend this.
    (argNum == 0) && (functionName == "datediff" || functionName == "timestampadd")
  }

  /** Normalizes the table name to a canonical representation, e.g., by stripping out namespace and/or optional prefixes.
    * This canonical table name should match the name provided in the schema config to ensure that schema information
    * can be retrieved for the table.
    */
  def normalizeTableName(tableName: String) = {
    tableName.replaceAll("^public.", "")  // strip any "public." prefix
  }
}