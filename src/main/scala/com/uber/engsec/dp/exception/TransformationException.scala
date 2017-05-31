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

package com.uber.engsec.dp.exception

/** Thrown when fatal errors are encountered during Presto to dataflow graph transformation.
  */
class TransformationException(val message: String) extends RuntimeException(message) {}

/** Thrown when Presto parsing fails, e.g., because the query has a syntax problem or because it uses a dialect of SQL
  * not supported by Presto's SQL grammar.
  */
class ParsingException(message: String) extends TransformationException(message) {}

/** Thrown when processing a query that uses SELECT * on a relation for which the schema is unknown or incomplete.
  */
class AmbiguousWildcardException(message: String) extends TransformationException(message) {}

/** Thrown when processing a query that references a column in two or more relations such that the reference is
  * ambiguous. For example, if tables A and B both have column "city_id", this query is ambiguous and would
  * produce a runtime error on the database:
  *
  * SELECT blah FROM A JOIN B on column_from_a = city_id
  *
  * Note this query would be legal if either: ambigous "city_id" is qualified with a dereference expression
  * (e.g., "B.city_id") OR all non-deference columns are unambiguous by schema (e.g., "column_from_a" appears only in
  * relation A and "city_id" appears only in relation B).
  */
class AmbiguousColumnReference(message: String) extends TransformationException(message) {}

/** Thrown when tree transformation detects an infinite loop (e.g., because the tree has a cycle), which would otherwise
  * result in a StackOverflowException.
  */
class InfiniteLoopException(message: String) extends TransformationException(message) {}

/** Thrown in exceptional cases when processing joins in the query.
  */
class JoinException(message: String) extends TransformationException(message) {}

/** Thrown during graph transformation when the schema mode is STRICT and the query references a table whose schema is
  * not defined.
  */
class UndefinedSchemaException(message: String) extends TransformationException(message) {}

/** Thrown when transforming a query that references a relation in such a way as we cannot determine which columns are
  * accessed. Possible causes: the query is invalid, schema is invalid or incomplete, or the name resolution analysis
  * has a bug.
  */
class UnknownColumnException(message: String) extends TransformationException(message) {}

/** Thrown when trying to parse a query that is known to be invalid, for example because the list of returned columns is
  * either empty or "error", or when trying to rewrite a query that is not a supported type (e.g., a raw data query in a
  * differential privacy rewriter).
  */
class InvalidQueryException(message: String) extends TransformationException(message) {}

/** Thrown when the tree contains a node type that is not recognized or unsupported.
  */
class UnrecognizedNodeTypeException(message: String) extends TransformationException(message) {}