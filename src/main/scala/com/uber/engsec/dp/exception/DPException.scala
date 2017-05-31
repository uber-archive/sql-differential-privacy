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

/** The exception type is raised for any exceptional condition encountered during end-to-end analysis of a query (i.e.,
  * by calls to AbstractAnalysis.analyzeQuery()). This includes parsing exceptions, tree transformation exceptions, and
  * analysis runtime errors.
  *
  * This is a checked exception, requiring callers to explicitly handle errors. Internal code may throw any of
  * the unchecked error types defined in [[com.uber.engsec.dp.exception]]. All public interfaces to this tool should
  * catch internal errors and wrap with this exception type. Callers can use the getCause() method to retrieve details
  * about the underlying exception.
  */
class DPException(val message: String, val cause: Throwable) extends Exception(message, cause)