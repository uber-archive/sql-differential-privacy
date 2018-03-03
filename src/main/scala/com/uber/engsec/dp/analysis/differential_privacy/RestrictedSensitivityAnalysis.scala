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

package com.uber.engsec.dp.analysis.differential_privacy

import com.uber.engsec.dp.dataflow.column.NodeColumnFacts
import com.uber.engsec.dp.exception.{AnalysisException, UnsupportedQueryException}
import com.uber.engsec.dp.sql.relational_algebra._
import org.apache.calcite.rel.core.Join

/** Restricted sensitivity analysis. Calculates the global sensitivity of a query over a restricted class of datasets
  * defined by properties of the data model (in particular the max frequency of join keys), which is presumed known by
  * the querier.
  *
  * @see [[https://arxiv.org/abs/1208.4586 Differentially Private Data Analysis of Social Networks via Restricted Sensitivity]]
  */
class RestrictedSensitivityAnalysis extends ElasticSensitivityAnalysis {

  override def transferJoin(node: Join, state: NodeColumnFacts[RelStability,ColSensitivity]): NodeColumnFacts[RelStability,ColSensitivity] = {
    /** Update the stability at every join, per restricted sensitivity definition.
      */
    val equijoinColumns = RelUtils.extractEquiJoinColumns(node, node.getCondition)
    if (equijoinColumns.isEmpty)
      throw new UnsupportedQueryException(s"This analysis only works on single-clause equijoins.")

    val (leftColumnIndex, rightColumnIndex) = equijoinColumns.get

    val leftState = resultMap(Relation(node.getLeft))
    val rightState = resultMap(Relation(node.getRight))

    val leftStability = leftState.nodeFact.stability
    val rightStability = rightState.nodeFact.stability

    // Determine if this is a self-join: get the intersection of ancestors for the left and right relations
    // If the intersection is not empty, then this is a self-join (and restricted sensitivity doesn't support it)
    val isSelfJoin = (leftState.nodeFact.ancestors intersect rightState.nodeFact.ancestors).nonEmpty
    if (isSelfJoin)
      throw new UnsupportedQueryException("This analysis does not support self joins")

    // Determine the stability of the join
    val leftColFact = leftState.colFacts(leftColumnIndex)
    val rightColFact = rightState.colFacts(rightColumnIndex)

    val maxFreqLeftJoinColumn = leftColFact.maxFreq
    val maxFreqRightJoinColumn = rightColFact.maxFreq

    val newStability =
      (maxFreqLeftJoinColumn, maxFreqRightJoinColumn) match {
        case (l, r) if l <= 1.0 => r * leftStability
        case (l, r) if r <= 1.0 => l * rightStability
        case _ => throw new UnsupportedQueryException("This analysis does not support many-to-many joins")
      }

    val newNodeState = state.nodeFact.copy(
      stability = newStability
    )

    /** Update the max frequency for every column by a factor of the max frequency of the join key in the opposing
      * relation. This models the worst-case situation where each record containing the most-frequent-key is duplicated
      * this many times by the join.
      */
    val newColState =
      leftState.colFacts.map { x => x.copy(maxFreq = x.maxFreq * maxFreqRightJoinColumn) } ++
        rightState.colFacts.map { x => x.copy(maxFreq = x.maxFreq * maxFreqLeftJoinColumn) }

    NodeColumnFacts(newNodeState, newColState)
  }

  override def setK(k: Int): Unit = throw new AnalysisException("This analysis does not use K")
}