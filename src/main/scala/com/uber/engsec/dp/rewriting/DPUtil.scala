package com.uber.engsec.dp.rewriting

import com.uber.engsec.dp.analysis.histogram.AggregationInfo
import com.uber.engsec.dp.dataflow.column.AbstractColumnAnalysis.ColumnFacts
import com.uber.engsec.dp.rewriting.rules.ColumnDefinition._
import com.uber.engsec.dp.rewriting.rules.Expr.{Abs, Case, Ln, Rand, _}
import com.uber.engsec.dp.rewriting.rules.Operations._
import com.uber.engsec.dp.rewriting.rules.{Helpers, ValueExpr}
import com.uber.engsec.dp.schema.Schema
import com.uber.engsec.dp.sql.relational_algebra.Relation
import org.apache.calcite.rel.core._

/** Utilities for differential privacy-based rewriters. */
object DPUtil {
  // Expression to sample a random value from the Laplace distribution.
  val LaplaceSample: ValueExpr = Case((Rand-0.5) < 0, -1.0, 1.0) * Ln(1 - (2 * Abs(Rand-0.5)))
}
