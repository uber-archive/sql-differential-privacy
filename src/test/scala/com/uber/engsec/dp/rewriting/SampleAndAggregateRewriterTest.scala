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

package com.uber.engsec.dp.rewriting

import com.uber.engsec.dp.rewriting.mechanism.{ElasticSensitivityRewriter, SampleAndAggregateConfig, SampleAndAggregateRewriter}
import com.uber.engsec.dp.sql.QueryParser
import junit.framework.TestCase

class SampleAndAggregateRewriterTest extends TestCase {

  def checkResult(query: String, epsilon: Double, lambda: Double, expected: String): Unit = {
    val root = QueryParser.parseToRelTree(query)
    val config = SampleAndAggregateConfig(epsilon, lambda)
    val result = (new SampleAndAggregateRewriter).run(root, config)
    TestCase.assertEquals(expected.stripMargin.stripPrefix("\n"), result.toSql)
  }

  def testStatisticalQuery() = {
    val query = """
      SELECT AVG(quantity)
      FROM orders
      WHERE product_id = 1
    """

    checkResult(query, 0.1, 10, """
      |WITH _partitioned_query AS (
      |  SELECT AVG(quantity) + 0 _col, MOD(ROW_NUMBER(), 250) _grp
      |  FROM public.orders
      |  WHERE product_id = 1
      |  GROUP BY MOD(ROW_NUMBER(), 250)
      |), _private_bounds AS (
      |  WITH _prob_table AS (
      |    WITH _y_calc AS (
      |      WITH _clamped_with_idx AS (
      |        SELECT _clamped__col, ROW_NUMBER() idx
      |        FROM (SELECT CASE WHEN _col < 0 THEN 0 ELSE CASE WHEN _col > 10.0 THEN 10.0 ELSE _col END END _clamped__col
      |        FROM (SELECT *
      |        FROM (SELECT _col
      |        FROM _partitioned_query
      |        UNION ALL
      |        SELECT 0
      |        FROM (VALUES  (0)) t (ZERO))
      |        UNION ALL
      |        SELECT 10.0
      |        FROM (VALUES  (0)) t (ZERO)) t5
      |        ORDER BY CASE WHEN _col < 0 THEN 0 ELSE CASE WHEN _col > 10.0 THEN 10.0 ELSE _col END END IS NULL, CASE WHEN _col < 0 THEN 0 ELSE CASE WHEN _col > 10.0 THEN 10.0 ELSE _col END END) t7
      |      )
      |      SELECT _clamped_with_idx._clamped__col, _clamped_with_idx.idx, _clamped_with_idx0._clamped__col _clamped__col0, _clamped_with_idx0.idx idx0, _clamped_with_idx._clamped__col _range_max, _clamped_with_idx0._clamped__col _range_min, (_clamped_with_idx._clamped__col - _clamped_with_idx0._clamped__col) * EXP(-0.025 * ABS(_clamped_with_idx.idx - 62.75)) _y_14, (_clamped_with_idx._clamped__col - _clamped_with_idx0._clamped__col) * EXP(-0.025 * ABS(_clamped_with_idx.idx - 188.25)) _y_34
      |      FROM _clamped_with_idx
      |      INNER JOIN _clamped_with_idx _clamped_with_idx0 ON _clamped_with_idx.idx = _clamped_with_idx0.idx + 1
      |    )
      |    SELECT t.y_14_normalizing, t.y_34_normalizing, _y_calc0._clamped__col, _y_calc0.idx, _y_calc0._clamped__col0, _y_calc0.idx0, _y_calc0._range_max, _y_calc0._range_min, _y_calc0._y_14, _y_calc0._y_34, _y_calc0._y_14 / t.y_14_normalizing _y_14_normalized_prob, _y_calc0._y_34 / t.y_34_normalizing _y_34_normalized_prob
      |    FROM (SELECT SUM(_y_14) y_14_normalizing, SUM(_y_34) y_34_normalizing
      |    FROM _y_calc) t,
      |    _y_calc _y_calc0
      |  ), _probs__y_14_normalized_prob AS (
      |    SELECT *
      |    FROM _prob_table
      |    WHERE _y_14_normalized_prob > 0
      |  ), _probs__y_34_normalized_prob AS (
      |    SELECT *
      |    FROM _prob_table
      |    WHERE _y_34_normalized_prob > 0
      |  )
      |  SELECT (t6._draw_result_14 + t14._draw_result_34) / 2 + 4 * 10.961212887232518 * ABS(t14._draw_result_34 - t6._draw_result_14) u, (t6._draw_result_14 + t14._draw_result_34) / 2 - 4 * 10.961212887232518 * ABS(t14._draw_result_34 - t6._draw_result_14) l
      |  FROM (SELECT RAND() * (_prob_table._range_max - _prob_table._range_min) + _prob_table._range_min _draw_result_14
      |  FROM (SELECT t4.idx
      |  FROM (SELECT *
      |  FROM (SELECT _probs__y_14_normalized_prob.idx, SUM(_probs__y_14_normalized_prob0._y_14_normalized_prob) _cdf
      |  FROM _probs__y_14_normalized_prob
      |  INNER JOIN _probs__y_14_normalized_prob _probs__y_14_normalized_prob0 ON _probs__y_14_normalized_prob.idx <= _probs__y_14_normalized_prob0.idx
      |  GROUP BY _probs__y_14_normalized_prob.idx) t,
      |  (SELECT RAND() _uniform
      |  FROM (VALUES  (0)) t (ZERO)) t1
      |  WHERE t._cdf >= t1._uniform
      |  ORDER BY t._cdf IS NULL, t._cdf
      |  LIMIT 1) t4) t5
      |  INNER JOIN _prob_table ON t5.idx = _prob_table.idx) t6,
      |  (SELECT RAND() * (_prob_table0._range_max - _prob_table0._range_min) + _prob_table0._range_min _draw_result_34
      |  FROM (SELECT t12.idx
      |  FROM (SELECT *
      |  FROM (SELECT _probs__y_34_normalized_prob.idx, SUM(_probs__y_34_normalized_prob0._y_34_normalized_prob) _cdf
      |  FROM _probs__y_34_normalized_prob
      |  INNER JOIN _probs__y_34_normalized_prob _probs__y_34_normalized_prob0 ON _probs__y_34_normalized_prob.idx <= _probs__y_34_normalized_prob0.idx
      |  GROUP BY _probs__y_34_normalized_prob.idx) t7,
      |  (SELECT RAND() _uniform
      |  FROM (VALUES  (0)) t (ZERO)) t9
      |  WHERE t7._cdf >= t9._uniform
      |  ORDER BY t7._cdf IS NULL, t7._cdf
      |  LIMIT 1) t12) t13
      |  INNER JOIN _prob_table _prob_table0 ON t13.idx = _prob_table0.idx) t14
      |)
      |SELECT (t1._u_private + ABS(t3.u - t3.l) / 50.2 * (CASE WHEN RAND() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(RAND() - 0.5)))) * 1 _col_private
      |FROM (SELECT SUM(CASE WHEN _partitioned_query._col < _private_bounds.l THEN _private_bounds.l ELSE CASE WHEN _partitioned_query._col > _private_bounds.u THEN _private_bounds.u ELSE _partitioned_query._col END END) _private_sum, SUM(CASE WHEN _partitioned_query._col < _private_bounds.l THEN _private_bounds.l ELSE CASE WHEN _partitioned_query._col > _private_bounds.u THEN _private_bounds.u ELSE _partitioned_query._col END END) / 251 _u_private
      |FROM _partitioned_query,
      |_private_bounds) t1,
      |(SELECT l, u
      |FROM _private_bounds
      |LIMIT 1) t3""".stripMargin)
  }
}