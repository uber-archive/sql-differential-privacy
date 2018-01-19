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
      |  SELECT AVG(quantity) + 0 _col, MOD(ROW_NUMBER(), 251) _grp
      |  FROM public.orders
      |  WHERE product_id = 1
      |  GROUP BY MOD(ROW_NUMBER(), 251)
      |), _priv_bounds AS (
      |  WITH _prob_tbl AS (
      |    WITH _y_calc AS (
      |      WITH _clamped_with_idx AS (
      |        SELECT _clamped__col, ROW_NUMBER() _idx
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
      |      SELECT _clamped_with_idx0._idx - 1 _i, _clamped_with_idx0._clamped__col _z_i, _clamped_with_idx._clamped__col _z_i_next, (_clamped_with_idx._clamped__col - _clamped_with_idx0._clamped__col) * EXP(-0.025 * ABS(_clamped_with_idx0._idx - 1 - 62.75)) _y_i_1qrt, (_clamped_with_idx._clamped__col - _clamped_with_idx0._clamped__col) * EXP(-0.025 * ABS(_clamped_with_idx0._idx - 1 - 188.25)) _y_i_3qrt
      |      FROM _clamped_with_idx
      |      INNER JOIN _clamped_with_idx _clamped_with_idx0 ON _clamped_with_idx._idx = _clamped_with_idx0._idx + 1
      |    )
      |    SELECT t._y_1qrt_sum, t._y_3qrt_sum, _y_calc0._i, _y_calc0._z_i, _y_calc0._z_i_next, _y_calc0._y_i_1qrt, _y_calc0._y_i_3qrt, _y_calc0._y_i_1qrt / t._y_1qrt_sum _y_i_1qrt_normalized, _y_calc0._y_i_3qrt / t._y_3qrt_sum _y_i_3qrt_normalized
      |    FROM (SELECT SUM(_y_i_1qrt) _y_1qrt_sum, SUM(_y_i_3qrt) _y_3qrt_sum
      |    FROM _y_calc) t,
      |    _y_calc _y_calc0
      |  ), _probs__y_i_1qrt_normalized AS (
      |    SELECT *
      |    FROM _prob_tbl
      |    WHERE _y_i_1qrt_normalized > 0
      |  ), _probs__y_i_3qrt_normalized AS (
      |    SELECT *
      |    FROM _prob_tbl
      |    WHERE _y_i_3qrt_normalized > 0
      |  )
      |  SELECT (t6._draw_result_1qrt + t14._draw_result_3qrt) / 2 + 43.84485154893007 * ABS(t14._draw_result_3qrt - t6._draw_result_1qrt) u, (t6._draw_result_1qrt + t14._draw_result_3qrt) / 2 - 43.84485154893007 * ABS(t14._draw_result_3qrt - t6._draw_result_1qrt) l
      |  FROM (SELECT RAND() * (_prob_tbl._z_i_next - _prob_tbl._z_i) + _prob_tbl._z_i _draw_result_1qrt
      |  FROM (SELECT t4._i
      |  FROM (SELECT *
      |  FROM (SELECT _probs__y_i_1qrt_normalized._i, SUM(_probs__y_i_1qrt_normalized0._y_i_1qrt_normalized) _cdf
      |  FROM _probs__y_i_1qrt_normalized
      |  INNER JOIN _probs__y_i_1qrt_normalized _probs__y_i_1qrt_normalized0 ON _probs__y_i_1qrt_normalized._i <= _probs__y_i_1qrt_normalized0._i
      |  GROUP BY _probs__y_i_1qrt_normalized._i) t,
      |  (SELECT RAND() _uniform
      |  FROM (VALUES  (0)) t (ZERO)) t1
      |  WHERE t._cdf >= t1._uniform
      |  ORDER BY t._cdf IS NULL, t._cdf
      |  LIMIT 1) t4) t5
      |  INNER JOIN _prob_tbl ON t5._i = _prob_tbl._i) t6,
      |  (SELECT RAND() * (_prob_tbl0._z_i_next - _prob_tbl0._z_i) + _prob_tbl0._z_i _draw_result_3qrt
      |  FROM (SELECT t12._i
      |  FROM (SELECT *
      |  FROM (SELECT _probs__y_i_3qrt_normalized._i, SUM(_probs__y_i_3qrt_normalized0._y_i_3qrt_normalized) _cdf
      |  FROM _probs__y_i_3qrt_normalized
      |  INNER JOIN _probs__y_i_3qrt_normalized _probs__y_i_3qrt_normalized0 ON _probs__y_i_3qrt_normalized._i <= _probs__y_i_3qrt_normalized0._i
      |  GROUP BY _probs__y_i_3qrt_normalized._i) t7,
      |  (SELECT RAND() _uniform
      |  FROM (VALUES  (0)) t (ZERO)) t9
      |  WHERE t7._cdf >= t9._uniform
      |  ORDER BY t7._cdf IS NULL, t7._cdf
      |  LIMIT 1) t12) t13
      |  INNER JOIN _prob_tbl _prob_tbl0 ON t13._i = _prob_tbl0._i) t14
      |)
      |SELECT (t1._winsorized_mean + ABS(t3.u - t3.l) / 50.2 * (CASE WHEN RAND() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(RAND() - 0.5)))) * 1 _col_private
      |FROM (SELECT SUM(CASE WHEN _partitioned_query._col < _priv_bounds.l THEN _priv_bounds.l ELSE CASE WHEN _partitioned_query._col > _priv_bounds.u THEN _priv_bounds.u ELSE _partitioned_query._col END END) _private_sum, SUM(CASE WHEN _partitioned_query._col < _priv_bounds.l THEN _priv_bounds.l ELSE CASE WHEN _partitioned_query._col > _priv_bounds.u THEN _priv_bounds.u ELSE _partitioned_query._col END END) / 251 _winsorized_mean
      |FROM _partitioned_query,
      |_priv_bounds) t1,
      |(SELECT l, u
      |FROM _priv_bounds
      |LIMIT 1) t3""".stripMargin)
  }
}