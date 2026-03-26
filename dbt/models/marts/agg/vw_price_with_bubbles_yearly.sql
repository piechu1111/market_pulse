{{ config(materialized='view') }}

WITH
  daily AS (
   SELECT
     symbol
   , trade_date
   , year
   , close
   , volume
   , ret_1d
   , ret_21d
   , vol_21d
   , price_z_252d
   , drawdown_252d
   , bubble_score
   , is_bubble_loose
   , is_bubble_base
   , is_bubble_strict
   , is_crash_base
   , is_crash_strict
   , price_vs_ma_252d
   , ret_21d_vs_spy
   , asset_type
   , category
   , risk_group
   , liquidity_class
   , systemic_role
   , narrative_role
   , data_priority
   FROM {{ ref('vw_price_with_bubbles_daily') }}
) 
, ranked AS (
   SELECT
     d.*
   , first_value(close) OVER (PARTITION BY d.symbol, d.year ORDER BY d.trade_date ASC) first_close_year
   , last_value(close) OVER (PARTITION BY d.symbol, d.year ORDER BY d.trade_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_close_year
   FROM
     daily d
) 
, agg AS (
   SELECT
     symbol
   , year
   , MIN(trade_date) first_trade_date
   , MAX(trade_date) last_trade_date
   , MAX(first_close_year) year_open
   , MAX(last_close_year) year_close
   , COUNT(*) days_total
   , SUM(is_bubble_loose) bubble_days_loose
   , SUM(is_bubble_base) bubble_days_base
   , SUM(is_bubble_strict) bubble_days_strict
   , SUM(is_crash_base) crash_days_base
   , SUM(is_crash_strict) crash_days_strict
   , SUM((CASE WHEN ((is_bubble_base = 0) AND (is_crash_base = 0)) THEN 1 ELSE 0 END)) normal_days_base
   , ((SUM((CASE WHEN (is_bubble_base = 1) THEN 1 ELSE 0 END)) * 1E0) / COUNT(*)) share_days_bubble_base
   , ((SUM((CASE WHEN (is_crash_base = 1) THEN 1 ELSE 0 END)) * 1E0) / COUNT(*)) share_days_crash_base
   , ((SUM((CASE WHEN ((is_bubble_base = 0) AND (is_crash_base = 0)) THEN 1 ELSE 0 END)) * 1E0) / COUNT(*)) share_days_normal_base
   , (CASE WHEN (SUM(is_crash_strict) > 0) THEN 'crash' WHEN (SUM(is_bubble_strict) > 0) THEN 'bubble' WHEN ((SUM(is_crash_base) > SUM(is_bubble_base)) AND (SUM(is_crash_base) > 0)) THEN 'crash' WHEN (SUM(is_bubble_base) > 0) THEN 'bubble' ELSE 'normal' END) regime_label_year
   , MAX(bubble_score) max_bubble_score
   , AVG(bubble_score) avg_bubble_score
   , AVG(ret_1d) avg_ret_1d
   , SUM(ret_1d) sum_ret_1d
   , AVG(ret_21d) avg_ret_21d
   , approx_percentile(ret_21d, 1E-1) ret_21d_p10
   , approx_percentile(ret_21d, 9E-1) ret_21d_p90
   , AVG(vol_21d) avg_vol_21d
   , AVG(volume) avg_volume
   , AVG(price_z_252d) avg_price_z_252d
   , approx_percentile(price_z_252d, 9E-1) price_z_252d_p90
   , approx_percentile(price_z_252d, 9.5E-1) price_z_252d_p95
   , approx_percentile(drawdown_252d, 9.5E-1) drawdown_252d_p95
   , AVG(price_vs_ma_252d) avg_price_vs_ma_252d
   , MAX(price_vs_ma_252d) max_price_vs_ma_252d
   , AVG(ret_21d_vs_spy) avg_ret_21d_vs_spy
   , approx_percentile(ret_21d_vs_spy, 9E-1) ret_21d_vs_spy_p90
   , arbitrary(asset_type) asset_type
   , arbitrary(category) category
   , arbitrary(risk_group) risk_group
   , arbitrary(liquidity_class) liquidity_class
   , arbitrary(systemic_role) systemic_role
   , arbitrary(narrative_role) narrative_role
   , arbitrary(data_priority) data_priority
   FROM
     ranked
   GROUP BY symbol, year
) 
SELECT
  symbol
, year
, first_trade_date
, last_trade_date
, year_open
, year_close
, (CASE WHEN ((year_open IS NOT NULL) AND (year_open <> 0)) THEN ((year_close / year_open) - 1) ELSE null END) ret_1y
, avg_ret_1d
, sum_ret_1d
, avg_ret_21d
, ret_21d_p10
, ret_21d_p90
, avg_vol_21d
, avg_volume
, avg_price_z_252d
, price_z_252d_p90
, price_z_252d_p95
, drawdown_252d_p95
, avg_price_vs_ma_252d
, max_price_vs_ma_252d
, avg_ret_21d_vs_spy
, ret_21d_vs_spy_p90
, days_total
, bubble_days_loose
, bubble_days_base
, bubble_days_strict
, crash_days_base
, crash_days_strict
, normal_days_base
, share_days_bubble_base
, share_days_crash_base
, share_days_normal_base
, max_bubble_score
, avg_bubble_score
, regime_label_year
, asset_type
, category
, risk_group
, liquidity_class
, systemic_role
, narrative_role
, data_priority
FROM
  agg
