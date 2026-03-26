
WITH base AS (
    SELECT
        symbol,
        trade_date,
        year,

        -- features from gold (no OHLCV)
        ret_1d,
        ret_5d,
        ret_21d,
        ret_63d,
        ret_252d,

        vol_21d,
        vol_63d,

        ma_21d,
        ma_63d,
        ma_252d,

        price_z_21d,
        price_z_63d,
        price_z_252d,
        price_vs_ma_252d,

        rolling_max_252d,
        rolling_min_252d,
        drawdown_252d,

        volume_ma_21d,
        volume_z_21d,

        rsi_14d,
        ret_21d_vs_spy,

        -- symbol dimentions
        asset_type,
        category,
        risk_group,
        liquidity_class,
        systemic_role,
        narrative_role,
        data_priority

    FROM {{ source('market_pulse_meta', 'gold_daily_features_dim') }}
),

ohlcv AS (
    SELECT
        symbol,
        trade_date,
        year,
        open,
        high,
        low,
        close,
        volume
    FROM market_pulse_meta.gold_daily_features
),

vol_stats AS (
    SELECT
        symbol,
        approx_percentile(vol_21d, 0.9)  AS vol_21d_p90,
        approx_percentile(vol_21d, 0.95) AS vol_21d_p95
    FROM base
    GROUP BY symbol
),

signals AS (
    SELECT
        b.symbol,
        b.trade_date,
        b.year,

        b.price_z_252d,
        b.volume_z_21d,
        b.price_vs_ma_252d,
        b.drawdown_252d,
        b.vol_21d,

        v.vol_21d_p90,
        v.vol_21d_p95,

        -- BUBBLE: sensitive
        CASE WHEN 
            b.price_z_252d > 2.5 AND
            b.volume_z_21d > 1.5 AND
            b.price_vs_ma_252d > 0.30 AND
            b.drawdown_252d > -0.20
        THEN 1 ELSE 0 END AS is_bubble_loose,

        -- BUBBLE: base
        CASE WHEN
            b.price_z_252d > 3.0 AND
            b.volume_z_21d > 2.0 AND
            b.price_vs_ma_252d > 0.50 AND
            b.drawdown_252d > -0.25
        THEN 1 ELSE 0 END AS is_bubble_base,

        -- BUBBLE: strict
        CASE WHEN
            b.price_z_252d > 3.5 AND
            b.volume_z_21d > 2.5 AND
            b.price_vs_ma_252d > 0.75 AND
            b.drawdown_252d > -0.15
        THEN 1 ELSE 0 END AS is_bubble_strict,

        -- CRASH: base
        CASE WHEN
            b.drawdown_252d < -0.30 AND
            b.vol_21d > v.vol_21d_p90
        THEN 1 ELSE 0 END AS is_crash_base,

        -- CRASH: strict
        CASE WHEN
            b.drawdown_252d < -0.40 AND
            b.vol_21d > v.vol_21d_p95
        THEN 1 ELSE 0 END AS is_crash_strict,

        -- bubble score 0-100
        LEAST(
            100.0,
            GREATEST(
                0.0,
                   20 * b.price_z_252d
                 + 15 * b.volume_z_21d
                 + 10 * b.price_vs_ma_252d
                 -  5 * b.drawdown_252d
            )
        ) AS bubble_score

    FROM base b
    LEFT JOIN vol_stats v
      ON b.symbol = v.symbol
)

---------------------------------------------------------
-- FINAL SELECT — complete dataset for QuickSight + data grain
---------------------------------------------------------
SELECT
    b.symbol,
    b.trade_date,
    b.year,

    -- data grain
    month(b.trade_date)                         AS month_no,
    week(b.trade_date)                          AS week_no,
    quarter(b.trade_date)                       AS quarter_no,
    CAST(date_trunc('month',  CAST(b.trade_date AS timestamp)) AS date) AS month_start_date,
    CAST(date_trunc('week',   CAST(b.trade_date AS timestamp)) AS date) AS week_start_date,
    date_format(CAST(b.trade_date AS timestamp), '%Y-%m')      AS year_month,

    -- OHLCV
    o.open,
    o.high,
    o.low,
    o.close,
    o.volume,

    -- features
    b.ret_1d,
    b.ret_5d,
    b.ret_21d,
    b.ret_63d,
    b.ret_252d,

    b.vol_21d,
    b.vol_63d,

    b.ma_21d,
    b.ma_63d,
    b.ma_252d,

    b.price_z_21d,
    b.price_z_63d,
    b.price_z_252d,
    b.price_vs_ma_252d,

    b.rolling_max_252d,
    b.rolling_min_252d,
    b.drawdown_252d,

    b.volume_ma_21d,
    b.volume_z_21d,

    b.rsi_14d,
    b.ret_21d_vs_spy,

    -- dimentions
    b.asset_type,
    b.category,
    b.risk_group,
    b.liquidity_class,
    b.systemic_role,
    b.narrative_role,
    b.data_priority,

    -- bubble / crash signals
    s.is_bubble_loose,
    s.is_bubble_base,
    s.is_bubble_strict,

    s.is_crash_base,
    s.is_crash_strict,

    s.bubble_score,
    CASE
        WHEN s.is_crash_strict = 1 THEN 'crash'
        WHEN s.is_bubble_strict = 1 THEN 'bubble'
        WHEN s.is_crash_base   = 1 THEN 'crash'
        WHEN s.is_bubble_base  = 1 THEN 'bubble'
        ELSE 'normal'
    END AS regime_label,
    s.vol_21d_p90,
    s.vol_21d_p95

FROM base b
LEFT JOIN ohlcv o
  ON b.symbol     = o.symbol
 AND b.trade_date = o.trade_date
LEFT JOIN signals s
  ON b.symbol     = s.symbol
 AND b.trade_date = s.trade_date;