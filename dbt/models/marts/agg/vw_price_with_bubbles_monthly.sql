{{ config(materialized='view') }}

WITH daily AS (
    SELECT
        symbol,
        trade_date,
        year,
        month_no,
        month_start_date,

        -- price and volume
        close,
        volume,

        -- returns and volatility measures
        ret_1d,
        ret_5d,
        ret_21d,
        ret_63d,
        vol_21d,
        vol_63d,

        -- price position / risk
        price_z_21d,
        price_z_63d,
        price_z_252d,
        price_vs_ma_252d,
        drawdown_252d,
        rolling_max_252d,
        rolling_min_252d,

        -- volume – level and normalization
        volume_ma_21d,
        volume_z_21d,

        -- momentum / relative performance
        rsi_14d,
        ret_21d_vs_spy,

        -- bubble / crash signals
        bubble_score,
        is_bubble_loose,
        is_bubble_base,
        is_bubble_strict,
        is_crash_base,
        is_crash_strict,

        -- measures
        asset_type,
        category,
        risk_group,
        liquidity_class,
        systemic_role,
        narrative_role,
        data_priority
    FROM {{ ref('vw_price_with_bubbles_daily') }}
),

ranked AS (
    SELECT
        d.*,
        first_value(close) OVER (
            PARTITION BY d.symbol, d.month_start_date
            ORDER BY d.trade_date
        ) AS first_close_month,

        last_value(close) OVER (
            PARTITION BY d.symbol, d.month_start_date
            ORDER BY d.trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_close_month
    FROM daily d
),

agg AS (
    SELECT
        symbol,
        year,
        month_no,
        month_start_date,
        MIN(trade_date) AS first_trade_date,
        MAX(trade_date) AS last_trade_date,
        MAX(first_close_month) AS month_open,
        MAX(last_close_month)  AS month_close,

        -- number of sessions and regimes in month
        COUNT(*) AS days_total,

        SUM(is_bubble_loose)  AS bubble_days_loose,
        SUM(is_bubble_base)   AS bubble_days_base,
        SUM(is_bubble_strict) AS bubble_days_strict,

        SUM(is_crash_base)    AS crash_days_base,
        SUM(is_crash_strict)  AS crash_days_strict,

        -- "normal" by base definition
        SUM(CASE WHEN is_bubble_base = 0 AND is_crash_base = 0 THEN 1 ELSE 0 END) AS normal_days_base,

        -- share days in month
        SUM(CASE WHEN is_bubble_base = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS share_days_bubble_base,
        SUM(CASE WHEN is_crash_base  = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS share_days_crash_base,
        SUM(CASE WHEN is_bubble_base = 0 AND is_crash_base = 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS share_days_normal_base,

        -- dominating month regime
        CASE
            WHEN SUM(is_crash_strict)  > 0 THEN 'crash'
            WHEN SUM(is_bubble_strict) > 0 THEN 'bubble'
            WHEN SUM(is_crash_base)    > SUM(is_bubble_base) AND SUM(is_crash_base) > 0 THEN 'crash'
            WHEN SUM(is_bubble_base)   > 0 THEN 'bubble'
            ELSE 'normal'
        END AS regime_label_month,

        -- bubble score
        MAX(bubble_score) AS max_bubble_score,
        AVG(bubble_score) AS avg_bubble_score,

  
        -- returns
        -- 1d: monthly "quality"
        AVG(ret_1d) AS avg_ret_1d,
        SUM(ret_1d) AS sum_ret_1d,   -- suma dziennych jako przybliżenie 1M

        -- 5d: common weeks in month
        AVG(ret_5d) AS avg_ret_5d,
        approx_percentile(ret_5d, 0.1) AS ret_5d_p10,
        approx_percentile(ret_5d, 0.9) AS ret_5d_p90,

        -- 21d: classic ~monthly horizon
        AVG(ret_21d) AS avg_ret_21d,
        approx_percentile(ret_21d, 0.1) AS ret_21d_p10,
        approx_percentile(ret_21d, 0.9) AS ret_21d_p90,

        -- 63d: how the month fits into the ~quarter context
        AVG(ret_63d) AS avg_ret_63d,

        -- violatility and volume
        AVG(vol_21d) AS avg_vol_21d,
        AVG(vol_63d) AS avg_vol_63d,

        SUM(volume) AS volume_month_sum,
        AVG(volume) AS avg_volume,

        AVG(volume_ma_21d) AS avg_volume_ma_21d,
        AVG(volume_z_21d)  AS avg_volume_z_21d,

        -- price vs short and long term references
        -- short term z-scores
        AVG(price_z_21d) AS avg_price_z_21d,
        approx_percentile(price_z_21d, 0.9) AS price_z_21d_p90,

        AVG(price_z_63d) AS avg_price_z_63d,
        approx_percentile(price_z_63d, 0.9) AS price_z_63d_p90,

        -- position relative to the annual context
        AVG(price_z_252d) AS avg_price_z_252d,
        approx_percentile(price_z_252d, 0.9)  AS price_z_252d_p90,
        approx_percentile(price_z_252d, 0.95) AS price_z_252d_p95,

        -- drawdown: focus on the most painful point
        MIN(drawdown_252d) AS min_drawdown_252d,
        approx_percentile(drawdown_252d, 0.95) AS drawdown_252d_p95,

        -- trend structure vs 252-day moving average
        AVG(price_vs_ma_252d) AS avg_price_vs_ma_252d,
        MAX(price_vs_ma_252d) AS max_price_vs_ma_252d,

        -- rolling high/low over a one-year window
        MAX(rolling_max_252d) AS max_rolling_max_252d,
        MIN(rolling_min_252d) AS min_rolling_min_252d,

        -- mementum / relative performance
        AVG(rsi_14d) AS avg_rsi_14d,
        approx_percentile(rsi_14d, 0.9) AS rsi_14d_p90,

        AVG(ret_21d_vs_spy) AS avg_ret_21d_vs_spy,
        approx_percentile(ret_21d_vs_spy, 0.9) AS ret_21d_vs_spy_p90,

        -- dimentions (symbol-level, effectively static)
        arbitrary(asset_type)      AS asset_type,
        arbitrary(category)        AS category,
        arbitrary(risk_group)      AS risk_group,
        arbitrary(liquidity_class) AS liquidity_class,
        arbitrary(systemic_role)   AS systemic_role,
        arbitrary(narrative_role)  AS narrative_role,
        arbitrary(data_priority)   AS data_priority
    FROM ranked
    GROUP BY
        symbol,
        year,
        month_no,
        month_start_date
)

SELECT
    symbol,
    year,
    month_no,
    month_start_date,
    first_trade_date,
    last_trade_date,

    month_open,
    month_close,
    CASE
        WHEN month_open IS NOT NULL AND month_open <> 0
        THEN month_close / month_open - 1
        ELSE NULL
    END AS ret_1m,

    -- returns
    avg_ret_1d,
    sum_ret_1d,
    avg_ret_5d,
    ret_5d_p10,
    ret_5d_p90,
    avg_ret_21d,
    ret_21d_p10,
    ret_21d_p90,
    avg_ret_63d,

    -- violatility and volume
    avg_vol_21d,
    avg_vol_63d,
    volume_month_sum,
    avg_volume,
    avg_volume_ma_21d,
    avg_volume_z_21d,

    -- price vs reference levels
    avg_price_z_21d,
    price_z_21d_p90,
    avg_price_z_63d,
    price_z_63d_p90,
    avg_price_z_252d,
    price_z_252d_p90,
    price_z_252d_p95,
    min_drawdown_252d,
    drawdown_252d_p95,
    avg_price_vs_ma_252d,
    max_price_vs_ma_252d,
    max_rolling_max_252d,
    min_rolling_min_252d,

    -- momentum / relative
    avg_rsi_14d,
    rsi_14d_p90,
    avg_ret_21d_vs_spy,
    ret_21d_vs_spy_p90,

    -- regimes
    days_total,
    bubble_days_loose,
    bubble_days_base,
    bubble_days_strict,
    crash_days_base,
    crash_days_strict,
    normal_days_base,

    share_days_bubble_base,
    share_days_crash_base,
    share_days_normal_base,

    max_bubble_score,
    avg_bubble_score,
    regime_label_month,

    -- dimentions
    asset_type,
    category,
    risk_group,
    liquidity_class,
    systemic_role,
    narrative_role,
    data_priority

FROM agg;