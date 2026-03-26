{{ config(materialized='view') }}

WITH daily AS (
    SELECT
        *
    FROM {{ ref('vw_price_with_bubbles_daily') }}
),

daily_q AS (
    SELECT
        d.*,
        CAST(date_trunc('quarter', CAST(d.trade_date AS timestamp)) AS date) AS quarter_start_date
    FROM daily d
)

SELECT
    symbol,
    year,
    quarter_no,
    quarter_start_date,

    COUNT(*) AS days_in_quarter,

    min_by(open,  trade_date)  AS open_q,
    max_by(close, trade_date)  AS close_q,
    MAX(high)                  AS high_q,
    MIN(low)                   AS low_q,
    SUM(volume)                AS volume_q,

    TRY(
        max_by(close, trade_date) / NULLIF(min_by(close, trade_date), 0) - 1.0
    ) AS ret_q,

    AVG(ret_1d)        AS avg_ret_1d,
    AVG(ret_21d)       AS avg_ret_21d,
    AVG(vol_21d)       AS avg_vol_21d,
    AVG(vol_63d)       AS avg_vol_63d,

    AVG(price_z_252d)  AS avg_price_z_252d,
    MAX(price_z_252d)  AS max_price_z_252d,
    MIN(drawdown_252d) AS min_drawdown_252d,

    AVG(volume_z_21d)  AS avg_volume_z_21d,
    MAX(volume_z_21d)  AS max_volume_z_21d,

    AVG(bubble_score)  AS avg_bubble_score,
    MAX(bubble_score)  AS max_bubble_score,

    SUM(is_bubble_loose)   AS days_bubble_loose,
    SUM(is_bubble_base)    AS days_bubble_base,
    SUM(is_bubble_strict)  AS days_bubble_strict,
    SUM(is_crash_base)     AS days_crash_base,
    SUM(is_crash_strict)   AS days_crash_strict,

    SUM(is_bubble_base)  / CAST(COUNT(*) AS double) AS share_bubble_base,
    SUM(is_crash_base)   / CAST(COUNT(*) AS double) AS share_crash_base,

    asset_type,
    category,
    risk_group,
    liquidity_class,
    systemic_role,
    narrative_role,
    data_priority

FROM daily_q
GROUP BY
    symbol,
    year,
    quarter_no,
    quarter_start_date,
    asset_type,
    category,
    risk_group,
    liquidity_class,
    systemic_role,
    narrative_role,
    data_priority;