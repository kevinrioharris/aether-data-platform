-- Use case 1: sales performance vs. target. Grain: one row per (month, category).
-- USD revenue (from CDC orders + FX API) against Sales Ops targets (Excel). The as-of month gets a
-- run-rate projection. Only months covered by order data are included.
-- test: unique(month_start, category)
-- test: not_null(category)
-- test: assert_empty: SELECT 1 FROM gold.mart_sales_vs_target WHERE target_usd IS NULL
WITH actuals AS (
    SELECT order_month AS month_start, category,
           sum(amount_usd)          AS revenue_usd,
           count(DISTINCT order_id) AS orders,
           sum(quantity)            AS units
    FROM gold.fct_order_items
    WHERE order_date <= getvariable('as_of')
    GROUP BY ALL
),
joined AS (
    SELECT
        coalesce(a.month_start, t.month_start) AS month_start,
        coalesce(a.category, t.category)       AS category,
        coalesce(a.revenue_usd, 0)             AS revenue_usd,
        coalesce(a.orders, 0)                  AS orders,
        coalesce(a.units, 0)                   AS units,
        t.target_usd
    FROM actuals a
    FULL OUTER JOIN silver.files_sales_targets t USING (month_start, category)
),
periods AS (
    SELECT *,
        day(last_day(month_start)) AS days_in_month,
        CASE
            WHEN last_day(month_start) <= getvariable('as_of') THEN 'closed'
            ELSE 'in_progress'
        END AS period_status,
        CASE
            WHEN last_day(month_start) <= getvariable('as_of') THEN day(last_day(month_start))
            ELSE day(getvariable('as_of'))
        END AS days_elapsed
    FROM joined
    WHERE month_start >= (SELECT min(order_month) FROM gold.fct_order_items)
      AND month_start <= getvariable('as_of')
),
projected AS (
    SELECT *,
        CAST(round(revenue_usd * days_in_month / days_elapsed, 2) AS DECIMAL(14, 2)) AS projected_revenue_usd
    FROM periods
)
SELECT
    month_start,
    category,
    period_status,
    days_elapsed,
    days_in_month,
    orders,
    units,
    revenue_usd,
    target_usd,
    round(revenue_usd / target_usd, 4)                        AS attainment_pct,
    projected_revenue_usd,
    round(projected_revenue_usd / target_usd, 4)              AS projected_attainment_pct,
    revenue_usd - target_usd                                  AS gap_usd,
    CASE
        WHEN period_status = 'closed' AND revenue_usd >= target_usd THEN 'hit'
        WHEN period_status = 'closed'                               THEN 'missed'
        WHEN projected_revenue_usd >= target_usd                    THEN 'on_track'
        WHEN projected_revenue_usd >= 0.9 * target_usd              THEN 'at_risk'
        ELSE 'behind'
    END                                                       AS status
FROM projected
ORDER BY month_start, category
