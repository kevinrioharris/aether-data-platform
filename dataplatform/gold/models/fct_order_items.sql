-- Sales fact. Grain: one row per order line of a live, non-cancelled order.
-- Amounts in the order currency and in USD, converted at the latest ECB rate on or before the order date.
-- test: unique(order_item_id)
-- test: not_null(amount_usd)
-- test: not_null(category)
-- test: assert_empty: SELECT 1 FROM gold.fct_order_items WHERE amount_usd < 0
WITH fx AS (
    SELECT rate_date, currency, rate FROM silver.api_fx_rates WHERE base_currency = 'USD'
    UNION ALL
    SELECT DISTINCT rate_date, 'USD', 1.0 FROM silver.api_fx_rates
)
SELECT
    oi.order_item_id,
    o.order_id,
    o.customer_id,
    c.country                                                     AS customer_country,
    oi.product_id,
    p.sku,
    p.category,
    o.status,
    o.order_ts,
    CAST(o.order_ts AS DATE)                                      AS order_date,
    CAST(date_trunc('month', o.order_ts) AS DATE)                 AS order_month,
    o.currency,
    oi.quantity,
    oi.unit_price                                                 AS unit_price_local,
    oi.quantity * oi.unit_price                                   AS amount_local,
    fx.rate_date                                                  AS fx_rate_date,
    fx.rate                                                       AS fx_rate,
    CAST(round(oi.quantity * oi.unit_price / fx.rate, 2) AS DECIMAL(14, 2)) AS amount_usd
FROM silver.shop_order_items oi
JOIN silver.shop_orders o         ON o.order_id = oi.order_id AND NOT o._is_deleted
JOIN silver.shop_products p       ON p.product_id = oi.product_id
LEFT JOIN silver.shop_customers c ON c.customer_id = o.customer_id AND NOT c._is_deleted
ASOF LEFT JOIN fx                 ON fx.currency = o.currency AND CAST(o.order_ts AS DATE) >= fx.rate_date
WHERE NOT oi._is_deleted
  AND o.status <> 'cancelled'
