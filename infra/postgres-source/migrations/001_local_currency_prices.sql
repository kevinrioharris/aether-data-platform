-- One-time fix: order_items.unit_price was copied from products.unit_price (USD list price)
-- regardless of the order's currency. Store it in the order's currency instead, as a real shop would.
-- Applied to a running DB, this flows through CDC as ordinary UPDATE events.
UPDATE order_items oi
SET unit_price = round(oi.unit_price * r.rate, 2)
FROM orders o
JOIN (VALUES ('EUR', 0.86), ('GBP', 0.74), ('IDR', 17700), ('SGD', 1.27)) AS r(currency, rate)
  ON r.currency = o.currency
WHERE oi.order_id = o.order_id;
