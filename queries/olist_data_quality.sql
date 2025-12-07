CREATE TABLE IF NOT EXISTS silver.data_quality_violations (
    check_name   VARCHAR,
    run_at       TIMESTAMP,
    violation_count INT
);

DELETE FROM silver.data_quality_violations WHERE run_at::DATE = CURRENT_DATE;

---------------------------------- CLEANUP SANITY CHECKS ----------------------------------
INSERT INTO silver.data_quality_violations
SELECT 
	'empty_customers',
	CURRENT_TIMESTAMP,
	COUNT(*) FROM silver.customers HAVING COUNT(*) = 0;

INSERT INTO silver.data_quality_violations
SELECT 
	'empty_orders',
	CURRENT_TIMESTAMP,
	COUNT(*) FROM silver.orders HAVING COUNT(*) = 0;

INSERT INTO silver.data_quality_violations
SELECT 
	'empty_order_items',
	CURRENT_TIMESTAMP,
	COUNT(*) FROM silver.order_items HAVING COUNT(*) = 0;

INSERT INTO silver.data_quality_violations
SELECT 
	'empty_order_payments',
	CURRENT_TIMESTAMP,
	COUNT(*) FROM silver.order_items HAVING COUNT(*) = 0;

INSERT INTO silver.data_quality_violations
SELECT 
	'empty_products',
	CURRENT_TIMESTAMP,
	COUNT(*) FROM silver.products HAVING COUNT(*) = 0;

INSERT INTO silver.data_quality_violations
SELECT 
	'empty_sellers',
	CURRENT_TIMESTAMP,
	COUNT(*) FROM silver.products HAVING COUNT(*) = 0;
------------------------------------------------------------------------------------------------------

---------------------------------- REFERENTIAL INTEGRITY CHECKS ----------------------------------
INSERT INTO silver.data_quality_violations
SELECT
  'orphaned_orders_customer',
  CURRENT_TIMESTAMP,
  COUNT(*)
FROM silver.orders AS o
LEFT JOIN silver.customers AS c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL HAVING COUNT(*) > 0;


INSERT INTO silver.data_quality_violations
SELECT
  'orphaned_orders_order_payment',
  CURRENT_TIMESTAMP,
  COUNT(*)
FROM silver.orders AS o
LEFT JOIN silver.order_payments AS op ON o.order_id = op.order_id
WHERE op.order_id IS NULL HAVING COUNT(*) > 0;


INSERT INTO silver.data_quality_violations
SELECT
  'orphaned_order_items_order',
  CURRENT_TIMESTAMP,
  COUNT(*)
FROM silver.order_items AS oi
LEFT JOIN silver.orders AS o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL HAVING COUNT(*) > 0;


INSERT INTO silver.data_quality_violations
SELECT
  'orphaned_order_items_product',
  CURRENT_TIMESTAMP,
  COUNT(*)
FROM silver.order_items AS oi
LEFT JOIN silver.products AS p ON oi.product_id = p.product_id
WHERE p.product_id IS NULL HAVING COUNT(*) > 0;


INSERT INTO silver.data_quality_violations
SELECT
  'orphaned_order_items_seller',
  CURRENT_TIMESTAMP,
  COUNT(*)
FROM silver.order_items AS oi
LEFT JOIN silver.sellers AS s ON oi.seller_id = s.seller_id
WHERE s.seller_id IS NULL HAVING COUNT(*) > 0;


INSERT INTO silver.data_quality_violations
SELECT
  'orphaned_order_reviews_order',
  CURRENT_TIMESTAMP,
  COUNT(*)
FROM silver.order_reviews AS r
LEFT JOIN silver.orders AS o ON r.order_id = o.order_id
WHERE o.order_id IS NULL HAVING COUNT(*) > 0;
------------------------------------------------------------------------------------------------------

---------------------------------- TIMESTAMP LOGIC CHECKS ----------------------------------
INSERT INTO silver.data_quality_violations
SELECT
  'orders_delivered_before_purchase',
  CURRENT_TIMESTAMP,
  COUNT(*)
FROM silver.orders
WHERE order_delivered_customer_date IS NOT NULL
  AND order_purchase_timestamp IS NOT NULL
  AND order_delivered_customer_date < order_purchase_timestamp
HAVING COUNT(*) > 0;


INSERT INTO silver.data_quality_violations
SELECT
  'reviews_before_order_purchase',
  CURRENT_TIMESTAMP,
  COUNT(*)
FROM silver.order_reviews AS r
JOIN silver.orders AS o ON r.order_id = o.order_id
WHERE r.review_creation_date::DATE < o.order_purchase_timestamp::DATE --Reviews don't have hour, minute, seconds.
HAVING COUNT(*) > 0;


INSERT INTO silver.data_quality_violations
SELECT
  'orders_in_future',
  CURRENT_TIMESTAMP,
  COUNT(*)
FROM silver.orders
WHERE order_purchase_timestamp > CURRENT_TIMESTAMP
HAVING COUNT(*) > 0;
------------------------------------------------------------------------------------------------------

---------------------------------- VALUE RANGE SANITY CHECKS ----------------------------------
INSERT INTO silver.data_quality_violations
SELECT
  'negative_or_zero_price',
  CURRENT_TIMESTAMP,
  COUNT(*)
FROM silver.order_items
WHERE price <= 0
HAVING COUNT(*) > 0;


INSERT INTO silver.data_quality_violations
SELECT
  'negative_freight_value',
  CURRENT_TIMESTAMP,
  COUNT(*)
FROM silver.order_items
WHERE freight_value < 0
HAVING COUNT(*) > 0;



