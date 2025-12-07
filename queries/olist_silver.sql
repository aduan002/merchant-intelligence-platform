CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.customers AS SELECT * FROM public.olist_customers_dataset;
ALTER TABLE silver.customers DROP CONSTRAINT IF EXISTS pk_customers;
ALTER TABLE silver.customers ADD CONSTRAINT pk_customers PRIMARY KEY (customer_id);

CREATE TABLE IF NOT EXISTS silver.geolocation AS SELECT * FROM public.olist_geolocation_dataset;

CREATE TABLE IF NOT EXISTS silver.order_items AS 
SELECT
	order_id, order_item_id, product_id, seller_id, 
	TO_TIMESTAMP(shipping_limit_date, 'YYYY-MM-DD HH24:MI:SS') AS shipping_limit_date,
	price, freight_value
FROM public.olist_order_items_dataset;

CREATE TABLE IF NOT EXISTS silver.order_payments AS SELECT * FROM public.olist_order_payments_dataset;

CREATE TABLE IF NOT EXISTS silver.order_reviews AS 
SELECT
	review_id, order_id, review_score, review_comment_title, review_comment_message,
	TO_TIMESTAMP(review_creation_date, 'YYYY-MM-DD HH24:MI:SS') AS review_creation_date,
	TO_TIMESTAMP(review_answer_timestamp, 'YYYY-MM-DD HH24:MI:SS') AS review_answer_timestamp
FROM public.olist_order_reviews_dataset;

CREATE TABLE IF NOT EXISTS silver.orders AS
SELECT
	order_id, customer_id, order_status, 
	TO_TIMESTAMP(order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS') AS order_purchase_timestamp,
	TO_TIMESTAMP(order_approved_at, 'YYYY-MM-DD HH24:MI:SS') AS order_approved_at,
	TO_TIMESTAMP(order_delivered_carrier_date, 'YYYY-MM-DD HH24:MI:SS') AS order_delivered_carrier_date,
	TO_TIMESTAMP(order_delivered_customer_date, 'YYYY-MM-DD HH24:MI:SS') AS order_delivered_customer_date,
	TO_TIMESTAMP(order_estimated_delivery_date, 'YYYY-MM-DD HH24:MI:SS') AS order_estimated_delivery_date
FROM public.olist_orders_dataset;
ALTER TABLE silver.orders DROP CONSTRAINT IF EXISTS pk_orders;
ALTER TABLE silver.orders ADD CONSTRAINT pk_orders PRIMARY KEY (order_id);

CREATE TABLE IF NOT EXISTS silver.products AS
SELECT
	product_id, product_category_name, 
	CAST(product_name_lenght AS INT) AS product_name_length,
	CAST(product_description_lenght AS INT) AS product_description_length,
	CAST(product_photos_qty AS INT) AS product_photos_qty,
	CAST(product_weight_g AS INT) AS product_weight_g,
	CAST(product_length_cm AS INT) AS product_length_cm,
	CAST(product_height_cm AS INT) AS product_height_cm, 
	CAST(product_width_cm AS INT) AS product_width_cm
FROM public.olist_products_dataset;
ALTER TABLE silver.products DROP CONSTRAINT IF EXISTS pk_products;
ALTER TABLE silver.products ADD CONSTRAINT pk_products PRIMARY KEY (product_id);
	
CREATE TABLE IF NOT EXISTS silver.sellers AS SELECT * FROM public.olist_sellers_dataset;
ALTER TABLE silver.sellers DROP CONSTRAINT IF EXISTS pk_sellers;
ALTER TABLE silver.sellers ADD CONSTRAINT pk_sellers PRIMARY KEY (seller_id);

CREATE TABLE IF NOT EXISTS silver.product_category_name_translation AS 
SELECT * FROM public.product_category_name_translation;





