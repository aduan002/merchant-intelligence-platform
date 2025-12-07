CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_customers AS SELECT * FROM silver.customers;

CREATE TABLE IF NOT EXISTS gold.dim_sellers AS SELECT * FROM silver.sellers;

CREATE TABLE gold.dim_products AS
SELECT
    p.*,
    t.product_category_name_english AS product_category_name_en
FROM silver.products AS p
LEFT JOIN silver.product_category_name_translation AS t
    ON p.product_category_name = t.product_category_name;

CREATE TABLE IF NOT EXISTS gold.dim_geolocation AS SELECT * FROM silver.geolocation;

CREATE TABLE IF NOT EXISTS gold.dim_date(
    date_key            INTEGER PRIMARY KEY,
    full_date           DATE NOT NULL,
    day_of_week         INTEGER,  
    day_name            VARCHAR(10),
    day_of_month        INTEGER,
    day_of_year         INTEGER,
    week_of_year        INTEGER,
    month               INTEGER,                
    month_name          VARCHAR(10),
    quarter             INTEGER,               
    year                INTEGER,
    is_weekend          BOOLEAN
);

DELETE FROM gold.dim_date;

INSERT INTO gold.dim_date
SELECT
    TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'), '99999999') AS date_key,
    d::DATE                                       AS full_date,
    EXTRACT(ISODOW FROM d)::INT                   AS day_of_week,
    TO_CHAR(d, 'Day')                             AS day_name,
    EXTRACT(DAY FROM d)::INT                      AS day_of_month,
    EXTRACT(DOY FROM d)::INT                      AS day_of_year,

    EXTRACT(WEEK FROM d)::INT                     AS week_of_year,
    EXTRACT(MONTH FROM d)::INT                    AS month,
    TO_CHAR(d, 'Month')                          AS month_name,
    EXTRACT(QUARTER FROM d)::INT                  AS quarter,
    EXTRACT(YEAR FROM d)::INT                     AS year,

    CASE
        WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE
        ELSE FALSE
    END                                           AS is_weekend
FROM generate_series(
        '2016-01-01'::DATE,
        '2020-12-31'::DATE,
        INTERVAL '1 day'
) AS d;


CREATE TABLE IF NOT EXISTS gold.fact_order_reviews AS 
SELECT
    review_id, order_id, review_score, review_comment_title, review_comment_message, 
    dc.date_key AS review_creation_date_key,
    da.date_key AS review_answer_date_key
FROM silver.order_reviews 
JOIN gold.dim_date AS dc 
ON dc.full_date = CAST(review_creation_date AS DATE)
JOIN gold.dim_date AS da
ON da.full_date = CAST(review_answer_timestamp AS DATE)



CREATE TABLE IF NOT EXISTS gold.fact_order_items AS
SELECT
    o.order_id,
    oi.order_item_id,
    o.customer_id,
    oi.product_id,
    oi.seller_id,
	
    dp.date_key AS order_purchase_date_key,
    ds.date_key AS shipping_limit_date_key,

    oi.price,
    oi.freight_value
FROM silver.orders AS o
JOIN silver.order_items AS oi
  ON o.order_id = oi.order_id
JOIN gold.dim_date AS dp
  ON dp.full_date = CAST(o.order_purchase_timestamp AS DATE)
JOIN gold.dim_date AS ds
  ON ds.full_date = CAST(oi.shipping_limit_date AS DATE);



CREATE TABLE IF NOT EXISTS gold.fact_order_payments AS
SELECT
	o.order_id, o.customer_id, da.date_key AS order_approved_at,
	op.payment_sequential, op.payment_type, op.payment_installments, op.payment_value
FROM silver.orders AS o JOIN silver.order_payments AS op ON o.order_id = op.order_id
JOIN gold.dim_date AS da ON da.full_date = CAST(o.order_approved_at AS DATE);




CREATE TABLE IF NOT EXISTS gold.fact_orders AS
WITH order_items_agg AS (
    SELECT
        oi.order_id,
        COUNT(oi.order_item_id)               AS items_count,
        SUM(oi.price)                         AS items_gross_revenue,
        SUM(oi.freight_value)                 AS freight_total
    FROM silver.order_items AS oi
    GROUP BY oi.order_id
),
order_payments_agg AS (
    SELECT
        op.order_id,
        SUM(op.payment_value)                               AS payments_total_value,
        MAX(op.payment_installments)                        AS payments_installments_max,
        CASE
            WHEN SUM(CASE WHEN op.payment_type = 'credit_card' THEN 1 ELSE 0 END) > 0
                THEN 1
            ELSE 0
        END                                                 AS credit_card_used_flag
    FROM silver.order_payments AS op
    GROUP BY op.order_id
)
SELECT
    ROW_NUMBER() OVER ()                            AS order_key,

    o.order_id,
    o.customer_id,

    dpurchase.date_key  AS purchase_date_key,
    dapproved.date_key  AS approved_date_key,
    dcarrier.date_key   AS delivered_carrier_date_key,
    dcustomer.date_key  AS delivered_customer_date_key,
    dest.date_key       AS estimated_delivery_date_key,

    o.order_status,

    COALESCE(oi_agg.items_count, 0)                AS items_count,
    COALESCE(oi_agg.items_gross_revenue, 0)        AS items_gross_revenue,
    COALESCE(oi_agg.freight_total, 0)              AS freight_total,

    COALESCE(op_agg.payments_total_value, 0)       AS payments_total_value,
    op_agg.payments_installments_max,
    COALESCE(op_agg.credit_card_used_flag, 0)      AS credit_card_used_flag

FROM silver.orders AS o
LEFT JOIN order_items_agg    AS oi_agg
       ON oi_agg.order_id = o.order_id
LEFT JOIN order_payments_agg AS op_agg
       ON op_agg.order_id = o.order_id

LEFT JOIN gold.dim_date AS dpurchase
       ON dpurchase.full_date = CAST(o.order_purchase_timestamp AS DATE)
LEFT JOIN gold.dim_date AS dapproved
       ON dapproved.full_date = CAST(o.order_approved_at AS DATE)
LEFT JOIN gold.dim_date AS dcarrier
       ON dcarrier.full_date = CAST(o.order_delivered_carrier_date AS DATE)
LEFT JOIN gold.dim_date AS dcustomer
       ON dcustomer.full_date = CAST(o.order_delivered_customer_date AS DATE)
LEFT JOIN gold.dim_date AS dest
       ON dest.full_date = CAST(o.order_estimated_delivery_date AS DATE);





CREATE TABLE IF NOT EXISTS gold.fact_daily_marketplace_kpis AS
WITH daily_orders AS (
    SELECT
        fo.purchase_date_key AS date_key,

        COUNT(DISTINCT fo.order_id) AS orders_count,
        SUM(fo.items_count) AS items_sold_count,
        SUM(fo.items_gross_revenue) AS gross_revenue,
        SUM(fo.freight_total)  AS freight_revenue,
        COUNT(DISTINCT fo.customer_id) AS unique_customers_count,

        SUM(CASE WHEN fo.order_status = 'delivered' THEN 1 ELSE 0 END)
            AS delivered_orders_count,
        SUM(CASE WHEN fo.order_status = 'canceled' THEN 1 ELSE 0 END)
            AS canceled_orders_count,

        SUM(fo.credit_card_used_flag) AS credit_card_orders_count,

    
        SUM(
            CASE
                WHEN fo.order_status = 'delivered'
                 AND fo.delivered_customer_date_key IS NOT NULL
                 AND fo.estimated_delivery_date_key IS NOT NULL
                 AND fo.delivered_customer_date_key <= fo.estimated_delivery_date_key
                THEN 1 ELSE 0
            END
        ) AS delivered_on_time_count
    FROM gold.fact_orders AS fo
    GROUP BY fo.purchase_date_key
)
SELECT
    d.date_key,
    d.full_date,

    COALESCE(o.orders_count, 0) AS orders_count,
    COALESCE(o.items_sold_count, 0) AS items_sold_count,
    COALESCE(o.gross_revenue, 0) AS gross_revenue,
    COALESCE(o.freight_revenue, 0) AS freight_revenue,
    COALESCE(o.unique_customers_count, 0) AS unique_customers_count,
    COALESCE(o.delivered_orders_count, 0) AS delivered_orders_count,
    COALESCE(o.canceled_orders_count, 0) AS canceled_orders_count,
    COALESCE(o.credit_card_orders_count, 0) AS credit_card_orders_count,
    COALESCE(o.delivered_on_time_count, 0) AS delivered_on_time_count,

    CASE
        WHEN o.orders_count > 0
        THEN o.gross_revenue::NUMERIC / o.orders_count
        ELSE NULL
    END AS avg_order_value,

    CASE
        WHEN o.delivered_orders_count > 0
        THEN o.delivered_on_time_count::NUMERIC / o.delivered_orders_count
        ELSE NULL
    END AS on_time_delivery_rate

FROM gold.dim_date AS d
LEFT JOIN daily_orders AS o ON d.date_key = o.date_key;

	