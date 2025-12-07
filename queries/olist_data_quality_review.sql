SELECT *
FROM silver.orders AS o
LEFT JOIN silver.order_payments AS op ON o.order_id = op.order_id
WHERE op.order_id IS NULL 

SELECT r.review_id, r.order_id, r.review_creation_date, r.review_comment_message, o.order_purchase_timestamp
FROM silver.order_reviews AS r
JOIN silver.orders AS o ON r.order_id = o.order_id
WHERE r.review_creation_date::DATE < o.order_purchase_timestamp::DATE --Reviews don't have hour, minute, seconds.