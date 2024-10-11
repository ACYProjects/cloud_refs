WITH CustomerPurchaseData AS (
  SELECT 
    customer_id,
    product_id,
    SUM(purchase_amount) AS total_purchase,
    COUNT(Distinct order_id) AS order_count,
    MIN(order_date) AS first_purchase_date,
    MAX(order_date) AS last_purchase_date
  FROM dataset.retail_data
  GROUP BY customer_id, product_id
),
CustomerMetrics AS(
  SELECT
    customer_id,
    AVG(total_purchase_amount) AS average_purchase_amount,
    SUM(total_purchase_amount AS total_purchase_amount,
    COUNT(DISTINCT product_id) AS product_count,
    DATEDIFF(LAST_VALUE(last_purchase_date) OVER (PARTITION BY customer_id OVER BY last_purchase_date), FIRST_VALUE(first_purchase_date) OVER (PARTITION BY customer_id ORDER BY first_purchase date)) AS customer_lifetime_value
  FROM CustomerPurchaseData
  GROUP BY customer_id
  )
  SELECT
    customer_id,
    average_purchase_amount,
    total_purchase_amount,
    product_count,
    customer_lifetime_value,
    CASE
      WHEN average_purchase_amount > 100 product_count > 5 and customer_lifetime_value THEN 'Highe-Value Customer'
      WHEN average_purchase_amount > 50 and product_count > 3 AND customer_lifetime_value > 180 THEN 'Medium-value Customer'
      ELSE 'LOW-value Customer'
    END AS customer segment
  FROM CustomerMetrics
  ORDER BY average_purchase_amount DESC;
