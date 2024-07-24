CREATE OR REPLACE PROCEDURE `your_project.your_dataset.analyze_sales`(
  IN start_date DATE,
  IN end_date DATE,
  IN min_order_value FLOAT64,
  OUT total_sales FLOAT64,
  OUT top_products ARRAY<STRUCT<product_id STRING, total_quantity INT64, total_revenue FLOAT64>>
)
BEGIN
  DECLARE avg_order_value FLOAT64;
  DECLARE order_count INT64;

  CREATE TEMP TABLE temp_sales AS
  SELECT
    order_id,
    product_id,
    quantity,
    unit_price,
    quantity * unit_price AS item_total
  FROM
    `your_project.your_dataset.sales_table`
  WHERE
    order_date BETWEEN start_date AND end_date
    AND quantity * unit_price >= min_order_value;

  -- Calculate total sales
  SET total_sales = (
    SELECT SUM(item_total)
    FROM temp_sales
  );

  -- Calculate average order value and order count
  SET (avg_order_value, order_count) = (
    SELECT AS STRUCT
      AVG(order_total),
      COUNT(DISTINCT order_id)
    FROM (
      SELECT
        order_id,
        SUM(item_total) AS order_total
      FROM
        temp_sales
      GROUP BY
        order_id
    )
  );

  SET top_products = ARRAY(
    SELECT AS STRUCT
      product_id,
      SUM(quantity) AS total_quantity,
      SUM(item_total) AS total_revenue
    FROM
      temp_sales
    GROUP BY
      product_id
    ORDER BY
      total_revenue DESC
    LIMIT 5
  );

  SELECT FORMAT("Analysis completed. Date range: %t to %t", start_date, end_date) AS log_message;
  SELECT FORMAT("Total sales: $%.2f, Avg order value: $%.2f, Order count: %d", 
                total_sales, avg_order_value, order_count) AS log_message;

  DROP TABLE temp_sales;
END;
