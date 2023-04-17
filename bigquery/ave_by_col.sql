SELECT
  column1,
  AVG(column2) OVER (PARTITION BY column1) AS column2_avg
FROM
  your_table
