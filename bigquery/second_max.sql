SELECT
  MAX(column_name) as second_max
FROM (SELECT *,
      RANK() OVER (ORDER BY column_name DESC) AS rank_num
      FROM dataset.table)
WHERE
  rank_num = 2
