CREATE OR REPLACE TABLE your_dataset.deduped_table AS
SELECT
  *,
  ROW_NUMBER() OVER (
    PARTITION BY column1, column2
    ORDER BY created_at DESC
  ) AS row_num
FROM
  your_dataset.your_table;

SELECT
  *
FROM
  your_dataset.deduped_table
WHERE
  row_num = 1;

DROP TABLE your_dataset.deduped_table;
