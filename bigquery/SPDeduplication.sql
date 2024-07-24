CREATE OR REPLACE PROCEDURE `your_project.your_dataset.deduplicate_customer_data`(
  IN source_table STRING,
  IN destination_table STRING,
  IN dedup_columns ARRAY<STRING>,
  OUT duplicate_count INT64,
  OUT processed_count INT64
)
BEGIN
  DECLARE sql_statement STRING;
  DECLARE column_list STRING;
  
  SET sql_statement = FORMAT("""
    CREATE OR REPLACE TEMP TABLE deduplicated_data AS
    WITH ranked_data AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY %s
          ORDER BY last_updated_timestamp DESC
        ) AS row_num
      FROM `%s`
    )
    SELECT * EXCEPT(row_num)
    FROM ranked_data
    WHERE row_num = 1
  """, (SELECT STRING_AGG(CONCAT('`', column_name, '`'), ', ') FROM UNNEST(dedup_columns) AS column_name), source_table);

  EXECUTE IMMEDIATE sql_statement;

  SET duplicate_count = (
    SELECT COUNT(*) FROM `%s` - (SELECT COUNT(*) FROM deduplicated_data)
  );

  SET column_list = (
    SELECT STRING_AGG(column_name, ', ')
    FROM (
      SELECT column_name
      FROM `%s`.INFORMATION_SCHEMA.COLUMNS
      WHERE table_name = SPLIT(source_table, '.')[OFFSET(2)]
      ORDER BY ordinal_position
    )
  );

  SET sql_statement = FORMAT("""
    CREATE OR REPLACE TABLE `%s` AS
    SELECT %s
    FROM deduplicated_data
  """, destination_table, column_list);

  EXECUTE IMMEDIATE sql_statement;

  SET processed_count = (SELECT COUNT(*) FROM `%s`);

  SELECT FORMAT('Deduplication complete. Processed %d records, removed %d duplicates.', 
                processed_count, duplicate_count) AS log_message;

END;
