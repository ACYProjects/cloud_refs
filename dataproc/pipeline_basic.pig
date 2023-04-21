data = LOAD 'gs://my-bucket/my-csv-file.csv' USING PigStorage(',');

data_schema = LOAD 'gs://my-bucket/data-schema.txt' AS (column1:chararray, column2:int, column3:float);

joined_data = JOIN data BY $0, data_schema BY column1;

filtered_data = FILTER joined_data BY data.column2 > 10;

grouped_data = GROUP filtered_data BY data_schema.column3;
summed_data = FOREACH grouped_data GENERATE group, SUM(filtered_data.data.column2);

STORE summed_data INTO 'gs://my-bucket/my-output.csv' USING PigStorage(',');
