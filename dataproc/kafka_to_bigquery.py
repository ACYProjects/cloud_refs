from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, from_unixtime, sum, regexp_replace
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, TimestampType

spark = SparkSession.builder.appName("KafkaToBigQuery").getOrCreate()

kafka_source = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka_broker_host:port") \
    .option("subscribe", "topic_name") \
    .load()

value_schema = "value_schema_string"

df = kafka_source \
    .select(from_unixtime(col("value").cast("string")).alias("event_time"),
            regexp_replace(col("value").cast("string"), "^.*,", "").alias("data"))

parsed_df = df.withColumn("data", col("data").cast(DoubleType()))

validated_df = parsed_df.where(col("data").isNotNull() & (col("data").cast("string").rlike("^\\d+(\\.\\d+)?$")))

valid_data_df = validated_df.filter(col("data").isNotNull())

windowed_df = valid_data_df \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(window("event_time", "5 minutes")) \
    .agg(sum("data").alias("sum_data"))

bigquery_schema = StructType([
    StructField("window", StructType([
        StructField("start", TimestampType(), True),
        StructField("end", TimestampType(), True)
    ]), True),
    StructField("sum_data", DoubleType(), True)
])

windowed_df \
    .writeStream \
    .format("bigquery") \
    .option("temporaryGcsBucket", "gs://temp_bucket_name") \
    .option("table", "project_id:dataset_name.table_name") \
    .option("updateMode", "append") \
    .option("bigQueryCreateDisposition", "CREATE_IF_NEEDED") \
    .option("bigQueryCreateDisposition", "CREATE_IF_NEEDED") \
    .option("bigQueryWriteMode", "INSERT_OVERWRITE") \
    .option("bigQueryCreateDisposition", "CREATE_IF_NEEDED") \
    .option("bigQueryWriteMode", "INSERT_OVERWRITE") \
    .option("schema", bigquery_schema) \
    .start() \
    .awaitTermination()
