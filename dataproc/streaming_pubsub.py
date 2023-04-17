from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, client

spark = SparkSession.builder.appName("PubSubStreamingPipeline").getOrCreate()

schema = StructType([
    StructField("timestamp", StringType()),
    StructField("value", DoubleType())
])

pubsub_data = spark \
    .readStream \
    .format("pubsub") \
    .option("topic", "projects/project_id/topics/topic_name") \
    .option("startingOffset", "earliest") \
    .load()

parsed_data = pubsub_data \
    .selectExpr("CAST(data AS STRING)") \
    .select(from_json("data", schema).alias("parsed_data")) \
    .select("parsed_data.*") \
    .withColumn("log_value", log_udf(col("value")))

bq_client = bigquery.Client()
table_ref = bq_client.dataset("dataset_name").table("table_name")
schema = [SchemaField("timestamp", "STRING"), SchemaField("value", "FLOAT"), SchemaField("log_value", "FLOAT")]
parsed_data \
    .select(to_json(struct(col("*"))).alias("data")) \
    .writeStream \
    .format("bigquery") \
    .outputMode("append") \
    .option("checkpointLocation", "gs://bucket_name/checkpoints") \
    .option("tableReference", table_ref) \
    .option("schema", schema) \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .option("writeDisposition", "WRITE_APPEND") \
    .start()

spark.streams.awaitAnyTermination()

spark.stop()
