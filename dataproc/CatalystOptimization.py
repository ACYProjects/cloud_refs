import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, sum, count, avg, when, lit, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType, StringType

def create_spark_session():
    return SparkSession.builder \
        .appName("CatalystOptimization") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.io.compression.codec", "snappy") \
        .config("spark.sql.optimizer.enabled", "true")  
        .getOrCreate()

def process_data(spark):
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("product_id", StringType(), True),
        StructField("purchase_amount", DoubleType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    df = spark.read.schema(schema).parquet(f"gs://{BUCKET}/user_interactions.parquet")

    df = df.filter(col("purchase_amount") > 0) 
    df = df.withColumn("purchase_date", to_date(col("timestamp"), "yyyy-MM-dd"))

    df = df.select("user_id", "purchase_date", "purchase_amount", "country")  # Column pruning

    total_purchase_per_user = df.groupBy("user_id").agg(sum("purchase_amount").alias("total_purchase"))
    average_purchase_amount = df.agg(avg("purchase_amount")).collect()[0][0]
    purchase_count_by_country = df.groupBy("country").agg(count("*").alias("purchase_count"))

    metrics_df = spark.createDataFrame([
        (average_purchase_amount, "average_purchase_amount"),
        (purchase_count_by_country.count(), "total_purchase_count")
    ], ["value", "metric"])

    total_purchase_per_user.write.parquet(f"gs://{BUCKET}/total_purchase_per_user.parquet")
    metrics_df.write.parquet(f"gs://{BUCKET}/metrics.parquet")

if __name__ == "__main__":
    PROJECT_ID = os.environ.get('PROJECT_ID')
    REGION = os.environ.get('REGION')
    BUCKET = os.environ.get('BUCKET')

    spark = create_spark_session()
    process_data(spark)
    spark.stop()
