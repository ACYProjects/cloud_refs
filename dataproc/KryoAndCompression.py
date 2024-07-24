import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, sum, count, avg, when, lit, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType, StringType

def create_spark_session():
    return SparkSession.builder \
        .appName("KryoAndCompression") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.io.compression.codec", "snappy") \
        .getOrCreate()

def process_data(spark):
    # Define schema for user interactions
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("product_id", StringType(), True),
        StructField("purchase_amount", DoubleType(), True),
        StructField("country", StringType(), True)  # Added country column
    ])

    # Read data from Parquet file
    df = spark.read.schema(schema).parquet(f"gs://{BUCKET}/user_interactions.parquet")

    # Data cleaning and transformation
    df = df.filter(col("purchase_amount") > 0)  # Filter out negative purchases
    df = df.withColumn("purchase_date", to_date(col("timestamp"), "yyyy-MM-dd"))  # Extract purchase date

    # Calculate metrics
    total_purchase_per_user = df.groupBy("user_id").agg(sum("purchase_amount").alias("total_purchase"))
    average_purchase_amount = df.agg(avg("purchase_amount")).collect()[0][0]
    purchase_count_by_country = df.groupBy("country").agg(count("*").alias("purchase_count"))

    # Create a new DataFrame with calculated metrics
    metrics_df = spark.createDataFrame([
        (average_purchase_amount, "average_purchase_amount"),
        (purchase_count_by_country.count(), "total_purchase_count")
    ], ["value", "metric"])

    # Write results to Parquet
    total_purchase_per_user.write.parquet(f"gs://{BUCKET}/total_purchase_per_user.parquet")
    metrics_df.write.parquet(f"gs://{BUCKET}/metrics.parquet")

if __name__ == "__main__":
    PROJECT_ID = os.environ.get('PROJECT_ID')
    REGION = os.environ.get('REGION')
    BUCKET = os.environ.get('BUCKET')

    spark = create_spark_session()
    process_data(spark)
    spark.stop()
