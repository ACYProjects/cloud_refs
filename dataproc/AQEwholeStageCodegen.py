import os
from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("Optimization") \
        .config("spark.sql.adaptive.enabled", "true")  
        .config("spark.sql.codegen.wholeStageCodegen", "true")  
        .getOrCreate()

def process_data(spark):
    df = spark.read.parquet(f"gs://{BUCKET}/data.parquet")

    df = df.repartition(1000)

    result = df.groupBy("category").agg(
        sum("value").alias("total"),
        avg("value").alias("average")
    )

    result.show()

if __name__ == "__main__":
    PROJECT_ID = os.environ.get('PROJECT_ID')
    REGION = os.environ.get('REGION')
    BUCKET = os.environ.get('BUCKET')

    spark = create_spark_session()
    process_data(spark)
    spark.stop()
