from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder \
    .appName("DataprocExample") \
    .config("spark.scheduler.mode", "FAIR") \
    .config("spark.locality.wait", "10s") \
    .config("spark.speculation", "true") \
    .config("spark.speculation.multiplier", "2") \
    .getOrCreate()

df = spark.read.csv("gs://your-bucket/large_dataset.csv", header=True, inferSchema=True)

result = df.groupBy("category") \
    .agg(sum("sales").alias("total_sales")) \
    .orderBy(col("total_sales").desc())

result.write.csv("gs://your-bucket/output/sales_summary", header=True, mode="overwrite")

spark.stop()
