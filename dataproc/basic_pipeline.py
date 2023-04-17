from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("ExamplePipeline").getOrCreate()

data = spark.read.csv("gs://my-bucket/data.csv", header=True, inferSchema=True)

preprocessed_data = data.select("col1", "col2", "col3").withColumnRenamed("col1", "new_col1")

filtered_data = preprocessed_data.filter(preprocessed_data.col2 > 10)
grouped_data = preprocessed_data.groupBy("col1").agg({"col2": "mean", "col3": "max"})

def square(x):
    return x ** 2

square_udf = udf(square, IntegerType())

new_data = preprocessed_data.withColumn("new_col4", square_udf(preprocessed_data.col3))
other_data = spark.read.csv("gs://my-bucket/other_data.csv", header=True, inferSchema=True)
joined_data = preprocessed_data.join(other_data, "col1", "left")

filtered_data.write.mode("overwrite").parquet("gs://my-bucket/filtered_data.parquet")
grouped_data.write.mode("overwrite").parquet("gs://my-bucket/grouped_data.parquet")
new_data.write.mode("overwrite").parquet("gs://my-bucket/new_data.parquet")
joined_data.write.mode("overwrite").parquet("gs://my-bucket/joined_data.parquet")

spark.stop()
