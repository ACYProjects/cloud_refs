from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("PreprocessingPipeline").getOrCreate()

data = spark.read.csv("gs://my-bucket/data.csv", header=True, inferSchema=True)

preprocessed_data = data.drop("col1", "col2", "col5")\
    .fillna(0, subset=["col3", "col4"])\
    .withColumn("col3_2", when(col("col3") > 0, 1).otherwise(0))

concat_udf = udf(lambda x, y: x + " " + y, StringType())
feature_data = preprocessed_data\
    .withColumn("new_col", concat_udf(preprocessed_data.col1, preprocessed_data.col2))\
    .withColumn("col4_squared", col("col4") ** 2)

feature_data.write.mode("overwrite").parquet("gs://my-bucket/feature_data.parquet")

spark.stop()
