from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count

spark = SparkSession.builder.appName("IntermediateSparkScript").getOrCreate()

spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.shuffle.service.enabled", "true")

spark.conf.set("spark.sql.cacheTable.useDiskStore", "true")
spark.conf.set("spark.memory.offHeap.enabled", "true") 
spark.conf.set("spark.memory.offHeap.size", "1g") 

df = spark.read.parquet("/path/to/large/dataset.parquet")

df_repartitioned = df.repartition(100) 

df_repartitioned.cache()

df_repartitioned.count()

df_repartitioned.createOrReplaceTempView("temp_view")
spark.sql("CACHE TABLE temp_view")

# Perform some complex operations
result = (spark.table("temp_view")
    .groupBy("customer_id", "product_category")
    .agg(
        sum("order_value").alias("total_value"),
        avg("order_value").alias("avg_value"),
        count("order_id").alias("order_count")
    )
    .filter(col("total_value") > 1000)
    .sort(col("total_value").desc())
)

result.cache()
result.count() 

# Write the results to a Delta table
result.write.format("delta").mode("overwrite").saveAsTable("customer_order_summary")

result.show(10)

print(spark.catalog.listTables())

df_repartitioned.unpersist()
result.unpersist()

spark.sql("UNCACHE TABLE temp_view")

spark.catalog.clearCache()

spark.stop()
