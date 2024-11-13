from databricks.sql.dataframe import DataFrame
from pyspark.sql.functions import col, to_date, datediff, when

input_df = spark.read.csv("/dir/sales.csv", header=True)

cleaned_df = (
    input_df
    .withColumn("order_date", to_date("order_date", "yyyy-MM-dd"))
    .withColumn("ship_date", to_date("ship_date", "yyyy-MM-dd"))
    .withColumn("ship_delay", datediff(col("ship_date"), col("order_date")))
    .withColumn("order_priority", 
                when(col("ship_delay") <= 3, "Low")
                .when(col("ship_delay") <= 6, "Medium")
                .otherwise("High"))
)

aggregated_df = (
    cleaned_df
    .groupBy("order_priority")
    .agg(
        count("*").alias("total_orders"),
        sum("quantity").alias("total_quantity"),
        sum("sales").alias("total_sales")
    )
)

#Write lo datalake
(
    aggregated_df
    .write
    .format("delta")
    .mode("overwrite")
    .save("/mnt/output/sales_metrics")
)
