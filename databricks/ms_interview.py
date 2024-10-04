from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, sum, desc

spark = SparkSession.builder.appName("ProductSalesAnalysis").getOrCreate()

product_details_df = spark.createDataFrame([
    (1, "Dell Laptop PC", "PC", 200),
    (2, "HP Laptop PC", "PC", 250),
    (3, "Lenovo Laptop PC", "PC", 190),
    (4, "Dell Mouse", "PC Accessories", 20),
    (5, "Dell Keyboard", "PC Accessories", 30),
    (6, "HP Printer", "Printer & Scanners", 180)
], ["product_id", "product_name", "product_type", "price"])

sales_df = spark.createDataFrame([
    (101, "2023-12-01", 2),
    (102, "2023-12-02", 4),
    (103, "2023-12-03", 5),
    (104, "2024-06-29", 2),
    (105, "2024-06-30", 6),
    (106, "2024-07-01", 6),
    (107, "2024-07-01", 4),
    (108, "2024-07-03", 5)
], ["sales_id", "sales_date", "product_id"])

joined_df = product_details_df.join(sales_df, "product_id")

result_df = joined_df.filter(year(col("sales_date")) == 2024) \
    .groupBy("product_type") \
    .agg(sum("price").alias("total_sales_amount")) \
    .orderBy(desc("total_sales_amount"))

result_df.show()
