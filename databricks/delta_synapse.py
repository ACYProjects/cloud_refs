from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, sum as _sum

spark = SparkSession.builder \
    .appName("Databricks Example") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

delta_path = "/delta/location"
df = spark.read.format("delta").load(delta_path)
#df.show()

df = df.filter(col("col1") > 10)

df = df.withColumn("sum_column", col("col1") + col("col2"))

df = df.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))

df_grouped = df.groupBy("category").agg(
    _sum("sum_column").alias("total_sum")
)
df_grouped.show()

# Write the transformed data to Azure Synapse
synapse_jdbc_url = "jdbc:sqlserver://<server-name>.sql.azuresynapse.net:1433;database=<database-name>;user=<username>;password=<password>;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"

df_grouped.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url", synapse_jdbc_url) \
    .option("dbtable", "synapse_table_name") \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

spark.stop()
