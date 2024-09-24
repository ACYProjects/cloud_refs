from pyspark.sql.functions import col, when, lit, upper, trim, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import datetime

input_schema = StructType([
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("email", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=True),
    StructField("signup_date", StringType(), nullable=False)
])

df = spark.read.format("csv")
         .option("header", "true")
         .schema(input_schema)
         .load("wasbs://container@storageaccount.blob.core.windows.net/input_data.csv")

cleaned_df = (df
              .withColumn("email", when(col("email").isNull(), lit("unknown@example.com"))
                                  .when(~regexp_match(col("email"), r"^\w+([-+.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$"), lit("invalid@example.com"))
                                  .otherwise(trim(col("email"))))
              .withColumn("age", when(col("age").isNull(), lit(0)).otherwise(col("age")))
              .withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))
              .withColumn("name", upper(trim(col("name"))))
              .withColumn("customer_id", regexp_replace(col("customer_id"), r"^0+", ""))
              .filter(col("age") >= 18)
              .filter(col("signup_date") >= date(2022, 1, 1))
              .filter(col("signup_date") <= date(2023, 12, 31)))

cleaned_df.write.format("synapse")
            .option("dbtable", "customer_data")
            .mode("overwrite")
            .save()
