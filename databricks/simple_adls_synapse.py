from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark.conf.set("fs.adl.impl", "org.apache.hadoop.fs.adl.AdlsFs")
spark.conf.set("fs.adl.oauth2.accessProvider", "spark.adl.spn")
spark.conf.set("spark.adl.spn.servicePrincipalId", "<aad-app-id>")
spark.conf.set("spark.adl.spn.servicePrincipalSecret", "<aad-app-secret>")

data_schema = StructType([
    StructField("column1", StringType(), True),
    StructField("column2", IntegerType(), True)
])


data_path = "adl://<adl-storage-account>.azuredatalake.net/<container>/<file.csv>"
df = spark.read.csv(data_path, schema=data_schema, header=True)


def is_valid(row):
    return row["column1"] is not None and row["column2"] > 0

valid_data = df.filter(is_valid)


spark.conf.set(" jdbc.url", "jdbc:sqlserver://<synapse-workspace>.database.windows.net:1433;database=synapse;user=<username>;password=<password>;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net")
spark.conf.set("spark.sql.jdbc.spark.write.options.CheckConstraint", "false")

valid_data.write.format("jdbc") \
    .mode("append") \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .option("dbtable", "<synapse-linked-service-name>.<table-name>") \
    .save()

