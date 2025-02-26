### A sample use case of HUDI

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import uuid
from datetime import datetime


# Initialize Spark Session with Hudi
spark = SparkSession.builder \
    .appName("HudiExample") \
    .config("spark.jars", "hudi-spark-bundle_2.12-0.14.0.jar") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()


# Sample Data
data = [
    ("1", "Alice", 25, "2024-02-26T10:15:30"),
    ("2", "Bob", 30, "2024-02-26T10:16:30"),
    ("3", "Charlie", 28, "2024-02-26T10:17:30")
]

columns = ["id", "name", "age", "ts"]

df = spark.createDataFrame(data, columns)
print("*** df ***")
print(df)


# Define Hudi Table Path and Options
hudi_table_path = "file:///tmp/test_table"
hudi_table_name = "hudi_persons"

hudi_options = {
    "hoodie.table.name": hudi_table_name,
    "hoodie.datasource.write.operation": "insert",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.hive_sync.enable": "false",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
}

# Write Data as Hudi Table
df.write.format("hudi").options(**hudi_options).mode("overwrite").save(hudi_table_path)
print("*** Hudi table created successfully! ***")

update_data = [
    ("2", "Bob", 32, "2024-02-26T12:00:00"),
    ("4", "David", 29, "2024-02-26T12:01:00")
]
update_df = spark.createDataFrame(update_data, columns)

update_options = {
    "hoodie.table.name": hudi_table_name,
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.hive_sync.enable": "false",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
}

# Upsert Data
update_df.write.format("hudi").options(**update_options).mode("append").save(hudi_table_path)
print("*** Hudi table updated successfully! ***")


updated_hudi_df = spark.read.format("hudi").load(hudi_table_path)
print("*** updated_hudi_df ***")
print(updated_hudi_df)
