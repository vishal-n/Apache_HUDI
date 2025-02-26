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
