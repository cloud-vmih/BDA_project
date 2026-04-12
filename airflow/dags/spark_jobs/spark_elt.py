from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys


bronze_path = "hdfs://23133083thuyvan-master:9000/user/hive/lakehouse/bronze/raw_kaggle/GlobalWeatherRepository.csv"

spark = SparkSession.builder \
    .appName("ETL_Kaggle_to_Silver") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://23133083thuyvan-master:9083") \
    .config("spark.sql.catalog.iceberg.warehouse", "hdfs://23133083thuyvan-master:9000/user/hive/lakehouse") \
    .getOrCreate()

# Đọc từ Bronze
print(f"Đọc dữ liệu Bronze từ: {bronze_path}")
df = spark.read.option("header", "true").csv(bronze_path)

# Transform đơn giản
df_clean = df.withColumn("processing_time", current_timestamp())

spark.sql("CREATE SCHEMA IF NOT EXISTS iceberg.test_db")
print("Iceberg schema 'iceberg.test_db' đã sẵn sàng")

# Ghi vào Silver (Iceberg)
table_name = "iceberg.test_db.fact_air_quality"
if not spark.catalog.tableExists(table_name):
    print(f"Tạo Iceberg table mới: {table_name}")
    df_clean.writeTo(table_name) \
        .tableProperty("format-version", "2") \
        .create()
    print(f"Bảng {table_name} đã được tạo và dữ liệu đã được ghi.")
else:
    print(f"Bảng đã tồn tại, append dữ liệu vào {table_name}")
    df_clean.writeTo(table_name) \
        .tableProperty("format-version", "2") \
        .append()
    print(f"Dữ liệu đã được append vào {table_name}.")
print("ETL from Bronze to Silver completed!")
spark.stop()