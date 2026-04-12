from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
from pyspark.sql.functions import max as spark_max

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
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(bronze_path)


# Nếu bảng tồn tại → lấy max timestamp
table_name = "iceberg.test_db.fact_air_quality"

if spark.catalog.tableExists(table_name):
    max_time = spark.read.table(table_name) \
        .agg(spark_max("last_updated")) \
        .collect()[0][0]
    print(f"Max timestamp trong silver: {max_time}")
else:
    max_time = None

df = df.withColumn("last_updated", col("last_updated").cast("timestamp"))

if max_time:
    df = df.filter(col("last_updated") > lit(max_time))

df = df.drop(
    "sunrise", "sunset",
    "moonrise", "moonset",
    "moon_phase", "moon_illumination"
)

df = df \
    .withColumnRenamed("air_quality_PM2.5", "pm2_5") \
    .withColumnRenamed("air_quality_PM10", "pm10") \
    .withColumnRenamed("air_quality_Carbon_Monoxide", "carbon_monoxide") \
    .withColumnRenamed("air_quality_Nitrogen_dioxide", "nitrogen_dioxide") \
    .withColumnRenamed("air_quality_Sulphur_dioxide", "sulphur_dioxide") \
    .withColumnRenamed("air_quality_Ozone", "ozone") \
    .withColumnRenamed("air_quality_us-epa-index", "us_aqi") \
    .withColumnRenamed("air_quality_gb-defra-index", "european_aqi")

cols = ["pm2_5","pm10","carbon_monoxide","nitrogen_dioxide",
        "sulphur_dioxide","ozone","us_aqi","european_aqi"]

for c in cols:
    if c in df.columns:
        df = df.withColumn(c, col(c).cast("double"))

# Transform đơn giản
df_clean = df \
    .dropDuplicates() \
    .dropna(subset=["pm2_5","pm10","us_aqi"])
df_clean = df_clean.filter(
    (col("pm2_5") >= 0) &
    (col("pm10") >= 0) &
    (col("us_aqi") >= 0)
)

df_clean = df_clean \
    .withColumn("processing_time", current_timestamp()) \
    .withColumn("data_source", lit("kaggle"))


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