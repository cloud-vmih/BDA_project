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
table_name = "iceberg.air_quality_db.air_quality_silver"

if spark.catalog.tableExists(table_name):
    max_time = spark.read.table(table_name) \
        .filter(col("data_source") == "kaggle") \
        .agg(spark_max("timestamp")) \
        .collect()[0][0]
    print(f"Max timestamp trong silver: {max_time}")
else:
    max_time = None

df = df.withColumn("last_updated", col("last_updated").cast("timestamp"))

if max_time:
    df = df.filter(col("last_updated") > lit(max_time))

df = df.drop(
    "sunrise", "sunset", "last_updated_epoch",
    "moonrise", "moonset",
    "moon_phase", "moon_illumination", "wind_direction",
    "temperature_fahrenheit", "wind_mph", "pressure_mb", "pressure_in", "precip_in", "feels_like_fahrenheit", "visibility_km", "visibility_miles", "gust_mph"
)

df = df \
    .withColumnRenamed("air_quality_PM2.5", "pm2_5") \
    .withColumnRenamed("air_quality_PM10", "pm10") \
    .withColumnRenamed("air_quality_Carbon_Monoxide", "carbon_monoxide") \
    .withColumnRenamed("air_quality_Nitrogen_dioxide", "nitrogen_dioxide") \
    .withColumnRenamed("air_quality_Sulphur_dioxide", "sulphur_dioxide") \
    .withColumnRenamed("air_quality_Ozone", "ozone") \
    .withColumnRenamed("air_quality_us-epa-index", "us_aqi_index") \
    .withColumnRenamed("air_quality_gb-defra-index", "european_aqi") \
    .withColumnRenamed("last_updated", "timestamp") \
    .withColumnRenamed("location_name", "location") \
    .withColumnRenamed("temperature_celsius", "temperature_2m") \
    .withColumnRenamed("humidity", "relative_humidity_2m") \
    .withColumnRenamed("feels_like_celsius", "apparent_temperature") \
    .withColumnRenamed("precip_mm", "precipitation") \
    .withColumnRenamed("cloud", "cloud_cover") \
    .withColumnRenamed("wind_kph", "wind_speed_10m") \
    .withColumnRenamed("wind_degree", "wind_direction_10m") \
    .withColumnRenamed("gust_kph", "wind_gusts_10m") 
    

cols = ["pm2_5","pm10","carbon_monoxide","nitrogen_dioxide",
        "sulphur_dioxide","ozone","temperature_2m","relative_humidity_2m","apparent_temperature",
        "precipitation","cloud_cover","wind_speed_10m","wind_direction_10m","wind_gusts_10m"]

for c in cols:
    if c in df.columns:
        df = df.withColumn(c, col(c).cast("double"))

# Transform đơn giản
df_clean = df \
    .dropDuplicates() \
    .dropna(subset=["pm2_5","pm10","us_aqi_index", "carbon_monoxide","nitrogen_dioxide","sulphur_dioxide","ozone"]) 
df_clean = df_clean.filter(
    (col("pm2_5") >= 0) &
    (col("pm10") >= 0) &
    (col("us_aqi_index") >= 0) &
    (col("carbon_monoxide") >= 0) &
    (col("nitrogen_dioxide") >= 0) &
    (col("sulphur_dioxide") >= 0) &
    (col("ozone") >= 0)
)

# Thêm nhãn mô tả cho thang đo để sau này làm Dashboard cho dễ
df_clean = df_clean.withColumn("aqi_label", 
    when(col("us_aqi_index") == 1, "Tốt")
    .when(col("us_aqi_index") == 2, "Trung bình")
    .when(col("us_aqi_index") == 3, "Kém (nhạy cảm)")
    .when(col("us_aqi_index") == 4, "Xấu")
    .when(col("us_aqi_index") == 5, "Rất xấu")
    .otherwise("Nguy hại")
)

df_clean = df_clean \
    .withColumn("processing_time", current_timestamp()) \
    .withColumn("data_source", lit("kaggle")) \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("weather_code", lit(1)) \
    .withColumn("us_aqi", col("us_aqi_index")) 

target_schema = [
    "timestamp", "location", "latitude", "longitude", "country", 
    "condition_text", "timezone", "temperature_2m", "relative_humidity_2m", 
    "apparent_temperature", "precipitation", "cloud_cover", "wind_speed_10m", 
    "wind_direction_10m", "wind_gusts_10m", "uv_index", "weather_code", 
    "european_aqi", "us_aqi", "us_aqi_index", "aqi_label", "pm10", "pm2_5", 
    "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone", 
    "processing_time", "data_source"
]

df_clean = df_clean.select(*target_schema)

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS iceberg.air_quality_db
    LOCATION 'hdfs://23133083thuyvan-master:9000/user/hive/lakehouse/silver/'
""")

# Ghi vào Silver (Iceberg)
table_name = "iceberg.air_quality_db.air_quality_silver"
if not spark.catalog.tableExists(table_name):
    print(f"Tạo Iceberg table mới: {table_name}")
    df_clean.writeTo(table_name) \
        .tableProperty("format-version", "2") \
        .partitionedBy("days(timestamp)", "location") \
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