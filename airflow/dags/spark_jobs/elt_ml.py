from curses.ascii import TAB

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, day, days, hour, dayofweek, max

spark = SparkSession.builder \
    .appName("Silver_to_Gold") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://23133083thuyvan-master:9083") \
    .config("spark.sql.catalog.iceberg.warehouse", "hdfs://23133083thuyvan-master:9000/user/hive/lakehouse") \
    .getOrCreate()

# Ghi vào Gold (Iceberg)
table_name = "iceberg.air_quality_ml.air_quality_features"
max_kaggle = "1900-01-01 00:00:00"
max_api = "1900-01-01 00:00:00"

if spark.catalog.tableExists(table_name):
    print(f"Đọc max timestamp từ {table_name} để filter dữ liệu mới")
    stats = spark.table(table_name) \
            .groupBy("data_source") \
            .agg(max("timestamp").alias("max_ts")) \
            .collect()
    for row in stats:
        if row['data_source'] == 'kaggle':
            max_kaggle = row['max_ts']
        elif row['data_source'] == 'open-meteo':
            max_api = row['max_ts']

# Đọc từ Silver
print(f"Đọc dữ liệu Silver từ iceberg.air_quality_db.air_quality_silver")
df = spark.read \
    .format("iceberg") \
    .load("iceberg.air_quality_db.air_quality_silver")

# LOGIC LỌC CHUẨN: Nguồn nào lọc theo mốc của nguồn đó
df_kaggle_new = df.filter((col("data_source") == "kaggle") & (col("timestamp") > max_kaggle))
df_api_new = df.filter((col("data_source") == "open-meteo") & (col("timestamp") > max_api))

# Union lại để có tập dữ liệu mới hoàn toàn
df_final = df_kaggle_new.unionAll(df_api_new)

if df_final.count() == 0:
    print("Không có dữ liệu mới để xử lý. Kết thúc ETL.")
    spark.stop()
    exit(0)

df_gold = df_final.select(
    col("timestamp"),
    col("pm2_5").alias("pm2_5"),
    col("pm10").alias("pm10"),
    col("carbon_monoxide").alias("co"),
    col("nitrogen_dioxide").alias("no2"),
    col("sulphur_dioxide").alias("so2"),
    col("ozone").alias("o3"),
    col("temperature_2m").alias("temperature"),
    col("relative_humidity_2m").alias("humidity"),
    col("us_aqi_index").alias("AQI"),
    col("data_source"),
).withColumn(
    "hour", hour("timestamp")
).withColumn(
    "day_of_week", dayofweek("timestamp")
).withColumn(
    "ingestion_time", current_timestamp()
)

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS iceberg.air_quality_ml
    LOCATION 'hdfs://23133083thuyvan-master:9000/user/hive/lakehouse/gold/ml/'
""")


if not spark.catalog.tableExists(table_name):
    print(f"Tạo Iceberg table mới: {table_name}")
    df_gold.writeTo(table_name) \
        .tableProperty("format-version", "2") \
        .partitionedBy(days("timestamp")) \
        .create()
    print(f"Bảng {table_name} đã được tạo và dữ liệu đã được ghi.")
else:
    print(f"Bảng đã tồn tại, append dữ liệu vào {table_name}")
    df_gold.writeTo(table_name) \
        .tableProperty("format-version", "2") \
        .append()
    print(f"Dữ liệu đã được append vào {table_name}.")
print("ETL from Silver to Gold completed!")

spark.stop()