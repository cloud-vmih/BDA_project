from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("DW_GOLD_PRODUCTION_V4") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://23133083thuyvan-master:9083") \
    .config("spark.sql.catalog.iceberg.warehouse", "hdfs://23133083thuyvan-master:9000/user/hive/lakehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Starting Gold Warehouse creation with MERGE...")

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS iceberg.dwh
    LOCATION 'hdfs://23133083thuyvan-master:9000/user/hive/lakehouse/gold/dwh/'
""")

TABLE_NAME = "iceberg.dwh.fact_aqi"

# Lấy watermark từ Fact table
if spark.catalog.tableExists(TABLE_NAME):
    last_processing_time = spark.sql(f"""
        SELECT COALESCE(MAX(created_at), TIMESTAMP('1970-01-01 00:00:00'))
        FROM {TABLE_NAME}
    """).collect()[0][0]
else:
    last_processing_time = "1970-01-01 00:00:00"

print(f"Watermark: {last_processing_time}")

df_silver = spark.read.format("iceberg") \
    .load("iceberg.air_quality_db.air_quality_silver") \
    .filter(col("processing_time") > lit(last_processing_time)) \
    .select(
        col("timestamp"),
        col("location"),
        col("country"),
        col("latitude").cast("double"),
        col("longitude").cast("double"),
        col("condition_text"),
        col("temperature_2m").alias("temperature"),
        col("relative_humidity_2m").alias("humidity"),
        col("apparent_temperature").alias("feels_like"),
        col("cloud_cover"),
        col("wind_speed_10m").alias("wind_speed"),
        col("wind_direction_10m").alias("wind_degree"),
        col("wind_gusts_10m").alias("gust"),
        col("uv_index"),
        col("pm2_5"),
        col("pm10"),
        col("nitrogen_dioxide").alias("no2"),
        col("sulphur_dioxide").alias("so2"),
        col("carbon_monoxide").alias("co"),
        col("ozone"),
        col("us_aqi_index"),
        col("aqi_label"),
        col("data_source"),
        col("processing_time"),
        col("condition_text")
    ) \
    .dropDuplicates(["timestamp", "location"])

if df_silver.rdd.isEmpty():
    print("No new data to process.")
    spark.stop()
    exit()

print(f"Processing {df_silver.count()} new records from Silver.")


def merge_dim(df_new, target_table, join_condition):
    """
    Hàm merge dim table để tránh trùng lặp
    df_new: DataFrame chứa dữ liệu mới
    target_table: Tên bảng Iceberg đích (ví dụ: iceberg.dwh.dim_location)
    join_condition: Điều kiện join ON
    """
    if not spark.catalog.tableExists(target_table):
        print(f"Creating table {target_table} for the first time...")
        df_new.writeTo(target_table) \
            .tableProperty("format-version", "2") \
            .create()
    else:
        df_new.createOrReplaceTempView("new_dim")
        spark.sql(f"""
            MERGE INTO {target_table} t
            USING new_dim s
            ON {join_condition}
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Merged data into {target_table}")


# DIM LOCATION
dim_location_new = df_silver.select(
    "location", "country", "latitude", "longitude"
).dropDuplicates()

dim_location_new = dim_location_new.withColumn(
    "location_id", abs(hash("location", "country", "latitude", "longitude"))
)

merge_dim(
    dim_location_new, 
    "iceberg.dwh.dim_location",
    "t.location = s.location AND t.country = s.country AND t.latitude = s.latitude AND t.longitude = s.longitude"
)

# DIM DATE
dim_date_new = df_silver.select(
    col("timestamp").alias("full_date"),
    dayofmonth("timestamp").alias("day"),
    month("timestamp").alias("month"),
    year("timestamp").alias("year"),
    hour("timestamp").alias("hour")
).dropDuplicates()

dim_date_new = dim_date_new.withColumn("date_id", 
    (year("full_date")*1000000 + month("full_date")*10000 + dayofmonth("full_date")*100 + hour("full_date")*60 + minute("full_date"))
)

merge_dim(
    dim_date_new, 
    "iceberg.dwh.dim_date",
    "t.full_date = s.full_date"
)

# DIM WEATHER
dim_weather_new = df_silver.select(
    col("condition_text"),
    col("temperature"),
    col("humidity"),
    col("feels_like"),
    col("cloud_cover"),
    col("uv_index"),
    col("precipitation")
).dropDuplicates()

dim_weather_new = dim_weather_new.withColumn(
    "weather_id", abs(hash("condition_text", "temperature", "humidity", "uv_index", "cloud_cover", "precipitation"))
)

merge_dim(
    dim_weather_new, 
    "iceberg.dwh.dim_weather",
    "t.condition_text = s.condition_text AND t.temperature = s.temperature AND t.humidity = s.humidity AND t.uv_index = s.uv_index AND t.cloud_cover = s.cloud_cover AND t.precipitation = s.precipitation"
)

# DIM WIND
dim_wind_new = df_silver.select(
    col("wind_speed_10m"),
    col("wind_degree"),
    col("gust")
).dropDuplicates()

dim_wind_new = dim_wind_new.withColumn(
    "wind_id", abs(hash("wind_speed", "wind_degree", "gust"))
)

merge_dim(
    dim_wind_new, 
    "iceberg.dwh.dim_wind",
    "t.wind_speed = s.wind_speed AND t.wind_degree = s.wind_degree AND t.gust = s.gust"
)


fact = df_silver \
    .withColumn("date_id", abs(hash("timestamp"))) \
    .withColumn("location_id", abs(hash("location", "country", "latitude", "longitude"))) \
    .withColumn("weather_id", abs(hash("condition_text", "temperature", "humidity", "uv_index", "cloud_cover", "precipitation"))) \
    .withColumn("wind_id", abs(hash("wind_speed", "wind_degree", "gust")))

fact_aqi = fact.select(
    monotonically_increasing_id().alias("fact_id"),
    col("date_id"),
    col("location_id"),
    col("weather_id"),
    col("wind_id"),
    col("pm2_5").alias("pm25"),
    col("pm10"),
    col("no2"),
    col("so2"),
    col("co"),
    col("ozone"),
    col("us_aqi_index").alias("aqi_category"),
    col("aqi_label"),
    col("data_source"),
    col("processing_time").alias("created_at")
)

fact_aqi.writeTo("iceberg.dwh.fact_aqi").append()

print("GOLD WAREHOUSE UPDATED SUCCESSFULLY WITH MERGE!")

spark.catalog.clearCache()
spark.stop()