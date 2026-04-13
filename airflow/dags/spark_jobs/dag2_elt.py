from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# ==============================
# INIT SPARK
# ==============================
spark = SparkSession.builder \
    .appName("ELT_Silver_to_DWH") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://23133083thuyvan-master:9083") \
    .config("spark.sql.catalog.iceberg.warehouse", "hdfs://23133083thuyvan-master:9000/user/hive/lakehouse") \
    .getOrCreate()

# ==============================
# CREATE DWH SCHEMA
# ==============================
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS iceberg.dwh
    LOCATION 'hdfs://23133083thuyvan-master:9000/user/hive/lakehouse/gold/dwh/'
""")

# ==============================
# READ SILVER
# ==============================
df = spark.table("iceberg.air_quality_db.air_quality_silver")

# ==============================
# DIM DATE
# ==============================
dim_date = df.select(
    col("timestamp").alias("full_date"),
    dayofmonth("timestamp").alias("day"),
    month("timestamp").alias("month"),
    year("timestamp").alias("year"),
    hour("timestamp").alias("hour"),
    minute("timestamp").alias("minute")
).dropDuplicates()

dim_date = dim_date.withColumn("date_id", monotonically_increasing_id())

# ==============================
# DIM LOCATION
# ==============================
dim_location = df.select(
    col("location"),
    col("country"),
    col("latitude"),
    col("longitude")
).dropDuplicates()

dim_location = dim_location.withColumn("location_id", monotonically_increasing_id())

# ==============================
# DIM WEATHER
# ==============================
dim_weather = df.select(
    col("condition_text"),
    col("apparent_temperature").alias("feels_like"),
    col("relative_humidity_2m").alias("humidity"),
    col("temperature_2m").alias("temperature"),
    col("cloud_cover").alias("cloud"),
    col("uv_index"),
    col("precipitation")
).dropDuplicates()

dim_weather = dim_weather.withColumn("weather_id", monotonically_increasing_id())

# ==============================
# DIM WIND
# ==============================
dim_wind = df.select(
    col("wind_speed_10m").alias("wind_speed"),
    col("wind_direction_10m").alias("wind_degree"),
    col("wind_gusts_10m").alias("gust")
).dropDuplicates()

dim_wind = dim_wind.withColumn("wind_id", monotonically_increasing_id())


# ==============================
# FACT TABLE
# ==============================
fact = df.alias("f") \
    .join(dim_date.alias("d"),
          col("f.timestamp") == col("d.full_date")) \
    .join(dim_location.alias("l"),
          (col("f.location") == col("l.location")) &
          (col("f.latitude") == col("l.latitude")) &
          (col("f.longitude") == col("l.longitude"))) \
    .join(dim_weather.alias("w"),
          (col("f.condition_text") == col("w.condition_text")) &
          (col("f.apparent_temperature") == col("w.feels_like")) &
          (col("f.relative_humidity_2m") == col("w.humidity")) &
          (col("f.temperature_2m") == col("w.temperature")) &
          (col("f.cloud_cover") == col("w.cloud")) &
          (col("f.uv_index") == col("w.uv_index")) &
          (col("f.precipitation") == col("w.precipitation"))) \
    .join(dim_wind.alias("wi"),
          (col("f.wind_speed_10m") == col("wi.wind_speed")) &
          (col("f.wind_direction_10m") == col("wi.wind_degree")) &
          (col("f.wind_gusts_10m") == col("wi.gust"))) \
    .select(
        col("d.date_id"),
        col("l.location_id"),
        col("wi.wind_id"),
        col("w.weather_id"),

        col("f.pm2_5").alias("pm25"),
        col("f.pm10"),
        col("f.nitrogen_dioxide").alias("no2"),
        col("f.sulphur_dioxide").alias("so2"),
        col("f.carbon_monoxide").alias("co"),
        col("f.ozone").alias("o3"),

        col("f.us_aqi").alias("aqi"),
        col("f.us_aqi_index").alias("aqi_us"),
        col("f.european_aqi").alias("aqi_eu"),
        col("f.aqi_label").alias("aqi_category"),

        col("f.data_source"),
        col("f.processing_time").alias("created_at")
    ).dropDuplicates()

# ==============================
# WRITE TO ICEBERG (DWH)
# ==============================
def write_gold(df, table_name):
    df.writeTo(table_name) \
      .tableProperty("format-version", "2") \
      .tableProperty("write.parquet.compression-codec", "snappy") \
      .createOrReplace()

write_gold(dim_date, "iceberg.dwh.dim_date")
write_gold(dim_location, "iceberg.dwh.dim_location")
write_gold(dim_weather, "iceberg.dwh.dim_weather")
write_gold(dim_wind, "iceberg.dwh.dim_wind")
write_gold(fact, "iceberg.dwh.fact_aqi")
# ==============================    
# DONE
# ==============================
spark.stop()
print("ELT Completed SUCCESSFULLY")