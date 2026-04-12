from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType, TimestampType
import time

spark = SparkSession.builder \
    .appName("Weather_Kafka_to_Iceberg") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://23133083thuyvan-master:9083") \
    .config("spark.sql.catalog.iceberg.warehouse", "hdfs://23133083thuyvan-master:9000/user/hive/lakehouse") \
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
    .getOrCreate()

print("Spark + Iceberg ready!")

# Tạo schema phù hợp với data từ Open-Meteo
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("location", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timezone", StringType(), True),
    StructField("temperature_2m", DoubleType(), True),
    StructField("relative_humidity_2m", DoubleType(), True),
    StructField("apparent_temperature", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("cloud_cover", DoubleType(), True),
    StructField("wind_speed_10m", DoubleType(), True),
    StructField("wind_direction_10m", DoubleType(), True),
    StructField("wind_gusts_10m", DoubleType(), True),
    StructField("uv_index", DoubleType(), True),
    StructField("weather_code", IntegerType(), True),
    StructField("european_aqi", IntegerType(), True),
    StructField("us_aqi", IntegerType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("pm2_5", DoubleType(), True),
    StructField("carbon_monoxide", DoubleType(), True),
    StructField("nitrogen_dioxide", DoubleType(), True),
    StructField("sulphur_dioxide", DoubleType(), True),
    StructField("ozone", DoubleType(), True)
])

# Tạo table Iceberg
spark.sql("CREATE SCHEMA IF NOT EXISTS iceberg.air_quality_db")
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.air_quality_db.air_quality_silver (
        timestamp TIMESTAMP,
        location STRING,
        latitude DOUBLE,
        longitude DOUBLE,
        timezone STRING,
        temperature_2m DOUBLE,
        relative_humidity_2m DOUBLE,
        apparent_temperature DOUBLE,
        precipitation DOUBLE,
        cloud_cover DOUBLE,
        wind_speed_10m DOUBLE,
        wind_direction_10m DOUBLE,
        wind_gusts_10m DOUBLE,
        uv_index DOUBLE,
        weather_code INT,
        european_aqi INT,
        us_aqi INT,
        us_aqi_index INT,
        aqi_label STRING,
        pm10 DOUBLE,
        pm2_5 DOUBLE,
        carbon_monoxide DOUBLE,
        nitrogen_dioxide DOUBLE,
        sulphur_dioxide DOUBLE,
        ozone DOUBLE,
        processing_time TIMESTAMP,
        data_source STRING
    ) USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")
print("Iceberg table 'weather_real_time' ready!")

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather-topic") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df_stream \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema).alias("data")) \
    .select("data.*") \
    .withColumn("processing_time", current_timestamp()) \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("data_source", lit("open-meteo"))

def write_to_bronze_and_silver(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.cache()

        # --- 1. Ghi vào Bronze (Dạng file Parquet) ---
        # Dữ liệu sẽ được tổ chức theo thư mục ngày/tháng/năm để dễ quản lý
        batch_df.write \
            .format("parquet") \
            .mode("append") \
            .save("hdfs://23133083thuyvan-master:9000/user/hive/lakehouse/bronze/raw_api")

        # Lấy SparkSession hiện tại để dùng cho phần ELT tiếp theo
        current_spark_session = batch_df.sparkSession
        # --- 2. Xử lý ELT sang Silver ---
        # A. Lọc bỏ (dropna) nếu thiếu các chỉ số không khí quan trọng
        # Nếu trạm đo không trả về PM2.5 hoặc AQI thì bản ghi đó không đáng tin cậy
        air_quality_cols = ["pm2_5", "pm10", "us_aqi", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone", "european_aqi"]
        silver_df = batch_df.dropna(subset=air_quality_cols)

        # B. Chuyển đổi us_aqi (0-500) sang thang đo 1-6 (AQI Index)
        # Thang đo phổ biến: 1: Tốt, 2: TB, 3: Kém cho người nhạy cảm, 4: Xấu, 5: Rất xấu, 6: Nguy hại
        silver_df = silver_df.withColumn("us_aqi_index", 
            when(col("us_aqi") <= 50, 1)        # Good
            .when(col("us_aqi") <= 100, 2)      # Moderate
            .when(col("us_aqi") <= 150, 3)      # Unhealthy for Sensitive Groups
            .when(col("us_aqi") <= 200, 4)      # Unhealthy
            .when(col("us_aqi") <= 300, 5)      # Very Unhealthy
            .otherwise(6)                       # Hazardous
        )

        # C. Thêm nhãn mô tả cho thang đo để sau này làm Dashboard cho dễ
        silver_df = silver_df.withColumn("aqi_label", 
            when(col("us_aqi_index") == 1, "Tốt")
            .when(col("us_aqi_index") == 2, "Trung bình")
            .when(col("us_aqi_index") == 3, "Kém (nhạy cảm)")
            .when(col("us_aqi_index") == 4, "Xấu")
            .when(col("us_aqi_index") == 5, "Rất xấu")
            .otherwise("Nguy hại")
        )
        silver_df = silver_df.withColumn("timestamp", 
            window(col("timestamp"), "15 minutes").start
        )
        # Tạo view tạm cho Batch hiện tại
        silver_df.createOrReplaceTempView("current_batch")

        # Dùng MERGE INTO để chỉ nạp những dòng thực sự mới
        current_spark_session.sql("""
            MERGE INTO iceberg.air_quality_db.air_quality_silver a
            USING current_batch s
            ON a.location = s.location AND a.timestamp = s.timestamp
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        batch_df.unpersist()

# Ghi vào Iceberg
query = parsed_df.writeStream \
    .foreachBatch(write_to_bronze_and_silver) \
    .option("checkpointLocation", "hdfs://23133083thuyvan-master:9000/user/hive/checkpoints/air_quality_realtime") \
    .trigger(processingTime="30 seconds") \
    .start()

print("Streaming job started! Listening to 'weather-topic'...")
query.awaitTermination()