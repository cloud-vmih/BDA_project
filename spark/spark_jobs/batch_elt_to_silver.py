from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import max as spark_max

# Cấu hình đường dẫn
start_date = datetime.now()
BRONZE_PATH = f"hdfs://23133083thuyvan-master:9000/user/hive/lakehouse/bronze/raw_kaggle/{start_date.strftime('%Y-%m-%d')}/GlobalWeatherRepository.csv"
TABLE_NAME = "iceberg.air_quality_db.air_quality_silver"

spark = SparkSession.builder \
    .appName("ETL_Kaggle_to_Silver") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://23133083thuyvan-master:9083") \
    .config("spark.sql.catalog.iceberg.warehouse", "hdfs://23133083thuyvan-master:9000/user/hive/lakehouse") \
    .getOrCreate()

print("Driver Memory:", spark.conf.get("spark.driver.memory"))
print("Executor Memory:", spark.conf.get("spark.executor.memory"))
print("Shuffle Partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

# 1. Lấy Max Timestamp TRƯỚC khi đọc dữ liệu nặng để tránh chiếm dụng RAM sớm
max_time = None
if spark.catalog.tableExists(TABLE_NAME):
    max_time = spark.table(TABLE_NAME) \
        .filter(col("data_source") == "kaggle") \
        .select(spark_max("timestamp")) \
        .collect()[0][0]
    print(f"Max timestamp nguồn Kaggle: {max_time}")

# 2. Đọc dữ liệu (Chỉ chọn các cột cần thiết ngay từ đầu để tiết kiệm RAM)
df = spark.read.option("header", "true").option("inferSchema", "true").csv(BRONZE_PATH)

# Chuyển đổi timestamp sớm để filter
df = df.withColumn("last_updated", col("last_updated").cast("timestamp"))

if max_time:
    df = df.filter(col("last_updated") > lit(max_time))

# 3. Mapping cột & Ép kiểu trong 1 lần Select duy nhất (Giảm overhead)
# Cách này giúp Spark không phải tạo nhiều DataFrame trung gian
df_clean = df.select(
    col("last_updated").alias("timestamp"),
    col("location_name").alias("location"),
    col("latitude").cast("double"),
    col("longitude").cast("double"),
    col("country"),
    col("condition_text"),
    col("timezone"),
    col("temperature_celsius").cast("double").alias("temperature_2m"),
    col("humidity").cast("double").alias("relative_humidity_2m"),
    col("feels_like_celsius").cast("double").alias("apparent_temperature"),
    col("precip_mm").cast("double").alias("precipitation"),
    col("cloud").cast("double").alias("cloud_cover"),
    col("wind_kph").cast("double").alias("wind_speed_10m"),
    col("wind_degree").cast("double").alias("wind_direction_10m"),
    col("gust_kph").cast("double").alias("wind_gusts_10m"),
    col("uv_index").cast("double"),
    lit(1).alias("weather_code"),
    col("air_quality_gb-defra-index").cast("int").alias("european_aqi"),
    col("air_quality_us-epa-index").cast("int").alias("us_aqi"),
    col("air_quality_us-epa-index").cast("int").alias("us_aqi_index"),
    col("air_quality_PM10").cast("double").alias("pm10"),
    col("`air_quality_PM2.5`").cast("double").alias("pm2_5"),
    col("air_quality_Carbon_Monoxide").cast("double").alias("carbon_monoxide"),
    col("air_quality_Nitrogen_dioxide").cast("double").alias("nitrogen_dioxide"),
    col("air_quality_Sulphur_dioxide").cast("double").alias("sulphur_dioxide"),
    col("air_quality_Ozone").cast("double").alias("ozone"),
    current_timestamp().alias("processing_time"),
    lit("kaggle").alias("data_source")
)

# 4. Clean & Thêm nhãn (Kết hợp filter để loại bỏ rác sớm)
aqi_map = {1: "Tốt", 2: "Trung bình", 3: "Kém (nhạy cảm)", 4: "Xấu", 5: "Rất xấu"}
label_logic = when(col("us_aqi_index") == 1, aqi_map[1])
for k, v in list(aqi_map.items())[1:]:
    label_logic = label_logic.when(col("us_aqi_index") == k, v)
label_logic = label_logic.otherwise("Nguy hại")

thresholds = {
    "pm2_5": 1000.0,
    "pm10": 1500.0,
    "wind_speed_10m": 250.0, # Ép ngưỡng gió về mức thực tế (km/h)
    "wind_gusts_10m": 350.0,
    "temperature_2m": 60.0,   # Nhiệt độ không quá 60 độ C
    "relative_humidity_2m": 100.0,
    "carbon_monoxide": 4000.0,
    "nitrogen_dioxide": 500.0,
    "sulphur_dioxide": 500.0,
    "ozone": 500.0,
    "cloud_cover": 100.0,
}

# Áp dụng lọc trước khi ghi vào Silver
df_clean = df_clean.dropna(subset=["pm2_5", "pm10", "us_aqi_index", "sulphur_dioxide", "ozone", "carbon_monoxide"])

# 1. Lọc giá trị âm và các giá trị cực đoan phi lý (Outliers)
df_clean = df_clean.filter(
    (col("pm2_5").between(0, thresholds["pm2_5"])) &
    (col("pm10").between(0, thresholds["pm10"])) &
    (col("us_aqi_index").between(0, 6)) & # AQI Index chỉ có 6 mức
    (col("wind_speed_10m").between(0, thresholds["wind_speed_10m"])) &
    (col("temperature_2m").between(-50, thresholds["temperature_2m"])) &
    (col("relative_humidity_2m").between(0, thresholds["relative_humidity_2m"])) &
    (col("carbon_monoxide").between(0, thresholds["carbon_monoxide"])) &
    (col("nitrogen_dioxide").between(0, thresholds["nitrogen_dioxide"])) &
    (col("sulphur_dioxide").between(0, thresholds["sulphur_dioxide"])) &
    (col("ozone").between(0, thresholds["ozone"])) &
    (col("cloud_cover").between(0, thresholds["cloud_cover"]))
)

# 2. Thêm nhãn và loại trùng như cũ
df_clean = df_clean.withColumn("aqi_label", label_logic) \
    .dropDuplicates(["timestamp", "location"])

# 5. Đảm bảo Schema chuẩn cuối cùng
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

# 6. Ghi dữ liệu
if not spark.catalog.tableExists(TABLE_NAME):
    # Tạo Schema nếu chưa có
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS iceberg.air_quality_db LOCATION 'hdfs://23133083thuyvan-master:9000/user/hive/lakehouse/silver/'")
    
    df_clean.writeTo(TABLE_NAME) \
        .tableProperty("format-version", "2") \
        .partitionedBy(days("timestamp")) \
        .create()
else:
    df_clean.sortWithinPartitions("timestamp") \
        .writeTo(TABLE_NAME) \
        .append()

spark.stop()