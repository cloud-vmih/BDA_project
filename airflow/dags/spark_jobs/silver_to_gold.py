from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofweek, greatest

spark = SparkSession.builder \
    .appName("Silver_to_Gold") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://23133083thuyvan-master:9083") \
    .config("spark.sql.catalog.iceberg.warehouse", "hdfs://23133083thuyvan-master:9000/user/hive/lakehouse") \
    .getOrCreate()

# Đọc từ Silver
print(f"Đọc dữ liệu Silver từ iceberg.air_quality_db.air_quality_silver")
df = spark.read \
    .format("iceberg") \
    .load("iceberg.air_quality_db.air_quality_silver")

df_gold = df.select(
    "timestamp",
    "pm2_5", "pm10", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone",
    "us_aqi_index"
).withColumn(
    "hour", hour("timestamp")
).withColumn(
    "day_of_week", dayofweek("timestamp")
)

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS iceberg.air_quality_ml
    LOCATION 'hdfs://23133083thuyvan-master:9000/user/hive/lakehouse/gold/ml'
""")

# Ghi vào Gold (Iceberg)
table_name = "iceberg.air_quality_ml.air_quality_gold"
if not spark.catalog.tableExists(table_name):
    print(f"Tạo Iceberg table mới: {table_name}")
    df_gold.writeTo(table_name) \
        .tableProperty("format-version", "2") \
        .create()
    print(f"Bảng {table_name} đã được tạo và dữ liệu đã được ghi.")
else:
    print(f"Bảng đã tồn tại, append dữ liệu vào {table_name}")
    df_gold.writeTo(table_name) \
        .tableProperty("format-version", "2") \
        .append()   
    print(f"Dữ liệu đã được append vào {table_name}.")
print("ETL from Silver to Gold completed!")

df_gold.cache()
df_gold.count()

spark.stop()