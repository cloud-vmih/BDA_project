from pyspark.sql import SparkSession
from pyspark.sql.functions import abs

spark = SparkSession.builder \
    .appName("anomaly_detection") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://23133083thuyvan-master:9083") \
    .config("spark.sql.catalog.iceberg.warehouse", "hdfs://23133083thuyvan-master:9000/user/hive/lakehouse") \
    .getOrCreate()

# Đọc từ  Gold
print(f"Đọc dữ liệu Gold từ iceberg.air_quality_ml.air_quality_predictions")
df = spark.read \
    .format("iceberg") \
    .load("iceberg.air_quality_ml.air_quality_predictions")

predictions_with_error = df.withColumn(
    "residual",
    abs(df["us_aqi_index"] - df["prediction"])
)

# detect anomaly
anomalies = predictions_with_error.filter("residual > 20")

# lưu file dự đoán cuối cùng
table_name = "iceberg.air_quality_ml.air_quality_predictions_final"
if not spark.catalog.tableExists(table_name):
    print(f"Tạo Iceberg table mới: {table_name}")
    df.writeTo(table_name) \
        .tableProperty("format-version", "2") \
        .create()
    print(f"Bảng {table_name} đã được tạo và dữ liệu đã được ghi.")
else:
    print(f"Bảng đã tồn tại, append dữ liệu vào {table_name}")
    df.writeTo(table_name) \
        .tableProperty("format-version", "2") \
        .append()   
    print(f"Dữ liệu đã được append vào {table_name}.")

# lưu file phát hiện bất thường
table_anomalies = "iceberg.air_quality_ml.air_quality_anomalies"
if not spark.catalog.tableExists(table_anomalies):
    print(f"Tạo Iceberg table mới: {table_anomalies}")
    anomalies.writeTo(table_anomalies) \
        .tableProperty("format-version", "2") \
        .create()
    print(f"Bảng {table_anomalies} đã được tạo và dữ liệu đã được ghi.")
else:
    print(f"Bảng đã tồn tại, append dữ liệu vào {table_anomalies}")
    anomalies.writeTo(table_anomalies) \
        .tableProperty("format-version", "2") \
        .append()   
    print(f"Dữ liệu đã được append vào {table_anomalies}.")