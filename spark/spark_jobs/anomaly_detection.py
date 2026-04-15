from pyspark.sql import SparkSession
from pyspark.sql.functions import abs, col, max as spark_max

spark = SparkSession.builder \
    .appName("anomaly_detection") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://23133083thuyvan-master:9083") \
    .config("spark.sql.catalog.iceberg.warehouse", "hdfs://23133083thuyvan-master:9000/user/hive/lakehouse") \
    .getOrCreate()

table_name = "iceberg.air_quality_ml.air_quality_gold_predictions"
if not spark.catalog.tableExists(table_name):
    print(f"Bảng {table_name} không tồn tại. Vui lòng chạy các bước trước để tạo bảng và ghi dữ liệu.")
    spark.stop()
    exit(1)

table_anomalies = "iceberg.air_quality_ml.air_quality_anomalies"
max_time = "1970-01-01 00:00:00"
if spark.catalog.tableExists(table_anomalies):
    max_time = spark.table(table_anomalies) \
        .select(spark_max("prediction_timestamp")) \
        .collect()[0][0]
    print(f"Max prediction_timestamp trong bảng anomalies: {max_time}")

# Đọc từ  Gold
print(f"Đọc dữ liệu Gold từ iceberg.air_quality_ml.air_quality_gold_predictions")
df = spark.read \
    .format("iceberg") \
    .load(table_name) \
    .filter(col("prediction_timestamp") > max_time)

predictions_with_error = df.withColumn(
    "residual",
    abs(df["y_true"] - df["y_pred"])
)

# detect anomaly
anomalies = predictions_with_error.filter("residual >= 2")

# lưu file phát hiện bất thường
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