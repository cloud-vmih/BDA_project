from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, lit, current_timestamp

spark = SparkSession.builder \
    .appName("predict") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://23133083thuyvan-master:9083") \
    .config("spark.sql.catalog.iceberg.warehouse", "hdfs://23133083thuyvan-master:9000/user/hive/lakehouse") \
    .getOrCreate()

# Đọc từ  Gold
print(f"Đọc dữ liệu Gold từ iceberg.air_quality_ml.air_quality_gold")
df = spark.read \
    .format("iceberg") \
    .load("iceberg.air_quality_ml.air_quality_features") \
    .filter(col("ingestion_time") > current_timestamp() - lit(1 * 24 * 60 * 60)) # chỉ predict dữ liệu mới trong 1 ngày gần nhất

# sử dụng model mới nhất
model = PipelineModel.load("file:///home/spark/spark/ML")
predictions = model.transform(df)

table_pred = "iceberg.air_quality_ml.air_quality_predictions"
if not spark.catalog.tableExists(table_pred):
    print(f"Tạo Iceberg table mới: {table_pred}")
    predictions.writeTo(table_pred) \
        .tableProperty("format-version", "2") \
        .create()
    print(f"Bảng {table_pred} đã được tạo và dữ liệu đã được ghi.")
else:
    print(f"Bảng đã tồn tại, append dữ liệu vào {table_pred}")
    predictions.writeTo(table_pred) \
        .tableProperty("format-version", "2") \
        .append()
    print(f"Dữ liệu đã được append vào {table_pred}.")

spark.stop() 