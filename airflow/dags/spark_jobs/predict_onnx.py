from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, current_timestamp, pandas_udf
from pyspark.sql.types import DoubleType
import numpy as np
import pandas as pd
import onnxruntime as rt
import os


# 1. KHỞI TẠO SPARK SESSION
spark = SparkSession.builder \
    .appName("predict") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://23133083thuyvan-master:9083") \
    .config("spark.sql.catalog.iceberg.warehouse", "hdfs://23133083thuyvan-master:9000/user/hive/lakehouse") \
    .getOrCreate()


# 2. ĐỌC DỮ LIỆU CẦN PREDICT
print(f"Đọc dữ liệu từ iceberg.air_quality_ml.air_quality_features")
df = spark.read \
    .format("iceberg") \
    .load("iceberg.air_quality_ml.air_quality_features") \
    .filter(col("ingestion_time") > current_timestamp() - expr("INTERVAL 1 DAY")) \
    .limit(1000)  # Giới hạn số lượng records để predict (có thể điều chỉnh hoặc bỏ limit)

print(f"Số lượng records cần predict: {df.count()}")

# 3. LOAD ONNX MODEL
model_path = "/home/spark/spark/ML/air_quality_model.onnx"
feature_columns = ["pm2_5", "pm10", "co", "no2", "so2", "o3"]

# Kiểm tra model tồn tại
if not os.path.exists(model_path):
    raise FileNotFoundError(f"ONNX model not found at {model_path}")


# 4. TẠO PANDAS UDF ĐỂ PREDICT BATCH
@pandas_udf(returnType=DoubleType())
def predict_onnx_udf(*feature_cols):
    """
    Pandas UDF để predict batch bằng ONNX model
    feature_cols: các cột features (pm2_5, pm10, co, no2, so2, o3)
    """
    onnx_session = rt.InferenceSession(model_path)
    input_name = onnx_session.get_inputs()[0].name
    output_name = onnx_session.get_outputs()[0].name

    print(f"ONNX model loaded successfully")
    print(f"Input name: {input_name}, Output name: {output_name}")

    # Kết hợp các cột thành numpy array
    feature_arrays = []
    for i in range(len(feature_cols[0])):
        row_features = [col[i] for col in feature_cols]
        feature_arrays.append(row_features)
    
    # Chuyển thành numpy array
    input_data = np.array(feature_arrays, dtype=np.float32)
    
    # Predict batch
    predictions = onnx_session.run([output_name], {input_name: input_data})[0]
    
    # Trả về pandas series
    return pd.Series(predictions.flatten())

# 5. PREDICT BẰNG ONNX MODEL
print("Đang predict bằng ONNX model...")

# Áp dụng UDF để predict
df_with_pred = df.withColumn(
    "y_pred", 
    predict_onnx_udf(*feature_columns)
)

# Thêm các cột metadata
df_with_pred = df_with_pred.withColumn("y_true", col("AQI"))
df_with_pred = df_with_pred.withColumn("prediction_timestamp", current_timestamp())
df_with_pred = df_with_pred.withColumn("model_type", lit("ONNX_GBT"))


print("Predict completed!")

# 6. TÍNH TOÁN METRICS
from pyspark.sql.functions import sqrt, avg, pow, col as spark_col

# Tính RMSE
rmse = df_with_pred.select(
    sqrt(avg(pow(spark_col("y_true") - spark_col("y_pred"), 2))).alias("rmse")
).collect()[0]["rmse"]

print(f"RMSE: {rmse:.4f}")

# 7. LƯU VÀO GOLD TABLE
# Chuẩn bị gold table (chỉ lấy các cột cần thiết)
gold_table_data = df_with_pred.select(
    col("timestamp"),
    col("pm2_5"),
    col("pm10"),
    col("co"),
    col("no2"),
    col("so2"),
    col("o3"),
    col("y_true"),
    col("y_pred"),
    col("prediction_timestamp"),
    col("model_type"),
    lit(rmse).alias("rmse")  # Thêm RMSE vào mỗi row
)

# Tên bảng gold
table_gold = "iceberg.air_quality_ml.air_quality_gold_predictions"

# Ghi vào bảng gold
if not spark.catalog.tableExists(table_gold):
    print(f"Tạo Iceberg gold table mới: {table_gold}")
    gold_table_data.writeTo(table_gold) \
        .tableProperty("format-version", "2") \
        .tableProperty("write.format.default", "parquet") \
        .create()
    print(f"Bảng {table_gold} đã được tạo và dữ liệu đã được ghi.")
else:
    print(f"Bảng gold đã tồn tại, append dữ liệu vào {table_gold}")
    gold_table_data.writeTo(table_gold) \
        .tableProperty("format-version", "2") \
        .append()
    print(f"Dữ liệu đã được append vào {table_gold}.")


# 8. HIỂN THỊ KẾT QUẢ MẪU
print("\n=== Sample predictions (first 10 rows) ===")
df_with_pred.select("pm2_5", "pm10", "y_true", "y_pred").show(10)

print(f"\n=== Prediction Summary ===")
print(f"Total predictions: {df_with_pred.count()}")
print(f"RMSE: {rmse:.4f}")
print(f"Model: ONNX_GBT")
print(f"Features used: {', '.join(feature_columns)}")

# 9. THỐNG KÊ NHANH
print("\n=== Prediction Statistics ===")
df_with_pred.select(
    avg("y_true").alias("avg_true"),
    avg("y_pred").alias("avg_pred"),
    (avg("y_true") - avg("y_pred")).alias("avg_bias")
).show()

# 11. CLEANUP
spark.stop()
print("\nSpark session stopped. Predict completed successfully!")