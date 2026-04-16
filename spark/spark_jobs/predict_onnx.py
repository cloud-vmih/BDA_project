from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, current_timestamp, pandas_udf
from pyspark.sql.types import DoubleType
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
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
    .filter(col("ingestion_time") > current_timestamp() - expr("INTERVAL 1 DAY")) 

print(f"Số lượng records cần predict: {df.count()}")
if df.count() == 0:
    print("Không có dữ liệu mới để predict. Kết thúc.")
    spark.stop()
    exit(0)

# 3. LOAD ONNX MODEL
model_path = "/home/spark/spark/ML/air_quality_model.onnx"
feature_columns = ["pm2_5", "pm10", "co", "no2", "so2", "o3"]

# Kiểm tra model tồn tại
if not os.path.exists(model_path):
    raise FileNotFoundError(f"ONNX model not found at {model_path}")

sample_session = rt.InferenceSession(model_path)
input_names = [inp.name for inp in sample_session.get_inputs()]
print("Model input names:", input_names)

# 4. TẠO PANDAS UDF ĐỂ PREDICT BATCH
@pandas_udf(returnType=DoubleType())
def predict_onnx_udf(*feature_cols):
    """
    Pandas UDF để predict batch bằng ONNX model
    feature_cols: các cột features (pm2_5, pm10, co, no2, so2, o3)
    """
    onnx_session = rt.InferenceSession(model_path)

    input_names = [inp.name for inp in onnx_session.get_inputs()]

    # Kết hợp các cột thành numpy array
    feed = {}
    for i, name in enumerate(input_names):
        # Lấy Series tương ứng theo thứ tự: pm2_5, pm10, co, no2, so2, o3
        series = [col for col in feature_cols][i]
        # Reshape thành cột (N,1)
        feed[name] = series.values.reshape(-1, 1).astype(np.float32)
    preds = onnx_session.run(None, feed)[0].flatten()
    return pd.Series(preds)

# 5. PREDICT BẰNG ONNX MODEL
print("Đang predict bằng ONNX model...")

# Áp dụng UDF để predict
df_with_pred = df.withColumn(
    "y_pred", 
    predict_onnx_udf(*feature_columns)
)

evaluator = MulticlassClassificationEvaluator(
    labelCol="AQI",   # cột nhãn thực tế (0,1,2,...)
    predictionCol="y_pred", # cột dự đoán từ mô hình
    metricName="accuracy"      
)

accuracy = evaluator.evaluate(df_with_pred)
print(f"Accuracy: {accuracy}")

evaluator.setMetricName("f1")
f1 = evaluator.evaluate(df_with_pred)
print(f"Weighted F1 = {f1:.4f}")


# Thêm các cột metadata
df_with_pred = df_with_pred.withColumn("y_true", col("AQI"))
df_with_pred = df_with_pred.withColumn("prediction_timestamp", current_timestamp())
df_with_pred = df_with_pred.withColumn("model_type", lit("ONNX_RF"))
df_with_pred = df_with_pred.withColumn("accuracy", lit(accuracy))
df_with_pred = df_with_pred.withColumn("f1", lit(f1))


print("Predict completed!")
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
    lit(accuracy).alias("accuracy"),
    lit(f1).alias("f1"),
    col("prediction_timestamp"),
    col("model_type")
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
print(f"Model: ONNX_GBT")
print(f"Features used: {', '.join(feature_columns)}")

# 11. CLEANUP
spark.stop()
print("\nSpark session stopped. Predict completed successfully!")