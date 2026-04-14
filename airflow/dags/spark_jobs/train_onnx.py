from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col, lit, current_timestamp, round as spark_round
from pyspark.sql.types import DoubleType
import time
import numpy as np
import pandas as pd


# 1. KHỞI TẠO SPARK SESSION
spark = SparkSession.builder \
    .appName("TrainModel") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://23133083thuyvan-master:9083") \
    .config("spark.sql.catalog.iceberg.warehouse", "hdfs://23133083thuyvan-master:9000/user/hive/lakehouse") \
    .config("ONNX_DFS_PATH", "hdfs://23133083thuyvan-master:9000/tmp/onnx") \
    .getOrCreate()

# 2. ĐỌC DỮ LIỆU
print(f"Đọc dữ liệu Gold từ iceberg.air_quality_ml.air_quality_features")
df = spark.read \
    .format("iceberg") \
    .load("iceberg.air_quality_ml.air_quality_features")

features = ["pm2_5", "pm10", "co", "no2", "so2", "o3"]

if df.count() <= 20:
    print("Dữ liệu quá ít để train model, dừng lại.")
    spark.stop()
    exit(1)

# 3. HUẤN LUYỆN MODEL
assembler = VectorAssembler(inputCols=features, outputCol="features")

model = GBTRegressor(
    featuresCol="features",
    labelCol="AQI",
    maxIter=30,
    maxDepth=5,
    maxBins=32
)

pipeline = Pipeline(stages=[assembler, model])

# Split
train, test = df.randomSplit([0.8, 0.2], seed=42)
train.writeTo("iceberg.air_quality_ml.air_quality_train").createOrReplace()
test.writeTo("iceberg.air_quality_ml.air_quality_test").createOrReplace()

# Train
pipeline_model = pipeline.fit(train)

# 4. CHUYỂN ĐỔI SANG ONNX
print("Chuyển đổi model sang ONNX...")
import onnxmltools
from onnxmltools.convert.common.data_types import FloatTensorType
import onnxruntime as rt

gbt_model = pipeline_model.stages[1]

# Tạo initial types cho ONNX với tên feature columns
initial_types = [("features", FloatTensorType([None, len(features)]))]

# Chuyển đổi GBT model (không phải pipeline) sang ONNX
onnx_model = onnxmltools.convert_sparkml(
    model=gbt_model,
    name="Air_Quality_GBT_Model",
    initial_types=initial_types,
    spark_session=spark
)
print("ONNX conversion successful!")

# Lưu ONNX model
model_path = "/home/spark/spark/ML/air_quality_model.onnx"
onnxmltools.utils.save_model(onnx_model, model_path)
print(f"ONNX Model saved at {model_path}")
onnx_available = True
# except Exception as e:
#     print(f"ONNX conversion failed: {e}")
#     print("Falling back to Spark ML model")
#     onnx_available = False
#     # Lưu Spark model làm backup
#     spark_model_path = "file:///home/spark/spark/ML/spark_model"
#     pipeline_model.write().overwrite().save(spark_model_path)
#     print(f"Spark ML model saved at {spark_model_path}")

# 5. PREDICT BẰNG ONNX
print("Predict bằng ONNX runtime...")
@pandas_udf(returnType=DoubleType())
def predict_onnx_udf(pm2_5: pd.Series, pm10: pd.Series, co: pd.Series,
                     no2: pd.Series, so2: pd.Series, o3: pd.Series) -> pd.Series:
    # Mỗi executor sẽ tạo session riêng khi hàm được gọi
    sess = rt.InferenceSession(model_path)
    input_name = sess.get_inputs()[0].name
    output_name = sess.get_outputs()[0].name
    
    # Gom các cột thành feature matrix
    features = np.column_stack([pm2_5, pm10, co, no2, so2, o3]).astype(np.float32)
    preds = sess.run([output_name], {input_name: features})[0]
    return pd.Series(preds.flatten())

# Áp dụng UDF
test_with_pred = test.withColumn(
    "prediction",
    predict_onnx_udf(col("pm2_5"), col("pm10"), col("co"), col("no2"), col("so2"), col("o3"))
)

# 6. TÍNH TOÁN METRICS
evaluator = RegressionEvaluator(
    labelCol="AQI",
    predictionCol="prediction",
    metricName="rmse"
)

rmse = evaluator.evaluate(test_with_pred)
print(f"RMSE: {rmse}")

# 7. LƯU VÀO GOLD TABLE (features + y_true + y_pred)
print("Lưu kết quả vào gold table...")

# Chuẩn bị gold table data
gold_table_data = test_with_pred.select(
    col("timestamp"),
    col("pm2_5"),
    col("pm10"),
    col("co"),
    col("no2"),
    col("so2"),
    col("o3"),
    col("AQI").alias("y_true"),
    col("prediction").alias("y_pred"),
    lit(rmse).alias("rmse"),
    current_timestamp().alias("prediction_timestamp"),
    lit("ONNX_GBT").alias("model_type")
)

# Lưu vào gold table
table_gold = "iceberg.air_quality_ml.air_quality_gold_predictions"

if not spark.catalog.tableExists(table_gold):
    print(f"Tạo Iceberg gold table mới: {table_gold}")
    gold_table_data.writeTo(table_gold) \
        .tableProperty("format-version", "2") \
        .create()
    print(f"Bảng {table_gold} đã được tạo và dữ liệu đã được ghi.")
else:
    print(f"Bảng gold đã tồn tại, append dữ liệu vào {table_gold}")
    gold_table_data.writeTo(table_gold) \
        .tableProperty("format-version", "2") \
        .append()
    print(f"Dữ liệu đã được append vào {table_gold}.")

# 8. HIỂN THỊ KẾT QUẢ
print("\n=== Sample predictions ===")
test_with_pred.select("AQI", "prediction", "pm2_5", "pm10").show(10)

print(f"\nFinal RMSE: {rmse:.4f} ")

# 10. CLEANUP
spark.stop()