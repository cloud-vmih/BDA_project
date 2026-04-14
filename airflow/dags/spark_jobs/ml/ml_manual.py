from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofweek, greatest
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

# khởi tạo spark
spark = SparkSession.builder \
    .appName("ML_manual") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://23133083thuyvan-master:9083") \
    .config("spark.sql.catalog.iceberg.warehouse", "hdfs://23133083thuyvan-master:9000/user/hive/lakehouse") \
    .getOrCreate()

# Đọc từ Silver
print(f"Đọc dữ liệu Silver từ iceberg.air_quality_db.air_quality_silver")
df_silver = spark.read \
    .format("iceberg") \
    .load("iceberg.air_quality_db.air_quality_silver")

df_gold = df_silver.select(
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

features = [
    "pm2_5", "pm10", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone"
    ]

assembler = VectorAssembler(inputCols=features, outputCol="features")
model = RandomForestRegressor(
    featuresCol="features",
    labelCol="us_aqi_index",
    numTrees=50,
    maxDepth=8,
    maxBins=32
)
pipeline = Pipeline(stages=[assembler, model])

# SPLIT DATA
train, test = df_gold.randomSplit([0.8, 0.2], seed=42)
train_table = "iceberg.air_quality_ml.air_quality_train"
if not spark.catalog.tableExists(train_table):
    print(f"Tạo Iceberg table mới: {train_table}")
    train.writeTo(train_table) \
        .tableProperty("format-version", "2") \
        .create()
    print(f"Bảng {train_table} đã được tạo và dữ liệu đã được ghi.")
else:
    print(f"Bảng đã tồn tại, append dữ liệu vào {train_table}")
    train.writeTo(train_table) \
        .tableProperty("format-version", "2") \
        .append()
    print(f"Dữ liệu đã được append vào {train_table}.")

test_table = "iceberg.air_quality_ml.air_quality_test"
if not spark.catalog.tableExists(test_table):
    print(f"Tạo Iceberg table mới: {test_table}")
    test.writeTo(test_table) \
        .tableProperty("format-version", "2") \
        .create()
    print(f"Bảng {test_table} đã được tạo và dữ liệu đã được ghi.")
else:
    print(f"Bảng đã tồn tại, append dữ liệu vào {test_table}")
    test.writeTo(test_table) \
        .tableProperty("format-version", "2") \
        .append()
    print(f"Dữ liệu đã được append vào {test_table}.")

# TRAIN MODEL
pipeline_model = pipeline.fit(train)

# predict
predictions = pipeline_model.transform(test)

# EVALUATE
evaluator = RegressionEvaluator(
    labelCol="us_aqi_index",
    predictionCol="prediction",
    metricName="rmse"
)
rmse = evaluator.evaluate(predictions)
print("RMSE:", rmse)

# lưu model trên local
model_path = "file:///home/spark/spark/ML"
pipeline_model.write().overwrite().save(model_path)
print(f"Model saved at {model_path}")

# lưu vào database
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

# anomaly prediction
predictions_with_error = predictions.withColumn(
    "residual",
    abs(predictions["us_aqi_index"] - predictions["prediction"])
)

# detect anomaly
anomalies = predictions_with_error.filter("residual > 20")

# lưu file dự đoán cuối cùng
table_name = "iceberg.air_quality_ml.air_quality_predictions_final"
if not spark.catalog.tableExists(table_name):
    print(f"Tạo Iceberg table mới: {table_name}")
    predictions_with_error.writeTo(table_name) \
        .tableProperty("format-version", "2") \
        .create()
    print(f"Bảng {table_name} đã được tạo và dữ liệu đã được ghi.")
else:
    print(f"Bảng đã tồn tại, append dữ liệu vào {table_name}")
    predictions_with_error.writeTo(table_name) \
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

df_gold.unpersist()
train.unpersist()
test.unpersist()

spark.stop()