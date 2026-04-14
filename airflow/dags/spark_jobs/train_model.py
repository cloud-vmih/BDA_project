from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col
import time

spark = SparkSession.builder \
    .appName("TrainModel") \
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
    .load("iceberg.air_quality_ml.air_quality_gold")

features = [
    "pm2_5", "pm10", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone"
    ]

assembler = VectorAssembler(inputCols=features, outputCol="features")

model = GBTRegressor(
    featuresCol="features",
    labelCol="us_aqi_index",
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

# Predict
predictions = pipeline_model.transform(test)

# Evaluator
evaluator = RegressionEvaluator(
    labelCol="us_aqi_index",
    predictionCol="prediction",
    metricName="rmse"
)

rmse = evaluator.evaluate(predictions)
print(f"RMSE: {rmse}")

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

spark.stop()    
