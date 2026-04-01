from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
import time

# TẠO SPARK SESSION VỚI ICEBERG
spark = SparkSession.builder \
    .appName("Test_Kafka_Spark_Iceberg") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://23133083thuyvan-master:9083") \
    .config("spark.sql.catalog.iceberg.warehouse", "hdfs://23133083thuyvan-master:9000/user/hive/lakehouse") \
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
    .getOrCreate()

print("SparkSession with Iceberg created successfully!")

# TẠO SCHEMA VÀ TABLE NẾU CHƯA CÓ 
spark.sql("CREATE SCHEMA IF NOT EXISTS iceberg.test_db")
print("Schema 'test_db' is ready.")

spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.test_db.kafka_stream_test (
        id BIGINT,
        message STRING,
        value DOUBLE,
        processing_time TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")
print("Iceberg table 'kafka_stream_test' is ready!")

# SPARK STRUCTURED STREAMING TỪ KAFKA 
print("Starting Spark Structured Streaming from Kafka...")

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON từ Kafka value
json_schema = StructType([
    StructField("id", LongType(), True),
    StructField("message", StringType(), True),
    StructField("value", DoubleType(), True)
])

parsed_df = df_stream \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", json_schema).alias("data")) \
    .select("data.*") \
    .withColumn("processing_time", current_timestamp())

# Ghi stream vào Iceberg table
query = parsed_df.writeStream \
    .format("iceberg") \
    .option("checkpointLocation", "hdfs://23133083thuyvan-master:9000/user/hive/checkpoints/kafka_stream_test") \
    .toTable("iceberg.test_db.kafka_stream_test")

print("Streaming job started! Listening to Kafka topic 'test-topic'...")
print("You can now run Producer to send data...")

# Giữ streaming job chạy
query.awaitTermination()