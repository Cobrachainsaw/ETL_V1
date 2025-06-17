import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, ArrayType, DoubleType
from py4j.java_gateway import java_import

# ---------- Constants ----------
OFFSET_PATH = "s3a://ecg-iceberg/offsets/raw_offset.json"
RAW_OUTPUT_PATH = "s3a://ecg-raw/"
TOPIC_NAME = "ecg-signals"

# ---------- Define Schema ----------
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("label", StringType()) \
    .add("ecg_signal", ArrayType(DoubleType()))

# ---------- Create Spark Session ----------
spark = (
    SparkSession.builder
    .appName("KafkaToMinIORawBatch")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()

# For accessing MinIO via Java Hadoop FS
java_import(sc._jvm, "java.net.URI")
uri = sc._jvm.URI("s3a://ecg-iceberg")
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
offset_file_path = sc._jvm.org.apache.hadoop.fs.Path(OFFSET_PATH)

# ---------- Load Last Offset ----------
if fs.exists(offset_file_path):
    stream = fs.open(offset_file_path)
    reader = sc._jvm.java.io.BufferedReader(sc._jvm.java.io.InputStreamReader(stream))
    content = []
    while True:
        line = reader.readLine()
        if line is None:
            break
        content.append(line)
    reader.close()
    last_offset_json = json.loads("\n".join(content))
    print("✅ Loaded offset:", last_offset_json)
else:
    last_offset_json = {TOPIC_NAME: {"0": 0}}  # default to partition 0
    print("ℹ️ Using default offset:", last_offset_json)

starting_offsets = json.dumps(last_offset_json)

# ---------- Read from Kafka (Batch Mode) ----------
kafka_df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", starting_offsets)
    .option("endingOffsets", "latest")
    .load()
)

# ---------- Parse & Flatten ----------
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) AS json", "topic", "partition", "offset") \
    .withColumn("data", from_json(col("json"), schema)) \
    .select("data.*", "topic", "partition", "offset")

# ---------- Write Raw JSON to MinIO ----------
parsed_df.drop("topic", "partition", "offset") \
         .write.mode("append").json(RAW_OUTPUT_PATH)

print("✅ Wrote raw data to MinIO:", RAW_OUTPUT_PATH)

# ---------- Save Latest Offset ----------
latest_offsets = (
    parsed_df.groupBy("partition").agg({"offset": "max"})
    .withColumnRenamed("max(offset)", "offset")
    .toPandas()
)

offset_dict = {
    TOPIC_NAME: {
        str(row["partition"]): int(row["offset"]) + 1
        for _, row in latest_offsets.iterrows()
    }
}

stream = fs.create(offset_file_path, True)
writer = sc._jvm.java.io.OutputStreamWriter(stream)
writer.write(json.dumps(offset_dict))
writer.close()

print("✅ Updated raw offset file:", offset_dict)
