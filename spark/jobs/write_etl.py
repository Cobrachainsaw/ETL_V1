import json
import numpy as np
import pandas as pd
import pywt
from scipy.signal import butter, filtfilt
from scipy.stats import entropy, skew, kurtosis

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, pandas_udf
from pyspark.sql.types import StructType, StringType, ArrayType, DoubleType
from py4j.java_gateway import java_import
from pyspark.sql.types import *

# ---------- Constants ----------
TOPIC_NAME = "ecg-signals"
OFFSET_PATH = "s3a://ecg-iceberg/offsets/combined_offset.json"
RAW_OUTPUT_PATH = "s3a://ecg-raw/"
CATALOG = "my_catalog"
NAMESPACE = "db"
TABLE = "ecg_features"
TABLE_NAME = f"{CATALOG}.{NAMESPACE}.{TABLE}"

# ---------- Define Kafka Message Schema ----------
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("label", StringType()),
    StructField("ecg_signal", ArrayType(DoubleType()))
])

# ---------- Start Spark Session ----------
spark = (
    SparkSession.builder
    .appName("KafkaToMinIOAndIceberg")
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", "s3a://ecg-iceberg")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
java_import(sc._jvm, "java.net.URI")
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
    sc._jvm.URI("s3a://ecg-iceberg"), hadoop_conf
)
offset_file_path = sc._jvm.org.apache.hadoop.fs.Path(OFFSET_PATH)

# ---------- Load Previous Offset ----------
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
    last_offset_json = {TOPIC_NAME: {"0": 0}}
    print("ℹ️ Using default offset:", last_offset_json)

starting_offsets = json.dumps(last_offset_json)

# ---------- Read from Kafka in Batch Mode ----------
kafka_df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", starting_offsets)
    .option("endingOffsets", "latest")
    .load()
)

parsed_df = kafka_df.selectExpr("CAST(value AS STRING) AS json", "topic", "partition", "offset") \
    .withColumn("data", from_json(col("json"), schema)) \
    .select("data.*", "partition", "offset")

# ---------- Write Raw JSON to MinIO ----------
parsed_df.select("timestamp", "label", "ecg_signal") \
         .write.mode("append").json(RAW_OUTPUT_PATH)

print("✅ Raw ECG data written to MinIO.")

# ---------- Feature Extraction UDF ----------
@pandas_udf("array<double>")
def extract_features(signal_series: pd.Series) -> pd.Series:
    def teo(segment):
        if len(segment) < 100:
            segment = np.pad(segment, (0, 100 - len(segment)))
        teo = segment[1:-1]**2 - segment[:-2] * segment[2:]
        return [
            float(np.mean(teo)), float(np.std(teo)), float(np.max(teo)), float(np.min(teo)), float(np.median(teo)),
            float(np.sum(np.square(teo))), float(entropy(np.abs(teo))), float(skew(teo)), float(kurtosis(teo))
        ]

    def wavelet_features(segment):
        coeffs = pywt.wavedec(segment, 'db4', level=7)
        features = []
        for cD in coeffs[2:7]:  # cD2 to cD6
            features.extend([
                float(np.mean(cD)), float(np.std(cD)), float(np.max(cD)), float(np.min(cD)), float(np.median(cD)),
                float(np.sum(np.square(cD))), float(entropy(np.abs(cD))), float(skew(cD)), float(kurtosis(cD))
            ])
        return features

    def all_features(signal):
        signal = np.array(signal)
        fs = 360
        nyq = 0.5 * fs
        b, a = butter(4, [0.5 / nyq, 45 / nyq], btype='band')
        filtered = filtfilt(b, a, signal)
        return wavelet_features(filtered) + teo(filtered)

    return signal_series.apply(all_features)

# ---------- Apply Feature Extraction ----------
with_features = parsed_df.withColumn("features", extract_features(col("ecg_signal")))
feature_cols = [f"feature_{i}" for i in range(54)]

flattened_df = with_features.select(
    col("timestamp"), col("label"), col("partition"), col("offset"), *[
        col("features")[i].alias(feature_cols[i]) for i in range(54)
    ]
)

# ---------- Create Table if Needed ----------
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")

existing_tables = [
    row["tableName"] for row in spark.sql(f"SHOW TABLES IN {CATALOG}.{NAMESPACE}").collect()
]

if TABLE not in existing_tables:
    columns = ",\n".join([f"{col} DOUBLE" for col in feature_cols])
    spark.sql(f"""
        CREATE TABLE {TABLE_NAME} (
            timestamp STRING,
            label STRING,
            {columns}
        )
        USING iceberg
    """)
    print("✅ Created Iceberg table.")

# ---------- Write Extracted Features ----------
flattened_df.drop("partition", "offset") \
    .writeTo(TABLE_NAME).append()

print("✅ Features written to Iceberg.")

# ---------- Update Offset File ----------
latest_offsets = (
    flattened_df.groupBy("partition").agg({"offset": "max"})
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

print("✅ Offset file updated:", offset_dict)
