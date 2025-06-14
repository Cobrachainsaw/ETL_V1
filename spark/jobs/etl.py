import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, pandas_udf
from pyspark.sql.types import *
import pywt
import numpy as np
import pandas as pd
from scipy.signal import butter, filtfilt
from scipy.stats import entropy, skew, kurtosis

OFFSET_PATH = "s3a://ecg-iceberg/offsets/last_offset.json"
CATALOG = "my_catalog"
NAMESPACE = "db"
TABLE = "ecg_features"
TABLE_NAME = f"{CATALOG}.{NAMESPACE}.{TABLE}"

# ---------- Define Schema ----------
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("label", StringType()),
    StructField("ecg_signal", ArrayType(DoubleType()))
])

# ---------- Create Spark Session ----------
spark = (
    SparkSession.builder
    .appName("ECGFeatureExtractorBatch")
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
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minioadmin")
hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# ---------- Load Last Offset ----------
from py4j.java_gateway import java_import
java_import(sc._jvm, "java.net.URI")
uri = sc._jvm.URI("s3a://ecg-iceberg")
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
offset_file_path = sc._jvm.org.apache.hadoop.fs.Path(OFFSET_PATH)

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
    last_offset_json = {"ecg-signals": {"0": 0}}
    print("ℹ️ Using default offset:", last_offset_json)

starting_offsets = json.dumps(last_offset_json)

# ---------- Read Kafka Batch ----------
kafka_df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "ecg-signals")
    .option("startingOffsets", starting_offsets)
    .option("endingOffsets", "latest")
    .load()
)

parsed_df = kafka_df.selectExpr("CAST(value AS STRING) AS json", "topic", "partition", "offset") \
    .withColumn("data", from_json(col("json"), schema)) \
    .select("data.*", "topic", "partition", "offset")

# ---------- Feature Extraction ----------
@pandas_udf("array<double>")
def extract_features(signal_series: pd.Series) -> pd.Series:
    def teager_operator(segment):
        if len(segment) < 100:
            segment = np.pad(segment, (0, 100 - len(segment)))
        teo = segment[1:-1]**2 - segment[:-2] * segment[2:]
        return [
            float(np.mean(teo)), float(np.std(teo)), float(np.max(teo)), float(np.min(teo)), float(np.median(teo)),
            float(np.sum(np.square(teo))), float(entropy(np.abs(teo))), float(skew(teo)), float(kurtosis(teo))
        ]

    def wavelet_features(segment):
        coeffs = pywt.wavedec(segment, 'db4', level=7)
        wave_features = []
        for cD in coeffs[2:7]:
            wave_features.extend([
                float(np.mean(cD)), float(np.std(cD)), float(np.max(cD)), float(np.min(cD)), float(np.median(cD)),
                float(np.sum(np.square(cD))), float(entropy(np.abs(cD))), float(skew(cD)), float(kurtosis(cD))
            ])
        return wave_features

    def full_features(segment):
        segment = np.array(segment)
        fs = 360
        nyq = 0.5 * fs
        b, a = butter(4, [0.5 / nyq, 45 / nyq], btype='band')
        filtered = filtfilt(b, a, segment)
        return wavelet_features(filtered) + teager_operator(filtered)

    return signal_series.apply(full_features)

with_features = parsed_df.withColumn("features", extract_features(col("ecg_signal")))
feature_cols = [f"feature_{i}" for i in range(54)]

flattened_df = with_features.select(
    col("timestamp"), col("label"), col("partition"), col("offset"), *[
        col("features")[i].alias(feature_cols[i]) for i in range(54)
    ]
)

# ---------- Create Namespace and Table if Needed ----------
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")

tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{NAMESPACE}").collect()
existing_tables = [row["tableName"] for row in tables]

if TABLE not in existing_tables:
    cols = ",\n".join([f"{name} DOUBLE" for name in feature_cols])
    spark.sql(f"""
        CREATE TABLE {TABLE_NAME} (
            timestamp STRING,
            label STRING,
            {cols}
        )
        USING iceberg
    """)
    print("✅ Created Iceberg table.")

# ---------- Write to Iceberg ----------
flattened_df.drop("partition", "offset").writeTo(TABLE_NAME).append()
print("✅ Appended batch to Iceberg table.")

# ---------- Save Latest Offset ----------
latest_offsets = (
    flattened_df.groupBy("partition").agg({"offset": "max"})
    .withColumnRenamed("max(offset)", "offset")
    .toPandas()
)

offset_dict = {
    "ecg-signals": {
        str(row["partition"]): int(row["offset"]) + 1  # <-- convert numpy int64 to Python int here
        for _, row in latest_offsets.iterrows()
    }
}

stream = fs.create(offset_file_path, True)
writer = sc._jvm.java.io.OutputStreamWriter(stream)
writer.write(json.dumps(offset_dict))
writer.close()

print("✅ Updated offset file:", offset_dict)
