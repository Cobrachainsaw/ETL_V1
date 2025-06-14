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
NAMESPACE = "my_catalog.db"
TABLE_NAME = f"{NAMESPACE}.ecg_features"

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
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.my_catalog.type", "hadoop")
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://ecg-iceberg")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
offset_file_path = sc._jvm.org.apache.hadoop.fs.Path(OFFSET_PATH)

# ---------- Load Last Offset ----------
if fs.exists(offset_file_path):
    with fs.open(offset_file_path) as f:
        last_offset_json = json.loads(f.read().decode("utf-8"))
        print("Loaded offset:", last_offset_json)
else:
    last_offset_json = {"ecg-signals": {"0": 0}}
    print("Using default offset:", last_offset_json)

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
        coeffs = pywt.wavedec(filtered, 'db6', level=9)
        coeffs[0] = coeffs[1] = coeffs[7] = coeffs[8] = coeffs[9] = np.zeros_like(coeffs[1])
        denoised = pywt.waverec(coeffs, 'db6')
        return wavelet_features(denoised) + teager_operator(denoised)

    return signal_series.apply(full_features)

with_features = parsed_df.withColumn("features", extract_features(col("ecg_signal")))

feature_cols = [f"feature_{i}" for i in range(54)]
flattened_df = with_features.select(
    col("timestamp"), col("label"), col("partition"), col("offset"), *[
        col("features")[i].alias(feature_cols[i]) for i in range(54)
    ]
)

# ---------- Create Namespace and Table if Needed ----------
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")

# Create table schema (once)
table_exists = spark._jsparkSession.catalog().tableExists(TABLE_NAME)
if not table_exists:
    cols = ",\n".join([f"{name} DOUBLE" for name in feature_cols])
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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
        str(row["partition"]): row["offset"] + 1
        for _, row in latest_offsets.iterrows()
    }
}

with fs.create(offset_file_path, True) as out:
    out.write(bytearray(json.dumps(offset_dict), "utf-8"))

print("✅ Updated offset file:", offset_dict)
