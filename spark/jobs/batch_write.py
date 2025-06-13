from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, ArrayType, DoubleType

def main():
    spark = (
        SparkSession.builder
        .appName("KafkaToMinIORaw")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    # Define schema for the JSON messages
    schema = StructType() \
        .add("timestamp", StringType()) \
        .add("label", StringType()) \
        .add("ecg_signal", ArrayType(DoubleType()))

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "ecg-signals")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
                        .withColumn("data", from_json(col("json_string"), schema)) \
                        .select("data.*")  # Flatten the nested column

    # Write each microbatch to MinIO as JSON
    def write_to_minio(batch_df, batch_id):
        batch_df.write.mode("append").json("s3a://ecg-raw/")

    query = parsed_df.writeStream.foreachBatch(write_to_minio).start()
    query.awaitTermination()

if __name__ == "__main__":
    main()
