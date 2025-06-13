#!/bin/bash

SCRIPT=$1

/opt/bitnami/spark/bin/spark-submit \
  --master local[*] \
  --deploy-mode client \
  --name ecg_etl_job \
  --executor-memory 2g \
  --driver-memory 1g \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1 \
  --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.my_catalog.type=hadoop \
  --conf spark.sql.catalog.my_catalog.warehouse=s3a://ecg-iceberg \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  /app/spark_jobs/$SCRIPT
