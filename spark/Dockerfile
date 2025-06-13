FROM tabulario/spark-iceberg:latest

WORKDIR /app

COPY spark-submit.sh /app/spark-submit.sh
COPY ./jobs /app/spark_jobs

RUN chmod +x /app/spark-submit.sh
