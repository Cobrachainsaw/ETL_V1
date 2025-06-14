from prefect import flow, task
import subprocess

@task
def run_spark_job(script_name):
    result = subprocess.run(
        ["/app/spark-submit.sh", script_name],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"Spark job {script_name} failed:\n{result.stderr}")
    print(result.stdout)

@flow(name="ECG ETL Batch Pipeline")
def ecg_batch_pipeline():
    run_spark_job("batch_write.py")
    run_spark_job("etl.py")
