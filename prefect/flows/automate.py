from prefect import flow, task
import subprocess

@task
def run_spark_job(script_name):
    command = ["docker", "exec", "spark", "/app/spark-submit.sh", script_name]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Spark job {script_name} failed:\n{result.stderr}")
    print(result.stdout)

@flow(name="ecg_batch_pipeline")
def ecg_batch_pipeline():
    run_spark_job("write_etl.py")
