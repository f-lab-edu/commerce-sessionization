from datetime import timedelta, datetime

import gcsfs
import pyarrow.parquet as pq
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': True
}

SPARK_JOB = {
    "reference": {"project_id": "sixth-well-442104-a4"},
    "placement": {"cluster_name": "cluster-3ed3"},
    "spark_job": {
        "main_class": "sessionization.SessionizationBuiltIn",
        "jar_file_uris": ["gs://daeuk-tests/jars/SessionizationBuiltIn.jar"],
        "args": [
            "{{ ds }}",  # eventDate
            "{{ logical_date.strftime('%H') }}",  # eventHour
            "gs://daeuk-tests/behaviors"  # behaviorsPath
        ],
        "properties": {
            "spark.sql.sources.partitionOverwriteMode": "dynamic"
        }
    }
}


def validate(logical_date):
    ds = logical_date.strftime("%Y-%m-%d")
    hour = logical_date.strftime("%H")

    input_count = count_rows(
        f"gs://daeuk-tests/behaviors/logs/event_date={ds}/event_hour={hour}")

    output_count = count_rows(
        f"gs://daeuk-tests/behaviors/sessions/event_date={ds}/event_hour={hour}")

    if input_count != output_count:
        raise ValueError("Input and output record counts do not match.")


def count_rows(path):
    token = GCSHook().get_credentials()
    fs = gcsfs.GCSFileSystem(token=token)

    files = fs.glob(f"{path}/*.parquet")

    if not files:
        raise ValueError(f"No files found in path : {path}")

    total_rows = 0

    for file in files:
        with fs.open(file) as f:
            total_rows += pq.read_table(f).num_rows

    if total_rows <= 0:
        raise ValueError(f"Files are empty, path : {path}")

    return total_rows


with DAG(
        "sessionization_test",
        default_args=default_args,
        description="Sessionization Test",
        schedule="10 * * * *",
        start_date=datetime(2024, 11, 20, 00),
        catchup=True,
        max_active_runs=1
) as dag:
    submit_spark_job = DataprocSubmitJobOperator(
        task_id="sessionization",
        job=SPARK_JOB,
        region="asia-northeast3",
        project_id="sixth-well-442104-a4",
    )

    validate_task = PythonOperator(
        task_id="validate_task",
        python_callable=validate
    )

    submit_spark_job >> validate_task

if __name__ == "__main__":
    dag.test()
