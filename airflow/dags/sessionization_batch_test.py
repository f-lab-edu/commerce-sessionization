from datetime import timedelta, datetime

import gcsfs
import pyarrow.parquet as pq
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

BEHAVIOR_PATH = "gs://daeuk-tests/behaviors"

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
            BEHAVIOR_PATH
        ],
        "properties": {
            "spark.sql.sources.partitionOverwriteMode": "dynamic"
        }
    }
}


def validate_upstream(logical_date, ti):
    ds = logical_date.strftime("%Y-%m-%d")
    hour = logical_date.strftime("%H")
    fs = gcsfs.GCSFileSystem(token=GCSHook().get_credentials())
    files = fs.glob(f"{BEHAVIOR_PATH}/logs/event_date={ds}/event_hour={hour}/*.parquet")

    upstream_count = count_rows(fs, files)
    ti.xcom_push(key="upstream_count", value=upstream_count)


def validate_downstream(logical_date, ti):
    ds = logical_date.strftime("%Y-%m-%d")
    hour = logical_date.strftime("%H")
    fs = gcsfs.GCSFileSystem(token=GCSHook().get_credentials())
    files = fs.glob(f"{BEHAVIOR_PATH}/sessions/event_date={ds}/event_hour={hour}/*.parquet")

    downstream_count = count_rows(fs, files)

    upstream_count = ti.xcom_pull(task_ids="validate_upstream", key="upstream_count")

    if downstream_count != upstream_count:
        raise ValueError("Upstream and downstream record counts do not match.")

    for file in files:
        with fs.open(file) as f:
            parquet = pq.ParquetFile(f)

            if "session_id" not in parquet.schema.names:
                raise ValueError("session_id column missing")

            session_id_column = parquet.read(['session_id'])['session_id']
            if 0 < session_id_column.null_count:
                raise ValueError("Missing values found in session_id column")


def count_rows(fs, files):
    if not files:
        raise ValueError(f"No files found")

    total_rows = 0

    for file in files:
        with fs.open(file) as f:
            total_rows += pq.read_table(f).num_rows

    if total_rows <= 0:
        raise ValueError(f"Files are empty")

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
    validate_upstream_task = PythonOperator(
        task_id="validate_upstream",
        python_callable=validate_upstream,
    )

    submit_spark_job = DataprocSubmitJobOperator(
        task_id="sessionization_test",
        job=SPARK_JOB,
        region="asia-northeast3",
        project_id="sixth-well-442104-a4",
    )

    validate_downstream_task = PythonOperator(
        task_id="validate_downstream",
        python_callable=validate_downstream
    )

    validate_upstream_task >> submit_spark_job >> validate_downstream_task

if __name__ == "__main__":
    dag.test()
