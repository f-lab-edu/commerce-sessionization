from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': True
}

SPARK_JOB = {
    "reference": {"project_id": "sixth-well-442104-a4"},
    "placement": {"cluster_name": "cluster-3ed3"},
    "spark_job": {
        "main_class": "sessionization.SessionizationBuiltIn",
        "jar_file_uris": ["gs://daeuk/jars/SessionizationBuiltIn.jar"],
        "args": [
            "{{ ds }}",
            "{{ logical_date.strftime('%H') }}"
        ],
        "properties": {
            "spark.sql.sources.partitionOverwriteMode": "dynamic"
        }
    }
}

with DAG(
        "sessionization",
        default_args=default_args,
        description="Sessionization",
        schedule_interval="10 * * * *",
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

    submit_spark_job
