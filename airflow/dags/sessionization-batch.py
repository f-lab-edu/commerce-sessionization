from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
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
        schedule_interval="@hourly",
        start_date=days_ago(1),
        catchup=False,
) as dag:
    submit_spark_job = DataprocSubmitJobOperator(
        task_id="sessionization",
        job=SPARK_JOB,
        region="asia-northeast3",
        project_id="sixth-well-442104-a4",
    )

    submit_spark_job
