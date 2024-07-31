from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# Define cluster configuration
CLUSTER_NAME = "open-powerlifting-cluster"
REGION = "us-east1"
PROJECT_ID = "open-powerlifting"
GCS_BUCKET = "open_powerlifting_data_pipeline"

@dag(schedule=None, catchup=False)
def dataproc_spark_dag():

    @task
    def get_spark_job_config():
        return {
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"gs://{GCS_BUCKET}/spark_apps/spark_app.py",
                'args': ['--input_path', f'gs://{GCS_BUCKET}/data/op_data.csv', '--output_path', f'gs://{GCS_BUCKET}/data/partitioned_data'],
            },
        }

    spark_job_config = get_spark_job_config()

    submit_spark_job = DataprocSubmitJobOperator(
        task_id="submit_spark_job",
        project_id=PROJECT_ID,
        region=REGION,
        job=spark_job_config
    )

    # Define task dependencies
    spark_job_config >> submit_spark_job

# Instantiate the DAG
dataproc_spark_dag()