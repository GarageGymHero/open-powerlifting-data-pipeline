from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models import Variable

@dag(schedule=None, catchup=False)
def gcs_to_bigquery():

    load_data = GCSToBigQueryOperator(
        task_id='load_gcs_to_bq',
        bucket=Variable.get('gcs_bucket'),
        source_objects=['data/partitioned_data/*.parquet'],
        destination_project_dataset_table='open-powerlifting.open_powerlifting.records',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True
    )

gcs_to_bigquery()
