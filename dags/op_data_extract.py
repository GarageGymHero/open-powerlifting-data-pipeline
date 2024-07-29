from airflow.decorators import dag, task
from datetime import datetime
from airflow.models import Variable
import pandas as pd
import zipfile
import os

@dag()
def op_data_extract(schedule=None, catchup=False):

    @task
    def extract_data_from_url():
        import requests

        response = requests.get(Variable.get('op_url'))
        
        with open(Variable.get('op_zipfile'), 'wb') as f:
            f.write(response.content)

        with zipfile.ZipFile(Variable.get('op_zipfile'), 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                if file_info.filename.endswith('.csv'):
                    csv_file_name = file_info.filename
                    zip_ref.extract(file_info.filename)
                    break

        dtype_dict = {
            'Name': 'str',
            'Sex': 'str',
            'Event': 'str',
            'Equipment': 'str',
            'Age': 'float32',
            'AgeClass': 'str',
            'BirthYearClass': 'str',
            'Division': 'str',
            'BodyweightKg': 'float32',
            'WeightClassKg': 'str',
            'Squat1Kg': 'float32',
            'Squat2Kg': 'float32',
            'Squat3Kg': 'float32',
            'Squat4Kg': 'float32',
            'Best3SquatKg': 'float32',
            'Bench1Kg': 'float32',
            'Bench2Kg': 'float32',
            'Bench3Kg': 'float32',
            'Bench4Kg': 'float32',
            'Best3BenchKg': 'float32',
            'Deadlift1Kg': 'float32',
            'Deadlift2Kg': 'float32',
            'Deadlift3Kg': 'float32',
            'Deadlift4Kg': 'float32',
            'Best3DeadliftKg': 'float32',
            'TotalKg': 'float32',
            'Place': 'str',
            'Dots': 'float32',
            'Wilks': 'float32',
            'Glossbrenner': 'float32',
            'Goodlift': 'float32',
            'Tested': 'str',
            'Country': 'str',
            'State': 'str',
            'Federation': 'str',
            'ParentFederation': 'str',
            'Date': 'str',
            'MeetCountry': 'str',
            'MeetState': 'str',
            'MeetTown': 'str',
            'MeetName': 'str',
            'Sanctioned': 'str',
        }

        chunks = []
        for chunk in pd.read_csv(csv_file_name, chunksize=100000, low_memory=False, dtype=dtype_dict):
            chunks.append(chunk)

        df = pd.concat(chunks, ignore_index=True)
        local_csv_path = '/tmp/op_data.csv'
        df.to_csv(local_csv_path, index=False)

        return local_csv_path
    
    @task
    def upload_to_gcs(local_csv_path):
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        bucket_name = Variable.get('gcs_bucket')
        destination_blob_name = 'data/op_data.csv'

        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=destination_blob_name,
            filename=local_csv_path
        )
    
        os.remove(local_csv_path)


    upload_to_gcs(extract_data_from_url())

op_data_extract()