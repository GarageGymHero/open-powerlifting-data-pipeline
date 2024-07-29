from airflow.decorators import dag, task
from datetime import datetime
from airflow.models import Variable
from include.coindeskmodel import CoindeskModel
import json
from include.datasets import DATASET_BTC

@dag(start_date=datetime(2023, 1, 1), schedule=None, catchup=False)
def btc_extract():

    @task
    def extract_data_from_api():
        import requests
        response = requests.get(Variable.get('btc_api'))
        return response.json()
    
    @task
    def check_data(data):
        CoindeskModel(**data)
        return data
    
    @task(outlets=[DATASET_BTC])
    def store(data):
        with open(DATASET_BTC.uri, 'w') as f:
            json.dump(data, f)

    store(check_data(extract_data_from_api()))


btc_extract()