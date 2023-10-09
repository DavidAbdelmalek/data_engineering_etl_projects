from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'etl_streaming',
    'start_date': datetime(2023,10,6,15)
}



def get_data():
    import requests

    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    
    return res

def format_data(res):
    data = {}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['email'] = res['email']
    data['phone'] = res['phone']
    data['address'] = str(res['location']['street']['name']) + ' ' + str(res['location']['street']['number'])
    
    data['state'] = res['location']['state']
    data['postcode'] = res['location']['postcode']
    data['city'] = res['location']['city']
    data['country'] = res['location']['country']

    data['username'] = res['login']['username']
    data['registered_date'] = res['registered']['date']

    return data

def stream_data_from_api():
    import json

    res = get_data()
    data = format_data(res)

    print(json.dumps(data, indent=3))

stream_data_from_api()

# with DAG(
#     dag_id='etl_dag',
#     default_args=default_args,
#     schedule='@daily',
#     catchup=False,
#     tags=['kafka'],
#     max_active_runs=1
# ) as dag:
    
#     streaming_task = PythonOperator(
#         task_id='stream_data_from_api',
#         python_callable=stream_data_from_api
#     )