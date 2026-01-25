from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime, timedelta
import requests
import json
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def load_json_data(**context):
    url = "https://raw.githubusercontent.com/LearnWebCode/json-example/master/pets-data.json"
    response = requests.get(url)
    json_data = response.json()
    print(json_data)
    context['ti'].xcom_push(key='json_data', value=json.dumps(json_data))


with DAG(
        'json_to_db',
        default_args=default_args,
        start_date=datetime(2024, 1, 1),
        schedule='@daily',
        catchup=False,
        tags=['data_load'],
) as dag:
    load_json_data = PythonOperator(
        task_id='load_json_data',
        python_callable=load_json_data,
    )

    create_and_load = SQLExecuteQueryOperator(
        task_id='create_and_load',
        conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS json_to_db
            (name VARCHAR(255),species VARCHAR(100),
                favFoods TEXT,
                birthYear INTEGER,
                photo TEXT,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
           INSERT INTO json_to_db (name, species, favFoods, birthYear, photo)
            SELECT
                pet ->> 'name' AS name,
                pet ->> 'species' AS species,
                pet ->  'favFoods'::TEXT AS favFoods,
                (pet ->> 'birthYear')::INTEGER AS birthYear,
                pet ->> 'photo' AS photo
            FROM jsonb_array_elements(
                '{{ ti.xcom_pull(task_ids='load_json_data', key='json_data') }}'::jsonb -> 'pets'
            ) AS pet;


            """
    )

    load_json_data >> create_and_load