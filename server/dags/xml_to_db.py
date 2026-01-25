from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def xml_to_dict(element):

    data = {}

    if element.attrib:
        data.update(element.attrib)

    children = list(element)
    if children:
        for child in children:
            child_data = xml_to_dict(child)
            tag = child.tag

            if tag in data:
                if not isinstance(data[tag], list):
                    data[tag] = [data[tag]]
                data[tag].append(child_data)
            else:
                data[tag] = child_data
    else:
        text = element.text.strip() if element.text else ""
        if text:
            if not element.attrib:
                return text
            else:
                data["value"] = text

    return data


def load_xml_data(**context):
    import json
    import xml.etree.ElementTree as ET

    url = 'https://gist.githubusercontent.com/pamelafox/3000322/raw/6cc03bccf04ede0e16564926956675794efe5191/nutrition.xml'
    response = requests.get(url).text
    root = ET.fromstring(response)

    json_ = xml_to_dict(root)
    print(json_)
    context['ti'].xcom_push(key='xml_data', value=json.dumps(json_).replace("'", "''"))


with DAG(
        'xml_to_db',
        default_args=default_args,
        start_date=datetime(2024, 1, 1),
        schedule='@daily',
        catchup=False,
        tags=['data_load'],
) as dag:
    load_xml_data = PythonOperator(
        task_id='load_xml_data',
        python_callable=load_xml_data,
    )

    create_and_load = SQLExecuteQueryOperator(
        task_id='create_and_load',
        conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS xml_to_db (
                name TEXT,
                mfr TEXT,
                serving TEXT,
                saturated_fat REAL,
                cholesterol TEXT,
                sodium INTEGER,
                carb INTEGER,
                fiber INTEGER,
                protein INTEGER,
                vitamins TEXT,
                minerals TEXT,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

           INSERT INTO xml_to_db (name, mfr, serving,
                       saturated_fat, cholesterol,
                      sodium, carb, fiber, protein, vitamins,
                      minerals, loaded_at)
            SELECT
                fo ->> 'name' AS name,
                fo->> 'mfr' AS mfr,
                fo->> 'serving' AS serving_units,
                (fo->> 'saturated-fat')::REAL AS saturated_fat,  
                (fo->> 'cholesterol')::TEXT AS cholesterol,  
                (fo->> 'sodium')::INTEGER AS sodium, 
                (fo->> 'carb')::INTEGER AS carb,  
                (fo->> 'fiber')::INTEGER AS fiber, 
                (fo->> 'protein')::INTEGER AS protein,  
                fo-> 'vitamins' AS vitamins,
                fo-> 'minerals' AS minerals,
                CURRENT_TIMESTAMP AS loaded_at 
            FROM jsonb_array_elements(
                '{{ ti.xcom_pull(task_ids='load_xml_data', key='xml_data') }}'::jsonb -> 'food'
            ) AS fo;


            """
    )

    load_xml_data >> create_and_load