import sys
import subprocess
import os
import pandas as pd
import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def install_kagglehub():
    try:
        import kagglehub
    except ImportError:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "kagglehub"]
        )


def get_data_from_db_func(**context):
    """Получаем данные из базы данных с помощью PostgresHook"""
    hook = PostgresHook(postgres_conn_id="postgres")

    get_last_two_days_sql = """
        WITH _data_ AS (
        SELECT MAX(noted_date) as last_date 
        FROM temperature_filtered
        )
        SELECT device_id,
           temp,
           noted_date,
           location,
           out_in,
           loaded_at
        FROM temperature_filtered
            CROSS JOIN _data_
        WHERE noted_date >= _data_.last_date - INTERVAL '2 days'
        ORDER BY noted_date DESC;
    """

    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(get_last_two_days_sql)

    columns = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()

    data = []
    for row in results:
        data.append(dict(zip(columns, row)))

    cursor.close()
    connection.close()

    context['ti'].xcom_push(key='last_two_days_data', value=json.dumps(data, default=str))
    print(f"Получено {len(data)} записей из БД")

    return len(data)


def process_last_two_days_data(**context):
    ti = context['ti']

    result_json = ti.xcom_pull(task_ids='get_data_from_db_task', key='last_two_days_data')

    if not result_json:
        print("Нет данных для обработки")
        return

    result_data = json.loads(result_json)

    if not result_data:
        print("Пустой результат из БД")
        return

    print(f"Получены данные, количество записей: {len(result_data)}")

    df = pd.DataFrame(result_data)

    if len(df) > 0:
        print("Первые 5 записей:")
        print(df.head())

        if 'temp' in df.columns:
            print(f"\nСтатистика по температуре за последние 2 дня:")
            print(f"Средняя температура: {df['temp'].mean():.2f}")
            print(f"Минимальная температура: {df['temp'].min():.2f}")
            print(f"Максимальная температура: {df['temp'].max():.2f}")
            print(f"Количество уникальных дней: {df['noted_date'].nunique()}")

            if 'noted_date' in df.columns:
                daily_stats = df.groupby('noted_date')['temp'].agg(['mean', 'min', 'max', 'count'])
                print("\nСтатистика по дням:")
                print(daily_stats)



test_connection_sql = """
SELECT 1 as connection_test;
"""

with DAG(
        dag_id="dz_3",
        default_args=default_args,
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
        catchup=False,
        tags=["get_data", "get_last_two_day"],
) as dag:


    get_data_from_db_task = PythonOperator(
        task_id="get_data_from_db_task",
        python_callable=get_data_from_db_func,
    )

    process_data_task = PythonOperator(
        task_id="process_last_two_days",
        python_callable=process_last_two_days_data,
    )

    get_data_from_db_task >> process_data_task