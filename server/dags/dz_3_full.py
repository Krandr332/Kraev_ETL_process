import pandas as pd
import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def get_full_data_func(**context):
    hook = PostgresHook(postgres_conn_id="postgres")

    get_full_sql = """
                   SELECT device_id,
                          temp,
                          noted_date,
                          location,
                          out_in,
                          loaded_at
                   FROM temperature_filtered
                   ORDER BY noted_date DESC; \
                   """

    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(get_full_sql)

    columns = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()

    data = []
    for row in results:
        data.append(dict(zip(columns, row)))

    cursor.close()
    connection.close()

    context['ti'].xcom_push(key='full_data', value=json.dumps(data, default=str))
    print(f"Получено {len(data)} записей (полная история)")

    return len(data)


def process_full_data(**context):
    ti = context['ti']

    result_json = ti.xcom_pull(task_ids='get_full_data_task', key='full_data')

    if not result_json:
        print("Нет данных для обработки")
        return

    result_data = json.loads(result_json)

    if not result_data:
        print("Пустой результат из БД")
        return

    print(f"Получены полные данные, количество записей: {len(result_data)}")

    df = pd.DataFrame(result_data)

    if len(df) > 0:
        print("Первые 5 записей полных данных:")
        print(df.head())

        if 'temp' in df.columns:
            print(f"\nСтатистика по полным данным:")
            print(f"Средняя температура: {df['temp'].mean():.2f}")
            print(f"Минимальная температура: {df['temp'].min():.2f}")
            print(f"Максимальная температура: {df['temp'].max():.2f}")
            print(f"Количество уникальных дней: {df['noted_date'].nunique()}")


with DAG(
        dag_id="dz_3_full",
        default_args=default_args,
        start_date=datetime(2026, 1, 1),
        schedule="@monthly",
        catchup=False,
        tags=["full_load", "history"],
) as dag:
    get_full_data_task = PythonOperator(
        task_id="get_full_data_task",
        python_callable=get_full_data_func,
    )

    process_full_data_task = PythonOperator(
        task_id="process_full_data",
        python_callable=process_full_data,
    )

    get_full_data_task >> process_full_data_task