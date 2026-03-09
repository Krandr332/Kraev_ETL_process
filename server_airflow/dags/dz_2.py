import sys
import subprocess
import os
import pandas as pd
import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

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

def download_and_load_data(**context):
    df = pd.read_csv("/opt/airflow/dags/IOT-temp.csv")

    df["noted_date"] = pd.to_datetime(
        df["noted_date"],
        format="%d-%m-%Y %H:%M",
        errors="coerce"
    )
    df["noted_date"] = df["noted_date"].dt.floor("D")

    df = df[df["out/in"].str.lower() == "in"]

    p5 = df["temp"].quantile(0.05)
    p95 = df["temp"].quantile(0.95)
    df = df[(df["temp"] >= p5) & (df["temp"] <= p95)]

    daily_avg = (
        df.groupby("noted_date", as_index=False)
        .agg(temp=("temp", "mean"))
    )

    df["noted_date_str"] = df["noted_date"].dt.strftime("%Y-%m-%d")
    daily_avg["noted_date_str"] = daily_avg["noted_date"].dt.strftime("%Y-%m-%d")

    hottest_days = daily_avg.nlargest(5, "temp")
    coldest_days = daily_avg.nsmallest(5, "temp")

    filtered_data_records = df.assign(noted_date=df["noted_date_str"]).to_dict("records")
    hottest_days_records = hottest_days.rename(columns={"temp": "avg_temp"}).assign(
        noted_date=hottest_days["noted_date_str"]
    ).to_dict("records")
    coldest_days_records = coldest_days.rename(columns={"temp": "avg_temp"}).assign(
        noted_date=coldest_days["noted_date_str"]
    ).to_dict("records")

    result_json = json.dumps({
        "filtered_data": filtered_data_records,
        "hottest_days": hottest_days_records,
        "coldest_days": coldest_days_records,
    })

    context["ti"].xcom_push(
        key="processed_data",
        value=result_json
    )

create_tables_sql = """
CREATE TABLE IF NOT EXISTS temperature_filtered (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(100),
    temp FLOAT,
    noted_date DATE,
    location VARCHAR(255),
    out_in VARCHAR(10),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hottest_days (
    id SERIAL PRIMARY KEY,
    noted_date DATE,
    avg_temp FLOAT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS coldest_days (
    id SERIAL PRIMARY KEY,
    noted_date DATE,
    avg_temp FLOAT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

truncate_tables_sql = """
TRUNCATE TABLE temperature_filtered;
TRUNCATE TABLE hottest_days;
TRUNCATE TABLE coldest_days;
"""

insert_data_sql = """
INSERT INTO temperature_filtered (device_id, temp, noted_date, location, out_in)
SELECT 
    data->>'id' AS device_id,
    (data->>'temp')::FLOAT AS temp,
    (data->>'noted_date')::DATE AS noted_date,
    data->>'room_id/id' AS location,
    data->>'out/in' AS out_in
FROM jsonb_array_elements(
    '{{ ti.xcom_pull(task_ids='download_and_process', key='processed_data') }}'::jsonb->'filtered_data'
) AS data;

INSERT INTO hottest_days (noted_date, avg_temp)
SELECT 
    (data->>'noted_date')::DATE AS noted_date,
    (data->>'avg_temp')::FLOAT AS avg_temp
FROM jsonb_array_elements(
    '{{ ti.xcom_pull(task_ids='download_and_process', key='processed_data') }}'::jsonb->'hottest_days'
) AS data;

INSERT INTO coldest_days (noted_date, avg_temp)
SELECT 
    (data->>'noted_date')::DATE AS noted_date,
    (data->>'avg_temp')::FLOAT AS avg_temp
FROM jsonb_array_elements(
    '{{ ti.xcom_pull(task_ids='download_and_process', key='processed_data') }}'::jsonb->'coldest_days'
) AS data;
"""

count_rows_sql = """
SELECT 'temperature_filtered' AS table_name, COUNT(*) AS row_count FROM temperature_filtered
UNION ALL
SELECT 'hottest_days' AS table_name, COUNT(*) AS row_count FROM hottest_days
UNION ALL
SELECT 'coldest_days' AS table_name, COUNT(*) AS row_count FROM coldest_days;
"""

full_sql = create_tables_sql + truncate_tables_sql + insert_data_sql + count_rows_sql

with DAG(
        dag_id="dz_2",
        default_args=default_args,
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
        catchup=False,
        tags=["download", "data_processing"],
) as dag:
    process_data_task = PythonOperator(
        task_id="download_and_process",
        python_callable=download_and_load_data,
    )

    load_to_db_task = SQLExecuteQueryOperator(
        task_id="db_process",
        conn_id="postgres",
        sql=full_sql,
    )

    process_data_task >> load_to_db_task