import sys
from airflow import DAG
import subprocess
from datetime import datetime, timedelta
import pandas as pd
import json


def install_package(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--user", package])


try:
    from airflow.providers.mongo.hooks.mongo import MongoHook
except ImportError:
    install_package("apache-airflow-providers-mongo")
    install_package("pymongo")
    from airflow.providers.mongo.hooks.mongo import MongoHook

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def extract_from_mongo(**context):
    hook = MongoHook(mongo_conn_id='mongo_default')
    client = hook.get_conn()
    db = client['source_db']

    sessions = list(db.user_sessions.find({}, {'_id': 0}))
    tickets = list(db.support_tickets.find({}, {'_id': 0}))

    context['ti'].xcom_push(key='sessions', value=json.dumps(sessions, default=str))
    context['ti'].xcom_push(key='tickets', value=json.dumps(tickets, default=str))


def transform_and_load(**context):
    ti = context['ti']
    sessions_json = ti.xcom_pull(key='sessions', task_ids='extract_from_mongo')
    tickets_json = ti.xcom_pull(key='tickets', task_ids='extract_from_mongo')

    sessions = json.loads(sessions_json) if sessions_json else []
    tickets = json.loads(tickets_json) if tickets_json else []

    for session in sessions:
        if session.get('start_time') and session.get('end_time'):
            start = datetime.fromisoformat(session['start_time'].replace('Z', '+00:00'))
            end = datetime.fromisoformat(session['end_time'].replace('Z', '+00:00'))
            session['duration_seconds'] = (end - start).total_seconds()
            session['duration_minutes'] = session['duration_seconds'] / 60
            session['session_date'] = start.date().isoformat()
            session['session_hour'] = start.hour

    for ticket in tickets:
        if ticket.get('created_at') and ticket.get('updated_at'):
            created = datetime.fromisoformat(ticket['created_at'].replace('Z', '+00:00'))
            updated = datetime.fromisoformat(ticket['updated_at'].replace('Z', '+00:00'))
            ticket['resolution_time_seconds'] = (updated - created).total_seconds()
            ticket['resolution_time_hours'] = ticket['resolution_time_seconds'] / 3600

        if ticket.get('status') in ['resolved', 'closed']:
            ticket['is_resolved'] = True
        else:
            ticket['is_resolved'] = False

    sessions = [s for s in sessions if s.get('session_id') and s.get('user_id')]
    tickets = [t for t in tickets if t.get('ticket_id') and t.get('user_id')]

    pg_hook = PostgresHook(postgres_conn_id='postgres_data')
    engine = pg_hook.get_sqlalchemy_engine()

    if sessions:
        df_s = pd.DataFrame(sessions)
        df_s[['session_id', 'user_id', 'start_time', 'end_time', 'device',
              'duration_seconds', 'duration_minutes', 'session_date', 'session_hour']].to_sql(
            'sessions', engine, if_exists='replace', index=False
        )

        if 'pages_visited' in df_s.columns:
            pages_rows = []
            for _, row in df_s.iterrows():
                if isinstance(row['pages_visited'], list):
                    for i, page in enumerate(row['pages_visited']):
                        pages_rows.append({'session_id': row['session_id'], 'page_url': page, 'page_order': i})
            if pages_rows:
                pd.DataFrame(pages_rows).to_sql('session_pages', engine, if_exists='replace', index=False)

        if 'actions' in df_s.columns:
            actions_rows = []
            for _, row in df_s.iterrows():
                if isinstance(row['actions'], list):
                    for i, act in enumerate(row['actions']):
                        actions_rows.append({'session_id': row['session_id'], 'action_name': act, 'action_order': i})
            if actions_rows:
                pd.DataFrame(actions_rows).to_sql('session_actions', engine, if_exists='replace', index=False)

    if tickets:
        df_t = pd.DataFrame(tickets)
        df_t[['ticket_id', 'user_id', 'status', 'issue_type', 'created_at', 'updated_at',
              'resolution_time_seconds', 'resolution_time_hours', 'is_resolved']].to_sql(
            'tickets', engine, if_exists='replace', index=False
        )

        if 'messages' in df_t.columns:
            messages_rows = []
            for _, row in df_t.iterrows():
                if isinstance(row['messages'], list):
                    for i, msg in enumerate(row['messages']):
                        if isinstance(msg, dict):
                            messages_rows.append({
                                'ticket_id': row['ticket_id'],
                                'sender': msg.get('sender', ''),
                                'message': msg.get('message', ''),
                                'timestamp': pd.to_datetime(msg.get('timestamp')),
                                'message_order': i
                            })
            if messages_rows:
                pd.DataFrame(messages_rows).to_sql('ticket_messages', engine, if_exists='replace', index=False)


with DAG(
        dag_id='replication_mongo_to_postgres',
        default_args=default_args,
        schedule='@daily',
        catchup=False,
        tags=['etl']
) as dag:
    extract = PythonOperator(task_id='extract_from_mongo', python_callable=extract_from_mongo)
    load = PythonOperator(task_id='transform_and_load', python_callable=transform_and_load)
    extract >> load