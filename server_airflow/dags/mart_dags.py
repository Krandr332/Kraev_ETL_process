from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_user_activity_mart():
    pg_hook = PostgresHook(postgres_conn_id='postgres_data')
    sql = """
    DROP TABLE IF EXISTS user_activity_mart;
    CREATE TABLE user_activity_mart AS
    WITH session_stats AS (
        SELECT
            s.user_id,
            COUNT(DISTINCT s.session_id) AS total_sessions,
            COUNT(DISTINCT sp.page_url) AS total_unique_pages,
            COUNT(sp.page_url) AS total_page_views,
            COUNT(DISTINCT sa.action_name) AS total_unique_actions,
            COUNT(sa.action_name) AS total_actions,
            SUM(s.duration_seconds) AS total_duration_seconds,
            AVG(s.duration_seconds) AS avg_session_duration_seconds,
            MIN(s.session_date) AS first_activity_date,
            MAX(s.session_date) AS last_activity_date,
            MODE() WITHIN GROUP (ORDER BY s.device) AS preferred_device,
            COUNT(DISTINCT s.session_date) AS active_days
        FROM sessions s
        LEFT JOIN session_pages sp ON s.session_id = sp.session_id
        LEFT JOIN session_actions sa ON s.session_id = sa.session_id
        GROUP BY s.user_id
    )
    SELECT * FROM session_stats;
    """
    pg_hook.run(sql)

def create_support_efficiency_mart():
    pg_hook = PostgresHook(postgres_conn_id='postgres_data')
    sql = """
    DROP TABLE IF EXISTS support_efficiency_mart;
    CREATE TABLE support_efficiency_mart AS
    WITH resolved_tickets AS (
        SELECT
            issue_type,
            status,
            COUNT(*) AS ticket_count,
            AVG(resolution_time_hours) AS avg_resolution_hours,
            MAX(resolution_time_hours) AS max_resolution_hours,
            MIN(resolution_time_hours) AS min_resolution_hours,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY resolution_time_hours) AS median_resolution_hours
        FROM tickets
        WHERE is_resolved = True
        GROUP BY issue_type, status
    ),
    open_tickets AS (
        SELECT
            issue_type,
            status,
            COUNT(*) AS ticket_count,
            AVG(EXTRACT(EPOCH FROM (NOW() - created_at::timestamp))/3600) AS avg_age_hours,
            MAX(EXTRACT(EPOCH FROM (NOW() - created_at::timestamp))/3600) AS max_age_hours
        FROM tickets
        WHERE is_resolved = False
        GROUP BY issue_type, status
    ),
    message_stats AS (
        SELECT
            t.issue_type,
            AVG(tm.message_count) AS avg_messages_per_ticket
        FROM tickets t
        LEFT JOIN (
            SELECT ticket_id, COUNT(*) AS message_count
            FROM ticket_messages
            GROUP BY ticket_id
        ) tm ON t.ticket_id = tm.ticket_id
        GROUP BY t.issue_type
    )
    SELECT 
        COALESCE(r.issue_type, o.issue_type) AS issue_type,
        COALESCE(r.status, o.status) AS status,
        COALESCE(r.ticket_count, 0) AS resolved_count,
        COALESCE(o.ticket_count, 0) AS open_count,
        r.avg_resolution_hours,
        r.median_resolution_hours,
        o.avg_age_hours,
        o.max_age_hours,
        ms.avg_messages_per_ticket
    FROM resolved_tickets r
    FULL OUTER JOIN open_tickets o ON r.issue_type = o.issue_type AND r.status = o.status
    LEFT JOIN message_stats ms ON COALESCE(r.issue_type, o.issue_type) = ms.issue_type;
    """
    pg_hook.run(sql)

with DAG(
        dag_id='build_marts',
        default_args=default_args,
        schedule='@daily',
        catchup=False,
        tags=['marts']
) as dag:
    user_mart = PythonOperator(
        task_id='create_user_activity_mart',
        python_callable=create_user_activity_mart
    )
    support_mart = PythonOperator(
        task_id='create_support_efficiency_mart',
        python_callable=create_support_efficiency_mart
    )
    user_mart >> support_mart