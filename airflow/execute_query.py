from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'owner',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    't_employee_lane_actions',
    default_args=default_args,
    schedule_interval='@daily',
) as dag:

    execute_query = BigQueryExecuteQueryOperator(
        task_id='execute_query',
        sql='sql/query_script.sql',
        use_legacy_sql=False,
        write_disposition='WRITE_EMPTY',
        allow_large_results=True
    )
