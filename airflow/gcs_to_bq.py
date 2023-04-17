from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

default_args = {
    'owner': 'your-name',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'gcs_to_bq',
    default_args=default_args,
    description='Move data from GCS to BigQuery',
    schedule_interval=timedelta(days=1),
)

gcs_sensor = GoogleCloudStorageObjectSensor(
    task_id='gcs_sensor',
    bucket='your-gcs-bucket',
    object='your-source-object',
    google_cloud_storage_conn_id='google_cloud_default',
    poke_interval=60,
    timeout=7200,
    dag=dag
)

gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bq',
    bucket='your-gcs-bucket',
    source_objects=['your-source-object'],
    destination_project_dataset_table='your-project.your-dataset.your-table',
    schema_fields=[{'name': 'col1', 'type': 'STRING', 'mode': 'REQUIRED'},
                   {'name': 'col2', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                   {'name': 'col3', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                   {'name': 'col4', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}],
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id='bigquery_default',
    google_cloud_storage_conn_id='google_cloud_default',
    dag=dag
)

gcs_sensor >> gcs_to_bq
