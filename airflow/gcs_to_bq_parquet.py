from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'gcs_trigger_dag',
    default_args=default_args,
    description='A DAG that triggers when a new Parquet file is created in a GCS bucket and validates the file name convention.',
    schedule_interval=None,
)

def validate_file(bucket, prefix, delimiter, file_name_convention):
    from google.cloud import storage

    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blobs = bucket.list_blobs(prefix=prefix, delimiter=delimiter)
    newest_blob = None

    for blob in blobs:
        if not newest_blob or blob.updated > newest_blob.updated:
            newest_blob = blob

    if not newest_blob:
        raise ValueError('No file found in GCS bucket')

    if not newest_blob.name.endswith('.parquet'):
        raise ValueError('File is not a Parquet file')

    if not newest_blob.name.startswith(file_name_convention):
        raise ValueError('File name does not match convention')

    return newest_blob.name

def process_file(bucket, prefix, delimiter, file_name):
    # Your code to process the new file goes here
    pass

wait_for_file = GCSObjectExistenceSensor(
    task_id='wait_for_file',
    bucket='your-bucket',
    object='your-prefix',
    dag=dag,
)

validate_file_name = PythonOperator(
    task_id='validate_file_name',
    python_callable=validate_file,
    op_kwargs={
        'bucket': 'your-bucket',
        'prefix': 'your-prefix',
        'delimiter': '/',
        'file_name_convention': 'your-file-name-convention',
    },
    dag=dag,
)

validate_data = PythonOperator(
    task_id='validate_data',
    python_callable=process_file,
    op_kwargs={
        'bucket': 'your-bucket',
        'prefix': 'your-prefix',
        'delimiter': '/',
    },
    op_args=[{{ task_instance.xcom_pull(task_ids='validate_file_name') }}],
    dag=dag,
)

write_to_bigquery = BigQueryInsertJobOperator(
    task_id='write_to_bigquery',
    configuration={
        'query': {
            'query': 'SELECT * FROM `your-project-id.your-dataset-id.your-table-id`',
            'useLegacySql': False,
        },
    },
    project_id='your-project-id',
    dag=dag,
)

wait_for_file >> validate_file_name >> validate_data >> write_to_bigquery
