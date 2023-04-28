import re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dataflow_operator import DataFlowJavaOperator
from google.cloud import bigquery

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

def validate_data():
    input_file = '/tmp/your-source-object'
    with open(input_file, 'r') as f_in:
        for line in f_in:
            fields = line.strip().split(',')
            # Validate the data type of field 1
            try:
                field1 = int(fields[0])
            except ValueError:
                raise ValueError('Field 1 is not an integer')
            # Validate the data type of field 2
            try:
                field2 = float(fields[1])
            except ValueError:
                raise ValueError('Field 2 is not a float')
            # Validate the data type of field 3
            if not isinstance(fields[2], str):
                raise ValueError('Field 3 is not a string')
            # Validate the regex pattern of field 4
            if not re.match(r'^\d{3}-\d{2}-\d{4}$', fields[3]):
                raise ValueError('Field 4 does not match the pattern of XXX-XX-XXXX')
    return 'Data validation passed'

validate = PythonOperator(
    task_id='validate',
    python_callable=validate_data,
    dag=dag
)

job_name = 'your-dataflow-job-name'
project_id = 'your-gcp-project-id'
region = 'your-gcp-region'

run_dataflow = DataFlowJavaOperator(
    task_id='run_dataflow',
    jar='gs://your-bucket/dataflow_job.jar',
    options={
        'input': 'gs://your-gcs-bucket/your-source-object',
        'output': 'your-bq-project:your-bq-dataset.your-bq-table',
        'project': project_id,
        'region': region,
        'jobName': job_name,
        'tempLocation': 'gs://your-bucket/temp'
    },
    dag=dag
)

gcs_sensor >> validate >> run_dataflow
