#pip install apache-airflow-providers-google

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.bash_operator import BashOperator

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
    'dataproc_dag',
    default_args=default_args,
    description='A DAG to create and remove a Dataproc cluster, submit a PySpark job, and use BashOperator and GCSFileSensor.',
    schedule_interval=timedelta(days=1),
)

create_cluster = DataprocCreateClusterOperator(
    task_id='create_cluster',
    project_id='your-project-id',
    region='your-region',
    cluster_name='your-cluster-name',
    num_workers=2,
    worker_machine_type='n1-standard-2',
    master_machine_type='n1-standard-2',
    image_version='1.5-debian10',
    dag=dag,
)

submit_job = DataprocSubmitJobOperator(
    task_id='submit_job',
    job={
        'reference': {
            'project_id': 'your-project-id',
        },
        'placement': {
            'cluster_name': 'your-cluster-name',
        },
        'pyspark_job': {
            'main_python_file_uri': 'gs://your-bucket/your-job.py',
        },
    },
    dag=dag,
)

wait_for_job = GCSObjectExistenceSensor(
    task_id='wait_for_job',
    bucket='your-bucket',
    object='your-output-file',
    dag=dag,
)

preprocess_data = BashOperator(
    task_id='preprocess_data',
    bash_command='python /path/to/preprocess.py',
    dag=dag,
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_cluster',
    project_id='your-project-id',
    region='your-region',
    cluster_name='your-cluster-name',
    dag=dag,
)

create_cluster >> submit_job >> wait_for_job >> preprocess_data >> delete_cluster
