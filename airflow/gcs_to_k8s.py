from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'gcs_file_sensor_to_kubernetes',
    default_args=default_args,
    description='Trigger Kubernetes cluster after GCS file sensor',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

gcs_file_sensor = GoogleCloudStorageObjectSensor(
    task_id='gcs_file_sensor',
    bucket='your-gcs-bucket',
    object='path/to/your/file',
    google_cloud_conn_id='google_cloud_default',
    dag=dag,
)

kubernetes_task = KubernetesPodOperator(
    task_id='kubernetes_task',
    name='kubernetes_task',
    namespace='default',
    image='your-docker-image',
    cmds=['your-command'],
    arguments=['your-arguments'],
    in_cluster=True,
    config_file='/path/to/your/kubeconfig',
    is_delete_operator_pod=True,
    dag=dag,
)

gcs_file_sensor >> kubernetes_task
