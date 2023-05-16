from airflow import DAG
from airflow.operators.python import PythonOperator
import avro.schema
import pyodbc

def validate_data(avro_file_path, sql_server_connection_string):

    schema = avro.schema.parse(avro_file_path)

    connection = pyodbc.connect(sql_server_connection_string)
    cursor = connection.cursor()

    column_names = cursor.columns
    data_types = [column.type for column in column_names]

    for column_name, data_type in zip(column_names, data_types):
        if column_name not in schema.names:
            raise ValueError(f"Column '{column_name}' not found in Avro schema.")

        if data_type != schema[column_name].type:
            raise ValueError(f"Column '{column_name}' has incorrect data type. Expected '{schema[column_name].type}', got '{data_type}'.")

        if not re.match(schema[column_name].regex, cursor.execute(f"SELECT '{column_name}' FROM my_table").fetchone()[column_name]):
            raise ValueError(f"Column '{column_name}' does not match regular expression. Expected '{schema[column_name].regex}', got '{cursor.execute(f'SELECT {column_name} FROM my_table').fetchone()[column_name]}'.")


dag = DAG(
    dag_id="avro_gcs_to_sql_server",
    description="Copy data from an Avro file in GCS to a SQL Server database",
    start_date=datetime(2023, 5, 16),
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)

validate_data = PythonOperator(
    task_id="validate_data",
    python_callable=validate_data,
    op_args=[
        "gs://my-bucket/my-avro-file.avro",
        "my-sql-server-connection-string",
    ],
    dag=dag,
)

copy_data = CopyFromGoogleCloudStorageOperator(
    task_id="copy_data",
    source_bucket="my-bucket",
    source_object="my-avro-file.avro",
    table="my_table",
    connection_string="my-sql-server-connection-string",
    dag=dag,
)

validate_data >> copy_data


def validate_data(avro_file_path, sql_server_connection_string):

    schema = avro.schema.parse(avro_file_path)

    connection = pyodbc.connect(sql_server_connection_string)
    cursor = connection.cursor()

    column_names = cursor.columns
    data_types = [column.type for column in column_names]

    for column_name, data_type in zip(column_names, data_types):
        if column_name not in schema.names:
            raise ValueError(f"Column '{column_name}' not found in Avro schema.")

        if data_type != schema[column_name].type:
            raise ValueError(f"Column '{column_name}' has incorrect data type. Expected '{schema[column_name].type}', got '{data_type}'.")

        if not re.match(schema[column_name].regex, cursor.execute(f"SELECT '{column_name}' FROM my_table").fetchone()[column_name]):
            raise ValueError(f"Column '{column_name}' does not match regular expression. Expected '{schema[column_name].regex}', got '{cursor.execute(f'SELECT {column_name} FROM my_table').fetchone()[column_name]}'.")


