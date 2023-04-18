import re
import csv
from io import StringIO
from google.cloud import storage, bigquery

def validate_data(row):
    if not isinstance(row['id'], int):
        return False
    if not isinstance(row['name'], str):
        return False
    if not isinstance(row['age'], int):
        return False
    if not re.match(r"^\d{3}-\d{2}-\d{4}$", row['ssn']):
        return False
    return True

def load_csv_to_bq(data, context):
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()

    bucket_name = data['bucket']
    file_name = data['name']

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    file_content = blob.download_as_string().decode('utf-8')

    valid_rows = []
    invalid_rows = []
    reader = csv.DictReader(StringIO(file_content))
    for row in reader:
        if validate_data(row):
            valid_rows.append(row)
        else:
            invalid_rows.append(row)

    if len(valid_rows) > 0:
        # Set up BigQuery load job
        dataset_id = 'my_dataset'
        table_id = 'my_table'
        dataset_ref = bigquery_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = 'WRITE_APPEND'
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.autodetect = True

        temp_file = NamedTemporaryFile(delete=False)
        writer = csv.DictWriter(temp_file, fieldnames=valid_rows[0].keys())
        writer.writeheader()
        writer.writerows(valid_rows)
        temp_file.close()

        temp_blob = bucket.blob('temp/' + file_name)
        with open(temp_file.name, 'rb') as f:
            temp_blob.upload_from_file(f)
            
        load_job = bigquery_client.load_table_from_uri(
            'gs://{}/{}'.format(bucket_name, temp_blob.name),
            table_ref,
            job_config=job_config
        )

        load_job.result()

        temp_file.unlink()
        temp_blob.delete()

    if len(invalid_rows) > 0:
        print('Invalid rows:')
        for row in invalid_rows:
            print(row)

    print('Data loaded into BigQuery successfully')
