import os
import io
import datetime
from typing import NamedTuple
from google.cloud import storage
from google.cloud import firestore
import pyarrow.parquet as pq

class ValidationResult(NamedTuple):
    is_valid: bool
    error_message: str
    validation_timestamp: datetime.datetime

def validate_record(record):
    if 'name' not in record or not record['name']:
        return ValidationResult(False, 'Name is required.', None)

    if 'age' not in record or not isinstance(record['age'], int) or record['age'] < 0:
        return ValidationResult(False, 'Age must be a non-negative integer.', None)

    if 'salary' not in record or not isinstance(record['salary'], (int, float)) or record['salary'] < 0:
        return ValidationResult(False, 'Salary must be a non-negative number.', None)

    return ValidationResult(True, None, datetime.datetime.now())

def read_parquet_file(bucket_name, file_path):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_path)
    with io.BytesIO(blob.download_as_bytes()) as f:
        table = pq.read_table(f)
        return table.to_pandas().to_dict('records')

def write_to_firestore(collection_name, data):
    db = firestore.Client()
    collection_ref = db.collection(collection_name)
    for record in data:
        doc_ref = collection_ref.document(str(hash(frozenset(record.items()))))
        doc_ref.set(record)

def process_parquet_file(request):
    bucket_name = os.getenv('GCS_BUCKET_NAME')
    firestore_collection = os.getenv('FIRESTORE_COLLECTION')
    file_path = request.args.get('filePath')

    records = read_parquet_file(bucket_name, file_path)
    valid_records = []
    for record in records:
        validation_result = validate_record(record)
        if validation_result.is_valid:
            record['validationTimestamp'] = validation_result.validation_timestamp.isoformat()
            valid_records.append(record)
        else:
            return (
                f"Invalid data in the Parquet file: {validation_result.error_message}",
                400
            )

    write_to_firestore(firestore_collection, valid_records)
    return "Data processed and written to Firestore successfully.", 200
