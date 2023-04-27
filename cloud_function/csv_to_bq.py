import csv
import re
from google.cloud import bigquery
from google.cloud import storage


def validate_data(row):
    """
    Validate a row of data based on data type, regex, and data structure
    """
    # Define the expected data structure
    expected_data_structure = ["string", "integer", "float", "string", "date"]

    # Define the regular expressions for each field
    regexes = [
        r"^[a-zA-Z]+$",
        r"^\d+$",
        r"^\d+\.\d+$",
        r"^[a-zA-Z0-9_]+$",
        r"^\d{4}-\d{2}-\d{2}$",
    ]

    # Validate each field in the row
    for i, value in enumerate(row):
        # Validate the data type
        expected_data_type = expected_data_structure[i]
        if not isinstance(value, eval(expected_data_type)):
            return False

        # Validate the regex
        if not re.match(regexes[i], str(value)):
            return False

    return True


def csv_to_bigquery(data, context):
    """
    Cloud Function that reads a CSV file from Cloud Storage, validates the data,
    and loads the validated data to a BigQuery table.
    """
    # Get the file name and bucket name from the Cloud Storage event
    file_name = data["name"]
    bucket_name = data["bucket"]

    # Download the CSV file from Cloud Storage
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    csv_string = blob.download_as_string().decode("utf-8")

    # Parse the CSV string into a list of rows
    rows = csv.reader(csv_string.splitlines())

    # Validate each row of data
    validated_rows = []
    for row in rows:
        if validate_data(row):
            validated_rows.append(row)

    # Load the validated data to BigQuery
    if len(validated_rows) > 0:
        bigquery_client = bigquery.Client()
        table_id = "mydataset.mytable"
        table = bigquery_client.get_table(table_id)
        errors = bigquery_client.insert_rows(table, validated_rows)
        if errors:
            print(f"Errors: {errors}")
        else:
            print(f"{len(validated_rows)} rows inserted to BigQuery table {table_id}.")
