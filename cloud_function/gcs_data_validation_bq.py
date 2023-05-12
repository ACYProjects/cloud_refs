import io
import json
import time

from google.cloud import bigquery
from google.cloud import storage

def main(event, context):
  bucket = event['bucket']
  object_name = event['name']

  with storage.open(bucket, object_name, 'rb') as file:
    data = file.read()

  table = bigquery.Table.from_json(data)

  for column in table.schema:
    if column.type != 'STRING':
      raise ValueError('Column {} must be of type STRING'.format(column.name))
    if column.mode != 'REQUIRED':
      raise ValueError('Column {} must be REQUIRED'.format(column.name))
    if not re.match('^[a-zA-Z0-9_]+$', column.name):
      raise ValueError('Column name {} must match [a-zA-Z0-9_]+'.format(column.name))

  with client.writer('my_table') as writer:
    writer.write([
        (row['timestamp'], row['value']) for row in table.rows
    ])

  with client.writer('my_invalid_table') as writer:
    writer.write([
        (row['timestamp'], row['value'].as_string()) for row in table.rows if not row.valid
    ])
