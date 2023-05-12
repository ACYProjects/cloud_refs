from google.cloud import bigquery
import io
import json
import time

def main(event, context):
  message = event['data']
  
  data = json.loads(message)
  
  timestamp = data['timestamp']

  client = bigquery.Client()

  table = client.create_table(
      table_id='my_table',
      schema=[
          bigquery.SchemaField('timestamp', 'TIMESTAMP'),
          bigquery.SchemaField('value', 'FLOAT'),
      ])

  with client.writer('my_table') as writer:
    writer.write([
        (timestamp, data['value']),
    ])

  client.set_watermark('my_table', timestamp)

  window = client.create_tumbling_window(
      window_id='my_window',
      start_time=timestamp,
      end_time=timestamp + 60 * 60 * 1000,
      step_size=60 * 60 * 1000)

  average = client.compute_average('my_table', window)

  print(average)
