import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery

project = 'my-project'
topic_name = 'my-topic'
subscription_name = 'my-subscription'

dataset_id = 'my-dataset'
table_id = 'my-table'
deadletter_table_id = 'my-deadletter-table'
table_schema = 'field1:STRING,field2:INTEGER,field3:FLOAT'
deadletter_table_schema = 'message:STRING,error:STRING,timestamp:TIMESTAMP'

options = PipelineOptions()
options.view_as(StandardOptions).runner = 'DataflowRunner'

with beam.Pipeline(options=options) as pipeline:

    messages = (pipeline
                | 'ReadFromPubSub' >> ReadFromPubSub(subscription=f'projects/{project}/subscriptions/{subscription_name}')
                | 'DecodeMessage' >> beam.Map(lambda message: message.decode('utf-8')))

    # Parse the input and filter invalid messages
    valid_messages, invalid_messages = (messages
                                        | 'ParseJSON' >> beam.Map(lambda message: eval(message))
                                        | 'FilterInvalidMessages' >> beam.Partition(lambda element, num_partitions: 0 if all(key in element for key in ('field1', 'field2', 'field3')) else 1, 2))

    (valid_messages
     | 'AddTimestamp' >> beam.Map(lambda element: beam.window.TimestampedValue(element, element['timestamp']))
     | 'Window' >> beam.WindowInto(window.FixedWindows(1))
     | 'WriteToBigQuery' >> WriteToBigQuery(table=table_id, dataset=dataset_id, schema=table_schema))

    (invalid_messages
     | 'AddError' >> beam.Map(lambda message: {'message': message, 'error': 'Invalid message', 'timestamp': str(datetime.datetime.now())})
     | 'WriteToDeadLetterTable' >> WriteToBigQuery(table=deadletter_table_id, dataset=dataset_id, schema=deadletter_table_schema))
