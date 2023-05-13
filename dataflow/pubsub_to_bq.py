import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery

# Set the project ID, topic and subscription name
project = 'my-project'
topic_name = 'my-topic'
subscription_name = 'my-subscription'

dataset_id = 'my-dataset'
table_id = 'my-table'
table_schema = 'field1:STRING,field2:INTEGER,field3:FLOAT'

options = PipelineOptions()
options.view_as(StandardOptions).runner = 'DataflowRunner'

with beam.Pipeline(options=options) as pipeline:

    messages = (pipeline
                | 'ReadFromPubSub' >> ReadFromPubSub(subscription=f'projects/{project}/subscriptions/{subscription_name}')
                | 'DecodeMessage' >> beam.Map(lambda message: message.decode('utf-8')))

    (messages
     | 'ParseJSON' >> beam.Map(lambda message: eval(message))
     | 'AddTimestamp' >> beam.Map(lambda element: beam.window.TimestampedValue(element, element['timestamp']))
     | 'Window' >> beam.WindowInto(window.FixedWindows(1))
     | 'WriteToBigQuery' >> WriteToBigQuery(table=table_id, dataset=dataset_id, schema=table_schema))
