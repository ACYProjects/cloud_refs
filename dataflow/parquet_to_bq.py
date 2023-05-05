import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io import ReadFromParquet
from apache_beam.transforms import ParDo
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json

options = PipelineOptions()

google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'my-project'
google_cloud_options.region = 'us-central1'
google_cloud_options.job_name = 'my-job'
google_cloud_options.staging_location = 'gs://my-bucket/staging'
google_cloud_options.temp_location = 'gs://my-bucket/temp'

table_schema_json = '{"fields": [{"name": "id", "type": "INTEGER"}, {"name": "name", "type": "STRING"}, {"name": "age", "type": "INTEGER"}]}'
schema = parse_table_schema_from_json(table_schema_json)

dead_letter_table_schema_json = '{"fields": [{"name": "error_message", "type": "STRING"}, {"name": "data", "type": "RECORD", "fields": [{"name": "id", "type": "INTEGER"}, {"name": "name", "type": "STRING"}, {"name": "age", "type": "INTEGER"}]}]}'
dead_letter_schema = parse_table_schema_from_json(dead_letter_table_schema_json)

class ValidateDataTypesDoFn(beam.DoFn):
    def process(self, element):
        id, name, age = element
        if not isinstance(id, int):
            error_message = 'Invalid data type for id: {}'.format(type(id))
            yield beam.pvalue.TaggedOutput('dead_letter', (error_message, element))
        if not isinstance(name, str):
            error_message = 'Invalid data type for name: {}'.format(type(name))
            yield beam.pvalue.TaggedOutput('dead_letter', (error_message, element))
        if not isinstance(age, int):
            error_message = 'Invalid data type for age: {}'.format(type(age))
            yield beam.pvalue.TaggedOutput('dead_letter', (error_message, element))
        else:
            yield element

class ValidateNamesDoFn(beam.DoFn):
    def process(self, element):
        id, name, age = element
        if not re.match(r'^[a-zA-Z ]+$', name):
            error_message = 'Invalid name format: {}'.format(name)
            yield beam.pvalue.TaggedOutput('dead_letter', (error_message, element))
        else:
            yield element

pipeline = beam.Pipeline(options=options)

input_data = pipeline | 'ReadInput' >> ReadFromParquet('gs://my-bucket/input.parquet')

validated_data_types = input_data | 'ValidateDataTypes' >> ParDo(ValidateDataTypesDoFn()).with_outputs('dead_letter', main='valid_data')

validated_data = validated_data_types.valid_data | 'ValidateNames' >> ParDo(ValidateNamesDoFn())

output = validated_data | 'WriteOutputToBigQuery' >> beam.io.WriteToBigQuery(
table='my-project:my_dataset.my_table',
schema=schema,
create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
project='my-project',
batch_size=int(10)
)

dead_letter_output = validated_data_types.dead_letter | 'WriteDeadLetterToBigQuery' >> beam.io.WriteToBigQuery(
table='my-project:my_dataset.my_dead_letter_table',
schema=dead_letter_schema,
create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
project='my-project',
batch_size=int(10)
)

result = pipeline.run()
result.wait_until_finish()
