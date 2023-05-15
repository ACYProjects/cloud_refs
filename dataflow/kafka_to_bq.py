import apache_beam as beam
from apache_beam.io import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
import re

class DataValidationDoFn(beam.DoFn):
    def process(self, element, *args, **kwargs):
        id, name, age = element

        if not isinstance(id, int):
            yield f'Invalid data type for id: {type(id)}'
        elif not isinstance(name, str):
            yield f'Invalid data type for name: {type(name)}'
        elif not isinstance(age, int):
            yield f'Invalid data type for age: {type(age)}'

        if not re.match(r'^[a-zA-Z ]+$', name):
            yield f'Invalid name format: {name}'
        yield element

options = PipelineOptions()

pipeline = beam.Pipeline(options=options)

input_data = pipeline | 'ReadInput' >> ReadFromKafka(
    consumer_config={
        'bootstrap.servers': 'kafka-server:9092',
        'group.id': 'my-group'
    },
    topics=['my-topic']
)

tumbling_windowed_data = input_data | 'ApplyTumblingWindow' >> beam.WindowInto(
    windowfn=beam.window.FixedWindows(10)
)

validated_data = tumbling_windowed_data | 'ValidateData' >> beam.ParDo(DataValidationDoFn())

output = validated_data | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
    table='project_id.dataset.table',
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
)

pipeline.run()
