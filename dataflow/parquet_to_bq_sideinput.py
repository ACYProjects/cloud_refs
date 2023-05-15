import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io import ReadFromParquet
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime

class DataValidationDoFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        id, name, age = element

        if not isinstance(id, int):
            yield beam.pvalue.TaggedOutput('invalid_data', f'Invalid data type for id: {type(id)}')
        elif not isinstance(name, str):
            yield beam.pvalue.TaggedOutput('invalid_data', f'Invalid data type for name: {type(name)}')
        elif not isinstance(age, int):
            yield beam.pvalue.TaggedOutput('invalid_data', f'Invalid data type for age: {type(age)}')

        if not re.match(r'^[a-zA-Z ]+$', name):
            yield beam.pvalue.TaggedOutput('invalid_data', f'Invalid name format: {name}')

        yield element

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

pipeline = beam.Pipeline(options=options)

input_data = pipeline | 'ReadInput' >> ReadFromParquet('gs://my-bucket/input.parquet')

windowed_data = input_data | 'ApplyWindowing' >> beam.WindowInto(FixedWindows(60))

watermarked_data = windowed_data | 'ApplyWatermark' >> beam.WindowInto(
    beam.transforms.trigger.Repeatedly(
        AfterWatermark(
            past_end_of_window_delay=beam.Duration(seconds=10)
        )
    )
)

invalid_records = pipeline | 'CreateInvalidRecords' >> beam.Create([])

validated_data, invalid_data = watermarked_data | 'ValidateData' >> beam.ParDo(DataValidationDoFn()).with_outputs('invalid_data', main='validated_data')

output = validated_data | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
    table='project_id.dataset.table',
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
)

invalid_records | 'WriteInvalidToBigQuery' >> beam.io.WriteToBigQuery(
    table='project_id.dataset.invalid_table',
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
)

pipeline.run()
