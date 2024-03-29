import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions

options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'your-gcp-project-id'
google_cloud_options.job_name = 'your-job-name'
google_cloud_options.staging_location = 'gs://your-bucket/staging'
google_cloud_options.temp_location = 'gs://your-bucket/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'

with beam.Pipeline(options=options) as pipeline:
    lines = pipeline | 'ReadFromGCS' >> beam.io.ReadFromText('gs://your-bucket/input/*.txt')

    transformed_lines = lines | 'TransformData' >> beam.Map(lambda line: line.upper())

    transformed_lines | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
        'your-project:your-dataset.your-table',
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )
