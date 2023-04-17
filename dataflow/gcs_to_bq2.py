import apache_beam as beam

input_path = 'gs://your-bucket/input/*.txt'
output_table = 'your-project:your-dataset.your-table'

options = beam.options.pipeline_options.PipelineOptions()
google_cloud_options = options.view_as(beam.options.pipeline_options.GoogleCloudOptions)
google_cloud_options.project = 'your-project'
google_cloud_options.job_name = 'my-job'
google_cloud_options.staging_location = 'gs://your-bucket/staging'
google_cloud_options.temp_location = 'gs://your-bucket/temp'
options.view_as(beam.options.pipeline_options.StandardOptions).runner = 'DataflowRunner'

pipeline = beam.Pipeline(options=options)

lines = pipeline | 'ReadData' >> beam.io.ReadFromText(input_path)

filtered_lines = lines | 'FilterData' >> beam.Filter(lambda line: 'word' in line)
mapped_lines = filtered_lines | 'MapData' >> beam.Map(lambda line: line.split(',')[0:2])
grouped_lines = mapped_lines | 'GroupData' >> beam.Map(lambda line: (line[0], 1)) \
                                  | 'GroupByKey' >> beam.GroupByKey() \
                                  | 'CountWords' >> beam.CombineValues(sum)

schema = 'word:STRING,count:INTEGER'
table_schema = beam.io.gcp.bigquery.TableSchema.from_json(schema)
grouped_lines | 'WriteData' >> beam.io.WriteToBigQuery(output_table, schema=table_schema, write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

pipeline.run()
