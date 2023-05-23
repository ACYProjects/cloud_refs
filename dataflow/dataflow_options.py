import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromParquet
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        help='Input file to process (Parquet file on GCS).')
    parser.add_argument(
        '--output',
        dest='output',
        help='Output BigQuery table to write results to.')
    parser.add_argument(
        '--temp_location',
        dest='temp_location',
        help='Temporary location for storing files during pipeline execution.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'Read from Parquet' >> ReadFromParquet(known_args.input)
         | 'Write to BigQuery' >> WriteToBigQuery(
             known_args.output,
             schema='column1:STRING, column2:INTEGER',
             create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=BigQueryDisposition.WRITE_APPEND)
        )

if __name__ == '__main__':
    run()
