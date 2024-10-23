from google.cloud import storage, bigquery
from google.api_core import retry
import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.parquet import ReadFromParquet
import datetime
import logging
import os
import argparse

class GCStoBQPipeline:
    def __init__(self, project_id, gcs_bucket, source_prefix, dest_dataset, dest_table, file_location=None):
      
        self.project_id = project_id
        self.gcs_bucket = gcs_bucket
        self.source_prefix = source_prefix
        self.dest_dataset = dest_dataset
        self.dest_table = dest_table
        self.file_location = file_location
        
        self.storage_client = storage.Client(project=project_id)
        self.bq_client = bigquery.Client(project=project_id)
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def get_processed_files(self):
        query = f"""
        SELECT DISTINCT source_file 
        FROM `{self.project_id}.{self.dest_dataset}.{self.dest_table}_metadata`
        """
        try:
            processed_files = [row['source_file'] for row in self.bq_client.query(query).result()]
            return set(processed_files)
        except Exception as e:
            self.logger.warning(f"Metadata table might not exist yet: {str(e)}")
            return set()

    def list_new_files(self):
        if self.file_location:
            if not self.file_location.endswith('.parquet'):
                self.logger.error(f"Specified file is not a parquet file: {self.file_location}")
                return []
                
            bucket = self.storage_client.bucket(self.gcs_bucket)
            blob = bucket.blob(self.file_location)
            
            if not blob.exists():
                self.logger.error(f"Specified file does not exist: {self.file_location}")
                return []
                
            processed_files = self.get_processed_files()
            return [f"gs://{self.gcs_bucket}/{self.file_location}"] if self.file_location not in processed_files else []
        else:
            bucket = self.storage_client.bucket(self.gcs_bucket)
            blobs = bucket.list_blobs(prefix=self.source_prefix)
            all_files = [f"gs://{self.gcs_bucket}/{blob.name}" for blob in blobs if blob.name.endswith('.parquet')]
            processed_files = self.get_processed_files()
            return [f for f in all_files if f not in processed_files]

class ValidateData(beam.DoFn):
    def process(self, element):
        validation_errors = []
        
        required_fields = ['id', 'timestamp']
        for field in required_fields:
            if field not in element or element[field] is None:
                validation_errors.append(f"Missing or null value in required field: {field}")

        # Check for duplicate IDs (note: this will need to be handled differently in a distributed context)
        # You might want to use a GroupByKey operation instead for true duplicate detection

        # Add more validation rules as needed
        
        if validation_errors:
            logging.error(f"Validation errors found: {validation_errors}")
            return []  # Skip invalid records
        return [element]

class AddMetadata(beam.DoFn):
    def process(self, element, source_file):
        element['_processed_timestamp'] = datetime.datetime.now().isoformat()
        element['_source_file'] = source_file
        yield element

class CreateMetadataRecord(beam.DoFn):
    def process(self, element, source_file):
        yield {
            'source_file': source_file,
            'processed_timestamp': datetime.datetime.now().isoformat(),
            'record_count': element  # element will be the count from Count transform
        }

def run_pipeline(self):
    self.logger.info("Starting pipeline execution...")
    
    new_files = self.list_new_files()
    
    if not new_files:
        self.logger.info("No new files to process")
        return 0

    # Pipeline options
    options = PipelineOptions([
        '--project', self.project_id,
        '--runner', 'DirectRunner',  # Use DataflowRunner for production
        '--temp_location', f'gs://{self.gcs_bucket}/temp'
    ])

    success_count = 0
    for file_path in new_files:
        try:
            with beam.Pipeline(options=options) as pipeline:
                records = (pipeline 
                    | f'Read {file_path}' >> ReadFromParquet(file_path)
                    | 'Validate Data' >> beam.ParDo(ValidateData())
                    | 'Add Metadata' >> beam.ParDo(AddMetadata(), file_path))

                # Count records for metadata
                record_count = (records 
                    | 'Count Records' >> beam.combiners.Count.Globally())

                _ = (records | 'Write to BigQuery' >> WriteToBigQuery(
                    table=f'{self.project_id}:{self.dest_dataset}.{self.dest_table}',
                    schema='SCHEMA_AUTODETECT',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                ))

                _ = (record_count 
                    | 'Create Metadata' >> beam.ParDo(CreateMetadataRecord(), file_path)
                    | 'Write Metadata' >> WriteToBigQuery(
                        table=f'{self.project_id}:{self.dest_dataset}.{self.dest_table}_metadata',
                        schema='SCHEMA_AUTODETECT',
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                    ))

            success_count += 1
            self.logger.info(f"Successfully processed {file_path}")

        except Exception as e:
            self.logger.error(f"Error processing {file_path}: {str(e)}")

    self.logger.info(f"Pipeline completed. Successfully processed {success_count}/{len(new_files)} files")
    return success_count

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process parquet files from GCS to BigQuery using Apache Beam')
    parser.add_argument('--project-id', required=True, help='GCP Project ID')
    parser.add_argument('--gcs-bucket', required=True, help='GCS bucket name')
    parser.add_argument('--source-prefix', required=True, help='Source prefix path in GCS')
    parser.add_argument('--dest-dataset', required=True, help='BigQuery destination dataset')
    parser.add_argument('--dest-table', required=True, help='BigQuery destination table')
    parser.add_argument('--file-location', help='Specific file location in GCS to process')
    
    args = parser.parse_args()
    
    # Initialize and run pipeline
    pipeline = GCStoBQPipeline(
        project_id=args.project_id,
        gcs_bucket=args.gcs_bucket,
        source_prefix=args.source_prefix,
        dest_dataset=args.dest_dataset,
        dest_table=args.dest_table,
        file_location=args.file_location
    )
    
    pipeline.run_pipeline()

if __name__ == "__main__":
    main()
