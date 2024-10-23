from google.cloud import storage, bigquery
from google.api_core import retry
import pandas as pd
import pyarrow.parquet as pq
import datetime
import logging
import os

class GCStoBQPipeline:
    def __init__(self, project_id, gcs_bucket, source_prefix, dest_dataset, dest_table):
        self.project_id = project_id
        self.gcs_bucket = gcs_bucket
        self.source_prefix = source_prefix
        self.dest_dataset = dest_dataset
        self.dest_table = dest_table
        
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
        """List new parquet files that haven't been processed yet."""
        bucket = self.storage_client.bucket(self.gcs_bucket)
        blobs = bucket.list_blobs(prefix=self.source_prefix)
        all_files = [blob.name for blob in blobs if blob.name.endswith('.parquet')]
        processed_files = self.get_processed_files()
        
        return [f for f in all_files if f not in processed_files]

    def validate_data(self, df):
        validation_errors = []
        
        # Check for null values in critical columns
        critical_columns = ['id', 'timestamp']  # Adjust based on your schema
        for col in critical_columns:
            if col in df.columns and df[col].isnull().any():
                validation_errors.append(f"Null values found in critical column: {col}")

        # Check for duplicate IDs
        if 'id' in df.columns and df['id'].duplicated().any():
            validation_errors.append("Duplicate IDs found in data")

        # Check data types
        expected_dtypes = {
            'id': 'int64',
            'timestamp': 'datetime64[ns]'
            # Add more columns and expected types
        }
        
        for col, dtype in expected_dtypes.items():
            if col in df.columns and str(df[col].dtype) != dtype:
                validation_errors.append(f"Invalid data type for {col}: expected {dtype}, got {df[col].dtype}")

        return validation_errors

    def load_to_bigquery(self, df, source_file):
        df['_processed_timestamp'] = datetime.datetime.now()
        df['_source_file'] = source_file

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
        )

        try:
            job = self.bq_client.load_table_from_dataframe(
                df, 
                f"{self.project_id}.{self.dest_dataset}.{self.dest_table}",
                job_config=job_config
            )
            job.result() 

            metadata_df = pd.DataFrame({
                'source_file': [source_file],
                'processed_timestamp': [datetime.datetime.now()],
                'record_count': [len(df)]
            })
            
            self.bq_client.load_table_from_dataframe(
                metadata_df,
                f"{self.project_id}.{self.dest_dataset}.{self.dest_table}_metadata",
                job_config=job_config
            ).result()

            self.logger.info(f"Successfully loaded {len(df)} rows from {source_file}")
            return True

        except Exception as e:
            self.logger.error(f"Error loading {source_file} to BigQuery: {str(e)}")
            return False

    def process_file(self, file_path):
        try:
            bucket = self.storage_client.bucket(self.gcs_bucket)
            blob = bucket.blob(file_path)
            
            temp_path = f"/tmp/{os.path.basename(file_path)}"
            blob.download_to_filename(temp_path)
            
            df = pd.read_parquet(temp_path)
            os.remove(temp_path)  # Clean up

            validation_errors = self.validate_data(df)
            if validation_errors:
                self.logger.error(f"Validation failed for {file_path}: {validation_errors}")
                return False

            return self.load_to_bigquery(df, file_path)

        except Exception as e:
            self.logger.error(f"Error processing {file_path}: {str(e)}")
            return False

    def run_pipeline(self):
        self.logger.info("Starting pipeline execution...")
        
        new_files = self.list_new_files()
        self.logger.info(f"Found {len(new_files)} new files to process")
        
        success_count = 0
        for file_path in new_files:
            if self.process_file(file_path):
                success_count += 1
        
        self.logger.info(f"Pipeline completed. Successfully processed {success_count}/{len(new_files)} files")
        return success_count

def main():
    config = {
        'project_id': 'your-project-id',
        'gcs_bucket': 'your-bucket-name',
        'source_prefix': 'path/to/parquet/files/',
        'dest_dataset': 'your_dataset',
        'dest_table': 'your_table'
    }
    
    pipeline = GCStoBQPipeline(**config)
    pipeline.run_pipeline()

if __name__ == "__main__":
    main()
