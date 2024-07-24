import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.io import ReadFromPubSub
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = 'your-project-id'
PUBSUB_TOPIC = 'your-pubsub-topic'
PUBSUB_SUBSCRIPTION = 'your-pubsub-subscription'
BQ_DATASET = 'your-bigquery-dataset'
BQ_TABLE = 'your-bigquery-table'

REGION = 'us-central1'
STAGING_LOCATION = 'gs://your-bucket/staging'
TEMP_LOCATION = 'gs://your-bucket/temp'
MAX_WORKERS = 5
MACHINE_TYPE = 'n1-standard-2'
NETWORK = 'default'  # Set to None if not needed
SUBNETWORK = 'regions/us-central1/subnetworks/default'  # Set to None if not needed
WORKER_DISK_TYPE = 'compute.googleapis.com/projects/{}/zones/{}/diskTypes/pd-ssd'
WORKER_DISK_SIZE_GB = 50

class ProductSale(beam.typehints.NamedTuple):
    product_id: str
    product_name: str
    sale_amount: float
    sale_date: str

class ParseAndValidateMessage(beam.DoFn):
    def process(self, element):
        try:
            # Parse JSON message
            message = json.loads(element.decode('utf-8'))
            
            # Create ProductSale object
            sale = ProductSale(
                product_id=message['product_id'],
                product_name=message['product_name'],
                sale_amount=float(message['sale_amount']),
                sale_date=message['sale_date']
            )
            
            # Validate sale amount
            if sale.sale_amount <= 0:
                logger.warning(f"Invalid sale amount for product {sale.product_id}: {sale.sale_amount}")
                return []
            
            # Validate date format
            try:
                datetime.strptime(sale.sale_date, '%Y-%m-%d')
            except ValueError:
                logger.error(f"Invalid date format for product {sale.product_id}: {sale.sale_date}")
                return []
            
            return [sale]
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"Error processing message: {e}")
            return []

class ProcessSales(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | "Parse and Validate" >> beam.ParDo(ParseAndValidateMessage())
            | "Add Processing Timestamp" >> beam.Map(
                lambda sale: sale._replace(
                    sale_date=f"{sale.sale_date} {datetime.now().strftime('%H:%M:%S')}"
                )
            )
        )

def run_pipeline():
    options = PipelineOptions([
        f'--project={PROJECT_ID}',
        f'--job_name=pubsub-to-bigquery-sales-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
        f'--region={REGION}',
        f'--staging_location={STAGING_LOCATION}',
        f'--temp_location={TEMP_LOCATION}',
        f'--max_num_workers={MAX_WORKERS}',
        f'--machine_type={MACHINE_TYPE}',
        f'--disk_size_gb={WORKER_DISK_SIZE_GB}',
        f'--worker_disk_type={WORKER_DISK_TYPE.format(PROJECT_ID, REGION)}',
        '--runner=DataflowRunner',
    ])

    if NETWORK:
        options.view_as(PipelineOptions).add_value_provider_option('network', NETWORK)
    if SUBNETWORK:
        options.view_as(PipelineOptions).add_value_provider_option('subnetwork', SUBNETWORK)

    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = True

    # BigQuery table schema
    table_schema = {
        'fields': [
            {'name': 'product_id', 'type': 'STRING'},
            {'name': 'product_name', 'type': 'STRING'},
            {'name': 'sale_amount', 'type': 'FLOAT'},
            {'name': 'sale_date', 'type': 'TIMESTAMP'}
        ]
    }

    with beam.Pipeline(options=options) as p:
        # Read from Pub/Sub
        sales = (
            p | "Read from Pub/Sub" >> ReadFromPubSub(
                topic=f'projects/{PROJECT_ID}/topics/{PUBSUB_TOPIC}',
                subscription=f'projects/{PROJECT_ID}/subscriptions/{PUBSUB_SUBSCRIPTION}'
            )
        )

        processed_sales = sales | "Process Sales" >> ProcessSales()

        _ = (
            processed_sales
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=f"{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}",
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == "__main__":
    run_pipeline()
