from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    Factory,
    LinkedServiceReference,
    AzureStorageLinkedService,
    LinkedService,
    DatasetResource,
    AzureBlobDataset,
    PipelineResource,
    PipelineReference,
    Activity,
    CopyActivity,
    DatasetReference,
    BlobSource,
    BlobSink
)

subscription_id = ''
resource_group_name = ''
data_factory_name = ''
storage_account_name = ''
storage_account_key = ''

def create_data_factory(client):
    factory = Factory(location='East US')
    return client.factories.create_or_update(resource_group_name, data_factory_name, factory)

def create_linked_service(client):
    azure_storage_linked_service = LinkedService(
        type='AzureStorage',
        type_properties=AzureStorageLinkedService(
            connection_string=f'DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key}'
        )
    )
    return client.linked_services.create_or_update(
        resource_group_name, data_factory_name, 'AzureStorageLinkedService', azure_storage_linked_service
    )

def create_dataset(client, dataset_name, container_name, file_name):
    dataset = DatasetResource(
        properties=AzureBlobDataset(
            linked_service_name=LinkedServiceReference(reference_name='AzureStorageLinkedService'),
            folder_path=container_name,
            file_name=file_name
        )
    )
    return client.datasets.create_or_update(
        resource_group_name, data_factory_name, dataset_name, dataset
    )

def create_pipeline(client, source_dataset, sink_dataset):
    copy_activity = CopyActivity(
        name='CopyFromBlobToBlob',
        inputs=[DatasetReference(reference_name=source_dataset.name)],
        outputs=[DatasetReference(reference_name=sink_dataset.name)],
        source=BlobSource(),
        sink=BlobSink()
    )

    pipeline = PipelineResource(activities=[copy_activity])
    return client.pipelines.create_or_update(
        resource_group_name, data_factory_name, 'BlobToBlob', pipeline
    )

def main():
    credential = DefaultAzureCredential()

    adf_client = DataFactoryManagementClient(credential, subscription_id)
  
    print("Creating data factory...")
    create_data_factory(adf_client)

    print("Creating linked service...")
    create_linked_service(adf_client)

    print("Creating source dataset...")
    source_dataset = create_dataset(adf_client, 'SourceDataset', 'source-container', 'source.csv')

    print("Creating sink dataset...")
    sink_dataset = create_dataset(adf_client, 'SinkDataset', 'sink-container', 'output.csv')

    print("Creating pipeline...")
    pipeline = create_pipeline(adf_client, source_dataset, sink_dataset)

    print(f"Pipeline '{pipeline.name}' created successfully.")

if __name__ == "__main__":
    main()
