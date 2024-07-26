from azure.identity import DefaultAzureCredential
from azure.mgmt.datacatalog import DataCatalogManagementClient
from azure.mgmt.datacatalog.models import DataCatalogResource, Asset, AssetProperties

credential = DefaultAzureCredential()

subscription_id = ""
resource_group_name = ""
catalog_name = ""

data_catalog_client = DataCatalogManagementClient(credential, subscription_id)

def register_data_asset():
    asset_properties = AssetProperties(
        type_name="SQL Server Table",
        name="CustomerData",
        data_source_name="MyDatabase",
        container_name="dbo",
        description="Contains customer information"
    )

    asset = Asset(properties=asset_properties)

    data_catalog_client.data_assets.create_or_update(
        resource_group_name=resource_group_name,
        catalog_name=catalog_name,
        data_asset_name="CustomerData",
        asset=asset
    )
    print("Data asset registered successfully.")

def add_annotations():
    annotations = {
        "tags": ["customer", "sales", "sensitive"],
        "expert": "John Doe",
        "last_updated": "2024-07-26"
    }

    data_catalog_client.data_assets.update_annotations(
        resource_group_name=resource_group_name,
        catalog_name=catalog_name,
        data_asset_name="CustomerData",
        annotations=annotations
    )
    print("Annotations added successfully.")

def search_data_assets(search_term):
    search_results = data_catalog_client.data_assets.search(
        resource_group_name=resource_group_name,
        catalog_name=catalog_name,
        filter=f"contains(name, '{search_term}')"
    )

    print(f"Search results for '{search_term}':")
    for asset in search_results:
        print(f"- {asset.name}: {asset.description}")

# Main execution
if __name__ == "__main__":
    register_data_asset()
    add_annotations()
    search_data_assets("Customer")
