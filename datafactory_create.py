"""Creating a data factory using the Azure SDK for Python. On the East US region"""

from azure.identity import ClientSecretCredential 
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import time

### Functions for interacting with Azure Data Factory
#Function to print information about a resource group
def print_item(group):
    """Print an Azure object instance."""
    print("\tName: {}".format(group.name))
    print("\tId: {}".format(group.id))
    if hasattr(group, 'location'):
        print("\tLocation: {}".format(group.location))
    if hasattr(group, 'tags'):
        print("\tTags: {}".format(group.tags))
    if hasattr(group, 'properties'):
        print_properties(group.properties)

def print_properties(props):
    """Print a ResourceGroup properties instance."""
    if props and hasattr(props, 'provisioning_state') and props.provisioning_state:
        print("\tProperties:")
        print("\t\tProvisioning State: {}".format(props.provisioning_state))
    print("\n\n")

def print_activity_run_details(activity_run):
    """Print activity run details."""
    print("\n\tActivity run details\n")
    print("\tActivity run status: {}".format(activity_run.status))
    if activity_run.status == 'Succeeded':
        print("\tNumber of bytes read: {}".format(activity_run.output['dataRead']))
        print("\tNumber of bytes written: {}".format(activity_run.output['dataWritten']))
        print("\tCopy duration: {}".format(activity_run.output['copyDuration']))
    else:
        print("\tErrors: {}".format(activity_run.error['message']))


def main():
    """Main execution routine"""
    #Azure subscription information
    subscription_id = 'ccf02283-8af1-4745-9822-7af91dbc8ed5'

    #Resource Group information
    rg_name = 'rg-datafactory-jasm-1'
    rg_params = {'location':'eastus'}

    #Data Factory information
    df_name = 'df-datafactory-jasm-1'
    df_params = {'location':'eastus'}

    #Create Resource Group with the Resource Management Client
    credentials = ClientSecretCredential( client_id = '8d83de75-3dc4-4f99-b649-c0d4a6c27176', 
                                          client_secret = 'ua58Q~GfJ87qSDUkPdQNFuUdy2nOxS6-ijTs3bmn', 
                                            tenant_id = '6ce1d913-7635-4a90-a93d-c00301fb9d5c')
    
    # Specify following for Soverign Clouds, import right cloud constant and then use it to connect.
    # from msrestazure.azure_cloud import AZURE_PUBLIC_CLOUD as CLOUD
    # credentials = DefaultAzureCredential(authority=CLOUD.endpoints.active_directory, tenant_id=tenant_id)
    resource_client = ResourceManagementClient(credentials, subscription_id)
    adf_client = DataFactoryManagementClient(credentials, subscription_id)

    # Create Resource Group
    print('Create Resource Group')
    resource_client.resource_groups.create_or_update(rg_name, rg_params)
    #Create Data Factory
    print('Create Data Factory')
    df_reource = Factory(location='eastus')
    df = adf_client.factories.create_or_update(rg_name, df_name, df_reource)
    print_item(df)
    while df.provisioning_state != 'Succeeded':
        df = adf_client.factories.get(rg_name, df_name)
        print('#######################')
        print('Provisioning state: {}'.format(df.provisioning_state))
        print('#######################')
        print_item(df)
        time.sleep(1)

    #Create an Azure Storage linked service
    ls_name = 'datafactorytest1jasm'

    # IMPORTANT: specify the name and key of your Azure Storage account.
    storage_string = SecureString(value='DefaultEndpointsProtocol=https;AccountName=lsdatafactoryjasm1;AccountKey=JRQQ0ZaPmev2iuq1f7yA8fLZPo+Hmj95bExzEnifcRCYV9ZbpML1zeINttgmwK/TpW18oaUsggY8+AStwIu/XQ==;EndpointSuffix=https://lsdatafactoryjasm1.blob.core.windows.net/')
    ls_azure_storage = LinkedServiceResource(properties=AzureStorageLinkedService(connection_string=storage_string)) 
    ls = adf_client.linked_services.create_or_update(rg_name, df_name, ls_name, ls_azure_storage)
    print("Create a linked service for storage account") 
    print_item(ls)

    # Create an Azure blob dataset (input)
    ds_name = 'ds_in'
    ds_ls = LinkedServiceReference(reference_name=ls_name, type=Type.LINKED_SERVICE_REFERENCE)
    blob_path = 'test/prueba'
    blob_filename = 'movies.csv'
    ds_azure_blob = DatasetResource(properties=AzureBlobDataset(
        linked_service_name=ds_ls, folder_path=blob_path, file_name=blob_filename))
    ds = adf_client.datasets.create_or_update(
        rg_name, df_name, ds_name, ds_azure_blob)
    print("Create an Azure blob dataset (input)")
    print_item(ds)

    # Create an Azure blob dataset (output)
    dsOut_name = 'ds_out'
    output_blobpath = 'test2/prueba2'
    dsOut_azure_blob = DatasetResource(properties=AzureBlobDataset(linked_service_name=ds_ls, folder_path=output_blobpath))
    dsOut = adf_client.datasets.create_or_update(
        rg_name, df_name, dsOut_name, dsOut_azure_blob)
    print("Create an Azure blob dataset (output)")
    print_item(dsOut)

    # Create a copy activity
    act_name = 'copyBlobtoBlob'
    blob_source = BlobSource()
    blob_sink = BlobSink()
    dsin_ref = DatasetReference(reference_name=ds_name, type=DatasetReferenceType) #azure.mgmt.datafactory.models.DatasetReferenceType
    dsOut_ref = DatasetReference(reference_name=dsOut_name, type=DatasetReferenceType)
    copy_activity = CopyActivity(name=act_name, inputs=[dsin_ref], outputs=[
                                 dsOut_ref], source=blob_source, sink=blob_sink)

    # Create a pipeline with the copy activity
    p_name = 'copyPipeline'
    params_for_pipeline = {}
    p_obj = PipelineResource(
        activities=[copy_activity], parameters=params_for_pipeline)
    p = adf_client.pipelines.create_or_update(rg_name, df_name, p_name, p_obj)
    print_item(p)
    
    #create a pipeline run
    run_response = adf_client.pipelines.create_run(rg_name, df_name, p_name, parameters={})

    # Monitor the pipeline run
    time.sleep(30)
    pipeline_run = adf_client.pipeline_runs.get(
        rg_name, df_name, run_response.run_id)
    print("\n\tPipeline run status: {}".format(pipeline_run.status))
    filter_params = RunFilterParameters(
        last_updated_after=datetime.now() - timedelta(1), last_updated_before=datetime.now() + timedelta(1))
    query_response = adf_client.activity_runs.query_by_pipeline_run(
        rg_name, df_name, pipeline_run.run_id, filter_params)
    print_activity_run_details(query_response.value[0])

main()