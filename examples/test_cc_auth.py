from dotenv import load_dotenv
import globus_sdk
import os
import time
from prefect import flow, task, get_run_logger

# Load environment variables
load_dotenv()

# Set the client ID and fetch client secret from environment
CLIENT_ID = os.getenv('GLOBUS_CLIENT_ID')
CLIENT_SECRET = os.getenv('GLOBUS_CLIENT_SECRET')

# SCOPES = ['urn:globus:auth:scope:transfer.api.globus.org:all[*https://auth.globus.org/scopes/55c3adf6-31f1-4647-9a38-52591642f7e7/data_access]']
SCOPES = "urn:globus:auth:scope:transfer.api.globus.org:all"
# ENDPOINT_ID = "d40248e6-d874-4f7b-badd-2c06c16f1a58" # NERSC DTN alsdev Collab
ENDPOINT_ID = "55c3adf6-31f1-4647-9a38-52591642f7e7" #ALCF Iribeta CGS


def initialize_transfer_client():
    confidential_client = globus_sdk.ConfidentialAppAuthClient(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    cc_authorizer = globus_sdk.ClientCredentialsAuthorizer(confidential_client, SCOPES)
    return globus_sdk.TransferClient(authorizer=cc_authorizer)


@task
def check_permissions(transfer_client, endpoint_id):
    logger = get_run_logger()
    try:
        endpoint = transfer_client.get_endpoint(endpoint_id)
        logger.info(f"Endpoint ID: {endpoint['id']}")
        logger.info(f"Endpoint display name: {endpoint['display_name']}")
        logger.info(f"Endpoint owner: {endpoint['owner_string']}")
        # Print other relevant information about the endpoint
    except globus_sdk.GlobusAPIError as err:
        logger.error(f"Error fetching endpoint information: {err.message}")
        raise


@task
def list_directory(transfer_client, endpoint_id, path):
    logger = get_run_logger()
    start_time = time.time()
    try:
        response = transfer_client.operation_ls(endpoint_id, path=path)
        logger.info(f"Contents of {path} in endpoint {endpoint_id}:")
        if not response:
            logger.info(f"No contents found in {path}.")
        for item in response:
            logger.info(f"{item['type']} - {item['name']}")
    except globus_sdk.GlobusAPIError as err:
        logger.error(f"Error accessing {path} in endpoint {endpoint_id}: {err.message}")
        if err.info.consent_required:
            logger.error(f"Got a ConsentRequired error with scopes: {err.info.consent_required.required_scopes}")
        elif err.code == "PermissionDenied":
            logger.error(f"Permission denied for accessing {path}. Ensure proper permissions are set.")
        elif err.http_status == 500:
            logger.error(f"Server error when accessing {path} in endpoint {endpoint_id}.")
        else:
            logger.error(f"An unexpected error occurred: {err}")
    finally:
        elapsed_time = time.time() - start_time
        logger.info(f"list_directory task took {elapsed_time:.2f} seconds")

@task
def create_directory(transfer_client, endpoint_id, base_path, directory_name):
    logger = get_run_logger()
    start_time = time.time()
    try:
        # Ensure base path is realistic and ends with '/'
        if not base_path.endswith('/'):
            base_path += '/'
        if base_path.startswith('/'):
            base_path = base_path.lstrip('/')
        
        full_path = base_path + directory_name

        # Validate the path
        if full_path.startswith('/'):
            raise ValueError(f"Invalid directory path: {full_path}")

        # Attempt to create the directory
        transfer_client.operation_mkdir(endpoint_id, full_path)
        logger.info(f"Successfully created directory {full_path} in endpoint {endpoint_id}.")
    except ValueError as ve:
        logger.error(f"ValueError: {ve}")
    except globus_sdk.GlobusAPIError as err:
        logger.error(f"Error creating directory {full_path} in endpoint {endpoint_id}: {err.message}")
        if err.info.consent_required:
            logger.error(f"Got a ConsentRequired error with scopes: {err.info.consent_required.required_scopes}")
        elif err.code == "PermissionDenied":
            logger.error(f"Permission denied for creating directory {full_path}. Ensure proper permissions are set.")
        elif err.http_status == 500:
            logger.error(f"Server error when creating directory {full_path} in endpoint {endpoint_id}.")
        else:
            logger.error(f"An unexpected error occurred: {err}")
    finally:
        elapsed_time = time.time() - start_time
        logger.info(f"create_directory task took {elapsed_time:.2f} seconds")


@task
def remove_directory(transfer_client, endpoint_id, path):
    logger = get_run_logger()
    start_time = time.time()
    try:
        delete_data = globus_sdk.DeleteData(transfer_client, endpoint_id, recursive=True)
        delete_data.add_item(path)
        transfer_result = transfer_client.submit_delete(delete_data)
        logger.info(f"Successfully submitted request to remove directory {path} in endpoint {endpoint_id}. Task ID: {transfer_result['task_id']}")
    except globus_sdk.GlobusAPIError as err:
        logger.error(f"Error removing directory {path} in endpoint {endpoint_id}: {err.message}")
        if err.info.consent_required:
            logger.error(f"Got a ConsentRequired error with scopes: {err.info.consent_required.required_scopes}")
        elif err.code == "PermissionDenied":
            logger.error(f"Permission denied for removing directory {path}. Ensure proper permissions are set.")
        elif err.http_status == 500:
            logger.error(f"Server error when removing directory {path} in endpoint {endpoint_id}.")
        else:
            logger.error(f"An unexpected error occurred: {err}")
    finally:
        elapsed_time = time.time() - start_time
        logger.info(f"remove_directory task took {elapsed_time:.2f} seconds")


@flow
def main_flow():
    transfer_client = initialize_transfer_client()
    endpoint_id = ENDPOINT_ID
    base_path = ""

    # Check permissions for the endpoint
    check_permissions(transfer_client, endpoint_id)

    # List the contents of the root directory
    logger = get_run_logger()
    # logger.info("Listing / directory:")
    # list_directory(transfer_client, endpoint_id, base_path)

    # Create a new directory in the root directory
    # new_directory_name = "test/"
    # create_directory(transfer_client, endpoint_id, base_path, new_directory_name)

    remove_directory(transfer_client, endpoint_id, "bl832_test/scratch/BLS-00564_dyparkinson/")

    # List the contents again to verify the new directory
    # logger.info(f"\nListing / directory after creating {new_directory_name}:")
    # list_directory(transfer_client, endpoint_id, base_path+new_directory_name)

if __name__ == "__main__":
    main_flow()
