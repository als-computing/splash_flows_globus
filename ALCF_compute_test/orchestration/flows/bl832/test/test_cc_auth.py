from dotenv import load_dotenv
import globus_sdk
import os
from globus_sdk.scopes import TransferScopes

# Load environment variables
load_dotenv()

# Set the client ID and fetch client secret from environment
CLIENT_ID = os.getenv('GLOBUS_CLIENT_ID')
CLIENT_SECRET = os.getenv('GLOBUS_CLIENT_SECRET')
# ENDPOINT_ID = "cf333f97-ff8c-40f7-b25a-21fe726f1ea0" # CGS endpoint ID
ENDPOINT_ID = "55c3adf6-31f1-4647-9a38-52591642f7e7"
# ENDPOINT_ID = "05d2c76a-e867-4f67-aa57-76edeb0beda0" # Eagle endpoint ID
# Define the necessary scope for transfer
# SCOPES = "urn:globus:auth:scope:transfer.api.globus.org:all"
SCOPES = ['urn:globus:auth:scope:transfer.api.globus.org:all[*https://auth.globus.org/scopes/05d2c76a-e867-4f67-aa57-76edeb0beda0/data_access]']

# Initialize the ConfidentialAppAuthClient for a service account
def initialize_transfer_client():
    confidential_client = globus_sdk.ConfidentialAppAuthClient(
        client_id=CLIENT_ID, client_secret=CLIENT_SECRET
    )
    cc_authorizer = globus_sdk.ClientCredentialsAuthorizer(confidential_client, SCOPES)
    return globus_sdk.TransferClient(authorizer=cc_authorizer)

# Initialize the TransferClient
transfer_client = initialize_transfer_client()

# Access the directory in the endpoint
def list_directory(transfer_client, endpoint_id, path):
    try:
        response = transfer_client.operation_ls(endpoint_id, path=path)
        print(f"Contents of {path} in endpoint {endpoint_id}:")
        if not response:
            print(f"No contents found in {path}.")
        for item in response:
            print(f"{item['type']} - {item['name']}")
    except globus_sdk.GlobusAPIError as err:
        print(f"Error accessing {path} in endpoint {endpoint_id}: {err.message}")
        if err.info.consent_required:
            print(
                "Got a ConsentRequired error with scopes:",
                err.info.consent_required.required_scopes,
            )
        elif err.code == "PermissionDenied":
            print(f"Permission denied for accessing {path}. Ensure proper permissions are set.")
        else:
            print(f"An unexpected error occurred: {err}")

# Create a new directory in the endpoint
def create_directory(transfer_client, endpoint_id, path, directory_name):
    try:
        full_path = os.path.join(path, directory_name)
        transfer_client.operation_mkdir(endpoint_id, full_path)
        print(f"Successfully created directory {full_path} in endpoint {endpoint_id}.")
    except globus_sdk.GlobusAPIError as err:
        print(f"Error creating directory {full_path} in endpoint {endpoint_id}: {err.message}")
        if err.info.consent_required:
            print(
                "Got a ConsentRequired error with scopes:",
                err.info.consent_required.required_scopes,
            )
        elif err.code == "PermissionDenied":
            print(f"Permission denied for creating directory {full_path}. Ensure proper permissions are set.")
        else:
            print(f"An unexpected error occurred: {err}")

# Remove a directory in the endpoint
def remove_directory(transfer_client, endpoint_id, path):
    try:
        delete_data = globus_sdk.DeleteData(transfer_client, endpoint_id, recursive=True)
        delete_data.add_item(path)
        transfer_result = transfer_client.submit_delete(delete_data)
        print(f"Successfully submitted request to remove directory {path} in endpoint {endpoint_id}. Task ID: {transfer_result['task_id']}")
    except globus_sdk.GlobusAPIError as err:
        print(f"Error removing directory {path} in endpoint {endpoint_id}: {err.message}")
        if err.info.consent_required:
            print(
                "Got a ConsentRequired error with scopes:",
                err.info.consent_required.required_scopes,
            )
        elif err.code == "PermissionDenied":
            print(f"Permission denied for removing directory {path}. Ensure proper permissions are set.")
        else:
            print(f"An unexpected error occurred: {err}")


# List the contents of the specified directories
try:
    # List the contents of the root directory
    print("Listing / directory:")
    list_directory(transfer_client, ENDPOINT_ID, "/")

    # Create a new directory in the root directory
    new_directory_name = "test_directory"
    # create_directory(transfer_client, ENDPOINT_ID, "/test_directory/", new_directory_name)
    remove_directory(transfer_client, ENDPOINT_ID, "/test_directory/")

    # List the contents again to verify the new directory
    print(f"\nListing / directory after creating {new_directory_name}:")
    list_directory(transfer_client, ENDPOINT_ID, "/")


except globus_sdk.TransferAPIError as err:
    print(f"Error: {err.message}")
    if err.info.consent_required:
        print(
            "Got a ConsentRequired error with scopes:",
            err.info.consent_required.required_scopes,
        )
    elif err.code == "PermissionDenied":
        print("Permission denied. Ensure proper permissions are set.")
    else:
        print("An unexpected error occurred.")


