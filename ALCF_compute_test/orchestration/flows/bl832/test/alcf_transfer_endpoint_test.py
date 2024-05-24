import os
import globus_sdk
import logging
from globus_compute_sdk import Client as ComputeClient
from dotenv import load_dotenv

# Set up detailed logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Load environment variables
CLIENT_ID = os.getenv('GLOBUS_CLIENT_ID')
CLIENT_SECRET = os.getenv('GLOBUS_CLIENT_SECRET')
COMPUTE_ENDPOINT_ID = os.getenv('GLOBUS_COMPUTE_ENDPOINT')

# Ensure that CLIENT_ID and CLIENT_SECRET are loaded
if not CLIENT_ID or not CLIENT_SECRET:
    logger.error("CLIENT_ID or CLIENT_SECRET environment variables are not set.")
    exit(1)
else:
    logger.debug(f"CLIENT_ID: {CLIENT_ID}")

# Define the necessary scopes for Globus Compute
SCOPES = "https://auth.globus.org/scopes/compute.api.globus.org/all"

# Initialize the ConfidentialAppAuthClient
auth_client = globus_sdk.ConfidentialAppAuthClient(CLIENT_ID, CLIENT_SECRET)

try:
    # Request a token with the required scopes
    token_response = auth_client.oauth2_client_credentials_tokens(requested_scopes=SCOPES)
    compute_tokens = token_response.by_resource_server['compute.api.globus.org']
    access_token = compute_tokens['access_token']
    
    # Initialize the ComputeClient with the token
    compute_client = ComputeClient(authorizer=globus_sdk.AccessTokenAuthorizer(access_token))

    # Test the Compute endpoint by listing tasks or any other available method
    try:
        # Assuming you want to list tasks or similar action
        tasks = compute_client.get_tasks()
        logger.info(f"Compute tasks: {tasks}")
    except globus_sdk.GlobusAPIError as e:
        logger.error(f"Failed to retrieve tasks: {e}")
except globus_sdk.GlobusAPIError as e:
    logger.error(f"Failed to obtain tokens: {e}")
