import os
import globus_sdk
import logging
from dotenv import load_dotenv

# Set up detailed logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Load environment variables
CLIENT_ID = os.getenv('GLOBUS_CLIENT_ID')
CLIENT_SECRET = os.getenv('GLOBUS_CLIENT_SECRET')
ENDPOINT_ID = os.getenv('GLOBUS_COMPUTE_ENDPOINT')

# Ensure that CLIENT_ID and CLIENT_SECRET are loaded
if not CLIENT_ID or not CLIENT_SECRET:
    logger.error("CLIENT_ID or CLIENT_SECRET environment variables are not set.")
    exit(1)
else:
    logger.debug(f"CLIENT_ID: {CLIENT_ID}")

# Define the necessary scopes for transfer
SCOPES = "urn:globus:auth:scope:transfer.api.globus.org:all"

# Initialize the ConfidentialAppAuthClient
auth_client = globus_sdk.ConfidentialAppAuthClient(CLIENT_ID, CLIENT_SECRET)

try:
    # Request a token with the required scopes
    token_response = auth_client.oauth2_client_credentials_tokens(requested_scopes=SCOPES)
    transfer_tokens = token_response.by_resource_server['transfer.api.globus.org']
    access_token = transfer_tokens['access_token']
    expires_at = transfer_tokens['expires_at_seconds']
    
    # Initialize the ClientCredentialsAuthorizer with the client and token
    authorizer = globus_sdk.ClientCredentialsAuthorizer(auth_client, SCOPES, access_token=access_token, expires_at=expires_at)

    # Initialize the TransferClient
    transfer_client = globus_sdk.TransferClient(authorizer=authorizer)

    # Fetch and display endpoint details
    try:
        endpoint = transfer_client.get_endpoint(ENDPOINT_ID)
        logger.info(f"Endpoint details: {endpoint}")
    except globus_sdk.GlobusAPIError as e:
        logger.error(f"Failed to retrieve endpoint details: {e}")

    # Check the access permissions for the endpoint
    try:
        acl_rules = transfer_client.endpoint_acl_list(ENDPOINT_ID)
        for rule in acl_rules:
            logger.info(f"ACL Rule: {rule}")
    except globus_sdk.GlobusAPIError as e:
        logger.error(f"Failed to retrieve ACL rules: {e}")
except globus_sdk.GlobusAPIError as e:
    logger.error(f"Failed to obtain tokens: {e}")
