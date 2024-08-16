#!/usr/bin/env python

import os
from dotenv import load_dotenv
from globus_sdk import ConfidentialAppAuthClient, ClientCredentialsAuthorizer
from globus_compute_sdk.sdk.login_manager import LoginManager, ComputeScopes
from globus_compute_sdk import Client

# Load environment variables from .env file
load_dotenv()


def get_login_manager(client_id, client_secret):
    """
    Create and return a LoginManager instance for Globus Compute.

    :param client_id: Globus client ID
    :param client_secret: Globus client secret
    :return: LoginManager instance
    """

    # Create a ConfidentialAppAuthClient
    auth_client = ConfidentialAppAuthClient(client_id, client_secret)

    # Create an authorizer
    authorizer = ClientCredentialsAuthorizer(
        auth_client,
        scopes=[ComputeScopes.all]
    )

    # Create and return a LoginManager
    return LoginManager()


def check_endpoint(endpoint_id):
    """
    Check the status of a Globus Compute endpoint.

    :param endpoint_id: UUID of the Globus Compute endpoint
    :return: None
    """
    try:
        # Retrieve client ID and secret from environment variables
        GLOBUS_CLIENT_ID = os.getenv("GLOBUS_CLIENT_ID")
        GLOBUS_CLIENT_SECRET = os.getenv("GLOBUS_CLIENT_SECRET")

        # Initialize the LoginManager
        login_manager = get_login_manager(GLOBUS_CLIENT_ID, GLOBUS_CLIENT_SECRET)

        # Initialize the Globus Compute client with the LoginManager
        compute_client = Client(login_manager=login_manager)

        # Check endpoint status
        endpoint_status = compute_client.get_endpoint_status(endpoint_id)
        print(f"Endpoint {endpoint_id} status: {endpoint_status}")
    except Exception as e:
        print(f"Failed to check endpoint status: {e}")


if __name__ == "__main__":
    endpoint_id = "UUID"
    check_endpoint(endpoint_id)
