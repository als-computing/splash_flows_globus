#!/usr/bin/env python
import os
from dotenv import load_dotenv
from globus_compute_sdk.sdk.login_manager import LoginManager
from globus_compute_sdk import Client

# Load environment variables from .env file
load_dotenv()


def get_login_manager(environment: str = None) -> LoginManager:
    """
    Create and return a LoginManager instance for Globus Compute.

    :param environment: Optional environment name for token storage.
    :return: LoginManager instance
    """
    return LoginManager(environment=environment)


def check_endpoint(endpoint_id):
    """
    Check the status of a Globus Compute endpoint.

    :param endpoint_id: UUID of the Globus Compute endpoint
    :return: None
    """
    try:
        # Initialize the LoginManager
        login_manager = get_login_manager()

        # Ensure the user is logged in
        login_manager.ensure_logged_in()

        # Initialize the Globus Compute client with the LoginManager
        compute_client = Client(login_manager=login_manager)

        # Check endpoint status
        endpoint_status = compute_client.get_endpoint_status(endpoint_id)
        print(f"Endpoint {endpoint_id} status: {endpoint_status}")
    except Exception as e:
        print(f"Failed to check endpoint status: {e}")


if __name__ == "__main__":
    """
    Check the status of the Globus Compute endpoint specified by the
    GLOBUS_COMPUTE_ENDPOINT environment variable.

    IMPORTANT: run this in the terminal to login before running this script:
    export export GLOBUS_COMPUTE_CLIENT_ID="uuid" & GLOBUS_COMPUTE_CLIENT_SECRET="uuid"
    """
    endpoint_id = os.getenv("GLOBUS_COMPUTE_ENDPOINT")
    check_endpoint(endpoint_id)
