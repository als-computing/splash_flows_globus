
from dotenv import load_dotenv
import typer
from typing import Optional

from globus_compute_sdk.sdk.login_manager import LoginManager
from globus_compute_sdk import Client
from prefect import task, flow, get_run_logger

load_dotenv()

app = typer.Typer()


@task
def get_login_manager(environment: Optional[str] = None) -> LoginManager:
    """
    Create and return a LoginManager instance for Globus Compute.

    :param environment: Optional environment name for token storage.
    :return: LoginManager instance
    """
    return LoginManager(environment=environment)


@flow(name="check-compute-status")
def check_globus_compute_status(endpoint_id: str) -> bool:
    """
    Check the status of a Globus Compute endpoint and determine if it is online.

    :param endpoint_id: UUID of the Globus Compute endpoint.
    :return: bool - True if the status is 'online', False otherwise.
    """
    logger = get_run_logger()
    try:
        # Initialize the LoginManager
        login_manager = get_login_manager()

        # Ensure the user is logged in
        login_manager.ensure_logged_in()

        # Initialize the Globus Compute client with the LoginManager
        compute_client = Client(login_manager=login_manager)

        # Check endpoint status
        endpoint_status = compute_client.get_endpoint_status(endpoint_id)

        # Log the full status details
        logger.info(f"Endpoint {endpoint_id} status: {endpoint_status}")

        # Determine if the status is 'online'
        status = endpoint_status.get('status')
        if status == 'online':
            logger.info(f"Endpoint {endpoint_id} is online.")
            return True
        else:
            logger.info(f"Endpoint {endpoint_id} is not online. Status: {status}")
            return False
    except Exception as e:
        logger.error(f"Failed to check endpoint status: {str(e)}")
        return False

      
@app.command()
def main(endpoint_id: str) -> None:
    """
    Check the status of a Globus Compute endpoint by providing the endpoint_id.

    Example usage:
    python check_globus_compute.py --endpoint-id "your-uuid-here"

    IMPORTANT:
    Ensure you are logged into Globus Compute and have set the environment variables for the client credentials:
    export GLOBUS_COMPUTE_CLIENT_ID="your-client-id" & export GLOBUS_COMPUTE_CLIENT_SECRET="your-client-secret"

    :param endpoint_id: The UUID of the Globus Compute endpoint.
    """
    online = check_globus_compute_status(endpoint_id)
    if online:
        typer.echo(f"Endpoint {endpoint_id} is online.")
    else:
        typer.echo(f"Endpoint {endpoint_id} is not online.")


if __name__ == "__main__":
    app()
