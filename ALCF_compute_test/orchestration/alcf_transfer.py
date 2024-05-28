from prefect import task, get_run_logger
import globus_sdk
from globus_sdk import TransferClient
from orchestration.globus import GlobusEndpoint, start_transfer
import os

@task(name="transfer_data_to_alcf")
def transfer_data_to_alcf(
        file_path: str,
        transfer_client: TransferClient,
        source_endpoint: GlobusEndpoint,
        alcf_endpoint: GlobusEndpoint
    ):
    """
    Transfer data to ALCF endpoint.
    Args:
        file_path (str): Path to the file that needs to be transferred.
        transfer_client (TransferClient): TransferClient instance.
        source_endpoint (GlobusEndpoint): Source endpoint.
        alcf_endpoint (GlobusEndpoint): Destination endpoint.
    """
    logger = get_run_logger()

    # Prepare the transfer data
    # if source_file begins with "/", it will mess up os.path.join
    if file_path[0] == "/":
        file_path = file_path[1:]

    source_path = os.path.join(source_endpoint.root_path, file_path)
    dest_path = os.path.join(alcf_endpoint.root_path, file_path)

    # Start the transfer
    try:
        # transfer_result = transfer_client.submit_transfer(transfer_data)
        success = start_transfer(
            transfer_client,
            source_endpoint,
            source_path,
            alcf_endpoint,
            dest_path,
            max_wait_seconds=600,
            logger=logger,
        )
        logger.info(f"Transfer submitted, task ID: {success['task_id']}")
        return success
    except globus_sdk.services.transfer.errors.TransferAPIError as e:
        logger.error(f"Failed to submit transfer: {e}")
        return False
