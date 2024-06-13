from dotenv import load_dotenv
from globus_compute_sdk import Client, Executor
import globus_sdk
from globus_sdk import TransferClient
from orchestration.flows.bl832.config import Config832
from orchestration.globus_flows_utils import get_flows_client, get_specific_flow_client
from orchestration.globus import GlobusEndpoint, start_transfer
import os
from pathlib import Path
from prefect import flow, task, get_run_logger
import time
import uuid
from pprint import pprint

# Load environment variables
load_dotenv()

@task(name="transfer_spot_to_data")
def transfer_spot_to_data(
    file_path: str,
    transfer_client: TransferClient,
    spot832: GlobusEndpoint,
    data832: GlobusEndpoint,
):
    logger = get_run_logger()

    # if source_file begins with "/", it will mess up os.path.join
    if file_path[0] == "/":
        file_path = file_path[1:]

    source_path = os.path.join(spot832.root_path, file_path)
    dest_path = os.path.join(data832.root_path, file_path)
    success = start_transfer(
        transfer_client,
        spot832,
        source_path,
        data832,
        dest_path,
        max_wait_seconds=600,
        logger=logger,
    )
    logger.info(f"spot832 to data832 globus task_id: {task}")
    return success


@task(name="transfer_data_to_nersc")
def transfer_data_to_nersc(
    file_path: str,
    transfer_client: TransferClient,
    data832: GlobusEndpoint,
    nersc832: GlobusEndpoint,
):
    logger = get_run_logger()

    # if source_file begins with "/", it will mess up os.path.join
    if file_path[0] == "/":
        file_path = file_path[1:]
    source_path = os.path.join(data832.root_path, file_path)
    dest_path = os.path.join(nersc832.root_path, file_path)

    logger.info(f"Transferring {dest_path} data832 to nersc")

    success = start_transfer(
        transfer_client,
        data832,
        source_path,
        nersc832,
        dest_path,
        max_wait_seconds=600,
        logger=logger,
    )

    return success


@task(name="transfer_data_to_alcf")
def transfer_data_to_alcf(
        file_path: str,
        transfer_client: TransferClient,
        source_endpoint: GlobusEndpoint,
        destination_endpoint: GlobusEndpoint
    ):
    """
    Transfer data to/from ALCF endpoints.
    Args:
        file_path (str): Path to the file that needs to be transferred.
        transfer_client (TransferClient): TransferClient instance.
        source_endpoint (GlobusEndpoint): Source endpoint.
        alcf_endpoint (GlobusEndpoint): Destination endpoint.
    """
    logger = get_run_logger()

    if file_path[0] == "/":
        file_path = file_path[1:]

    source_path = os.path.join(source_endpoint.root_path, file_path)
    dest_path = os.path.join(destination_endpoint.root_path, file_path)

    try:
        success = start_transfer(
            transfer_client,
            source_endpoint,
            source_path,
            destination_endpoint,
            dest_path,
            max_wait_seconds=600,
            logger=logger,
        )
        if success:
            logger.info("Transfer to ALCF completed successfully.")
        else:
            logger.error("Transfer to ALCF failed.")
        return success
    except globus_sdk.services.transfer.errors.TransferAPIError as e:
        logger.error(f"Failed to submit transfer: {e}")
        return False
    

@flow(name="alcf_tomopy_reconstruction_flow")
def alcf_tomopy_reconstruction_flow():
    logger = get_run_logger()
    
    # Initialize the Globus Compute Client
    gcc = Client()
    polaris_endpoint_id = os.getenv("GLOBUS_COMPUTE_ENDPOINT") # COMPUTE endpoint, not TRANSFER endpoint
    gce = Executor(endpoint_id=polaris_endpoint_id, client=gcc)

    reconstruction_func = os.getenv("GLOBUS_RECONSTRUCTION_FUNC")
    source_collection_endpoint = os.getenv("GLOBUS_IRIBETA_CGS_ENDPOINT")
    destination_collection_endpoint = os.getenv("GLOBUS_IRIBETA_CGS_ENDPOINT") # os.getenv("GLOBUS_NERSC_ALSDEV_ENDPOINT")
    function_inputs = {"rundir": "/eagle/IRIBeta/als/sea_shell_test"}

    # Define the json flow
    flow_input = {
        "input": {
        "source": {
            "id": source_collection_endpoint,
            "path": "/sea_shell_test"
        },
        "destination": {
            "id": destination_collection_endpoint,
            "path": "/bl832"
        },
        "recursive_tx": True,
        "compute_endpoint_id": polaris_endpoint_id,
        "compute_function_id": reconstruction_func,
        "compute_function_kwargs": function_inputs
        }
    }
    collection_ids = [flow_input["input"]["source"]["id"], flow_input["input"]["destination"]["id"]]

    # Flow ID (only generate once!)
    flow_id = os.getenv("GLOBUS_FLOW_ID")

    # Run the flow
    fc = get_flows_client()
    flow_client = get_specific_flow_client(flow_id, collection_ids=collection_ids)

    try:
        logger.info("Starting globus flow action")
        flow_action = flow_client.run_flow(flow_input, label="ALS run", tags=["demo", "als", "tomopy"])
        flow_run_id = flow_action['action_id']
        logger.info( flow_action )
        logger.info(f'Flow action started with id: {flow_run_id}')
        logger.info(f"Monitor your flow here: https://app.globus.org/runs/{flow_run_id}")

        # Monitor flow status
        flow_status = flow_action['status']
        logger.info(f'Initial flow status: {flow_status}')
        while flow_status in ['ACTIVE', 'INACTIVE']:
            time.sleep(10)
            flow_action = fc.get_run(flow_run_id)
            flow_status = flow_action['status']
            logger.info(f'Updated flow status: {flow_status}')
            # Log additional details about the flow status
            logger.info(f'Flow action details: {flow_action}')

        if flow_status != 'SUCCEEDED':
            logger.error(f'Flow failed with status: {flow_status}')
            # Log additional details about the failure
            logger.error(f'Flow failure details: {flow_action}')
        else:
            logger.info(f'Flow completed successfully with status: {flow_status}')
    except Exception as e:
        logger.error(f"Error running flow: {e}")


@flow(name="new_832_file_flow")
def process_new_832_file(file_path: str, is_export_control=False, send_to_nersc=False, send_to_alcf=False):
    """
    Process and transfer a file from a source to the ALCF.
    Args:
        file_path (str): Path to the file that needs to be processed.
        is_export_control (bool, optional): Defaults to False. Whether the file is export controlled.
        send_to_nersc (bool, optional): Defaults to False. Whether to send the file to NERSC.
        send_to_alcf (bool, optional): Defaults to True. Whether to send the file to the ALCF.
        username (str, optional): Defaults to "testuser". The username of the user. This will update the correct file paths for saving the data at NERSC.
    """
    logger = get_run_logger()
    logger.info("Starting flow for new file processing and transfer.")

    config = Config832()
    
    # Send data from NERSC to ALCF (default is False), process it using Tomopy, and send it back to NERSC
    if not is_export_control and send_to_alcf:
        # assume file_path is the name of the file without the extension, but it is an h5 file
        # fp adds the .h5 extension back to the string (for the initial transfer to ALCF)
        fp = file_path + '.h5'
        
        # Transfer data from NERSC to ALCF
        logger.info(f"Transferring {file_path} from NERSC to ALCF")
        transfer_success = transfer_data_to_alcf(fp, config.tc, config.nersc_alsdev, config.alcf_iribeta_cgs)
        if not transfer_success:
            logger.error("Transfer failed due to configuration or authorization issues.")
        else:
            logger.info("Transfer successful.")

        logger.info(f"Running Tomopy reconstruction on {file_path} at ALCF")

        # Run the Tomopy reconstruction flow
        alcf_tomopy_reconstruction_flow()

        # Send reconstructed data to NERSC
        logger.info(f"Transferring {file_path} from ALCF to NERSC")
        file_path = '/bl832/rec' + file_path + '/'
        transfer_success = transfer_data_to_nersc(file_path, config.tc, config.alcf_iribeta_cgs, config.nersc_alsdev)
        if not transfer_success:
            logger.error("Transfer failed due to configuration or authorization issues.")
        else:
            logger.info("Transfer successful.")
            
if __name__ == "__main__":
    new_file = str('20230224_132553_sea_shell')
    process_new_832_file(new_file, False, False, True)