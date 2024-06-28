from globus_compute_sdk import Client, Executor
import globus_sdk
from globus_sdk import TransferClient
from orchestration.flows.bl832.config import Config832
from orchestration.globus_flows_utils import get_flows_client, get_specific_flow_client
from orchestration.globus import GlobusEndpoint, start_transfer
import os
from prefect import flow, task, get_run_logger
import time


@task(name="transfer_data_to_alcf")
def transfer_data_to_alcf(
        file_path: str,
        transfer_client: TransferClient,
        source_endpoint: GlobusEndpoint,
        destination_endpoint: GlobusEndpoint) -> bool:
    """
    Transfer data to ALCF endpoints.
    
    Args:
        file_path (str): Path to the file that needs to be transferred.
        transfer_client (TransferClient): TransferClient instance.
        source_endpoint (GlobusEndpoint): Source endpoint.
        destination_endpoint (GlobusEndpoint): Destination endpoint.
    
    Returns:
        bool: Whether the transfer was successful.
    """
    logger = get_run_logger()

    if file_path[0] == "/":
        file_path = file_path[1:]

    source_path = os.path.join(source_endpoint.root_path, file_path)
    dest_path = os.path.join(destination_endpoint.root_path, file_path)
    logger.info(f"Transferring {source_path} to {dest_path} at ALCF")
    # Start the timer
    start_time = time.time()

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
    finally:
        # Stop the timer and calculate the duration
        elapsed_time = time.time() - start_time
        logger.info(f"Transfer process took {elapsed_time:.2f} seconds.")    


@task(name="transfer_data_to_nersc")
def transfer_data_to_nersc(
    file_path: str,
    transfer_client: TransferClient,
    source_endpoint: GlobusEndpoint,
    nersc832: GlobusEndpoint) -> bool:
    """
    Transfer data to NERSC endpoints.

    Args:
        file_path (str): Path to the file that needs to be transferred.
        transfer_client (TransferClient): TransferClient instance.
        source_endpoint (GlobusEndpoint): Source endpoint.
        nersc832 (GlobusEndpoint): Destination endpoint.

    Returns:
        bool: Whether the transfer was successful.

    """
    logger = get_run_logger()

    # if source_file begins with "/", it will mess up os.path.join
    if file_path[0] == "/":
        file_path = file_path[1:]
    source_path = os.path.join(source_endpoint.root_path, file_path)
    dest_path = os.path.join(nersc832.root_path, file_path)

    logger.info(f"Transferring {dest_path} to nersc")

    # Start the timer
    start_time = time.time()

    success = start_transfer(
        transfer_client,
        source_endpoint,
        source_path,
        nersc832,
        dest_path,
        max_wait_seconds=600,
        logger=logger,
    )

    # Stop the timer and calculate the duration
    elapsed_time = time.time() - start_time

    # Log the elapsed time
    logger.info(f"Transfer process took {elapsed_time:.2f} seconds.")

    return success


@flow(name="alcf_tomopy_reconstruction_flow")
def alcf_tomopy_reconstruction_flow(
    raw_path: str,
    scratch_path: str,
    folder_name: str,
    file_name: str) -> bool:
    """
    Run the Tomopy reconstruction flow on the ALCF.

    Args:
        raw_path (str): The path to the raw data on ALCF.
        scratch_path (str): The path to the scratch directory on ALCF.
        file_name (str): The name of the file to be processed.

    Returns:
        str: The status of the flow ('SUCCEEDED', 'FAILED', etc.).
    """
    logger = get_run_logger()
    
    # Initialize the Globus Compute Client
    gcc = Client()
    polaris_endpoint_id = os.getenv("GLOBUS_COMPUTE_ENDPOINT") # COMPUTE endpoint, not TRANSFER endpoint
    gce = Executor(endpoint_id=polaris_endpoint_id, client=gcc)

    reconstruction_func = os.getenv("GLOBUS_RECONSTRUCTION_FUNC")
    source_collection_endpoint = os.getenv("GLOBUS_IRIBETA_CGS_ENDPOINT")
    destination_collection_endpoint = os.getenv("GLOBUS_IRIBETA_CGS_ENDPOINT")

    function_inputs = {"rundir": "/eagle/IRIBeta/als/bl832_test/raw", "file_name": file_name, "folder_path": folder_name}

    # Define the json flow
    flow_input = {
        "input": {
            "source": {
                "id": source_collection_endpoint,
                "path": raw_path # "/sea_shell_test"
            },
            "destination": {
                "id": destination_collection_endpoint,
                "path": scratch_path # "/bl832"
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

    # Start the timer
    start_time = time.time()

    # Run the flow
    fc = get_flows_client()
    flow_client = get_specific_flow_client(flow_id, collection_ids=collection_ids)

    success = False

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
            success = True
    except Exception as e:
        logger.error(f"Error running flow: {e}")
        success = False
    finally:
        # Stop the timer and calculate the duration
        elapsed_time = time.time() - start_time
        logger.info(f"Total duration of the flow operation: {elapsed_time:.2f} seconds.")
    
    return success


@flow(name="new_832_ALCF_flow")
def process_new_832_ALCF_flow(proposal_folder_name: str,
                              file_name: str,
                              is_export_control: bool = False,
                              send_to_alcf: bool = True) -> list:
    """
    Process and transfer a file from a source to the ALCF.
    
    Args:
        proposal_folder_name (str): The name of the proposal folder. Ex: "BLS-00564_dyparkinson"
        file_name (str): The name of the file to be processed. Ex: "20230224_132553_sea_shell"
        is_export_control (bool, optional): Defaults to False. Whether the file is export controlled.
        send_to_alcf (bool, optional): Defaults to True. Whether to send the file to the ALCF.

    Returns:
        list: A list of booleans indicating the success of each step (transfer to ALCF, reconstruction, transfer back to NERSC).

    """
    logger = get_run_logger()
    logger.info("Starting flow for new file processing and transfer.")
    config = Config832()
    
    # Send data from NERSC to ALCF, reconstructions run on ALCF and tiffs sent back to NERSC
    if not is_export_control and send_to_alcf:
        h5_file_name = file_name + '.h5'
        
        alcf_raw_path = f"bl832_test/raw/{proposal_folder_name}"
        alcf_scratch_path = f"bl832_test/scratch/{proposal_folder_name}"
        nersc_scratch_path = f"8.3.2/scratch/{proposal_folder_name}"

        # Step 1: Transfer data from NERSC to ALCF
        alcf_transfer_success = transfer_data_to_alcf(h5_file_name, config.tc, config.nersc832_alsdev_raw, config.alcf_iribeta_cgs_raw)
        
        logger.info(f"Transferring {file_name} from NERSC to {alcf_raw_path} at ALCF")
        alcf_transfer_success = transfer_data_to_alcf(proposal_folder_name+'/'+h5_file_name, config.tc, config.nersc_test, config.alcf_iribeta_cgs_raw)

        if not alcf_transfer_success:
            logger.error("Transfer failed due to configuration or authorization issues.")
        else:
            logger.info("Transfer successful.")

        # # Step 2: Run the Tomopy reconstruction flow
        logger.info(f"Running Tomopy reconstruction on {file_name} at ALCF")
        alcf_reconstruction_success = alcf_tomopy_reconstruction_flow(raw_path=alcf_raw_path, scratch_path=alcf_scratch_path, folder_name=proposal_folder_name, file_name=h5_file_name)

        if not alcf_reconstruction_success:
            logger.error("Reconstruction Failed.")
        else:
            logger.info("Reconstruction Successful.")

        # # Step 3: Send reconstructed data to NERSC
        logger.info(f"Transferring {file_name} from {alcf_raw_path} at ALCF to {nersc_scratch_path} at NERSC")
        reconstructed_file_path = proposal_folder_name + '/rec' + file_name + '/'
        logger.info(f"Reconstructed file path: {reconstructed_file_path}")
        nersc_transfer_success = transfer_data_to_nersc(reconstructed_file_path, config.tc, config.alcf_iribeta_cgs_scratch, config.nersc832_alsdev_scratch)
        
        if not nersc_transfer_success:
            logger.error("Transfer failed due to configuration or authorization issues.")
        else:
            logger.info("Transfer successful.")

        return [alcf_transfer_success, alcf_reconstruction_success, nersc_transfer_success]
        

    else:
        logger.info("Export control is enabled or send_to_alcf is set to False. No action taken.")
        return [False, False, False]


if __name__ == "__main__":
    # new_file = str('20230224_132553_sea_shell')
    folder_name = str('BLS-00564_dyparkinson')
    file_name = str('20230224_132553_sea_shell')
    flow_success = process_new_832_ALCF_flow(proposal_folder_name=folder_name, file_name=file_name, is_export_control=False, send_to_alcf=True)
    print(flow_success)