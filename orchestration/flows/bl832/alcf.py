import datetime
from dotenv import load_dotenv
import os
from pathlib import Path
import time

from globus_compute_sdk import Client, Executor
import globus_sdk
from globus_sdk import TransferClient
from prefect import flow, task, get_run_logger
from prefect.blocks.system import JSON, Secret

from orchestration.flows.bl832.config import Config832
from orchestration.globus.flows import get_flows_client, get_specific_flow_client
from orchestration.globus.transfer import GlobusEndpoint, start_transfer
from orchestration.prefect import schedule_prefect_flow


dotenv_file = load_dotenv()


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

    try:
        success = start_transfer(
            transfer_client,
            source_endpoint,
            source_path,
            nersc832,
            dest_path,
            max_wait_seconds=600,
            logger=logger,
        )
        if success:
            logger.info("Transfer to NERSC completed successfully.")
        else:
            logger.error("Transfer to NERSC failed.")
        return success
    except globus_sdk.services.transfer.errors.TransferAPIError as e:
        logger.error(f"Failed to submit transfer: {e}")
        return False
    finally:
        # Stop the timer and calculate the duration
        elapsed_time = time.time() - start_time
        logger.info(f"Transfer process took {elapsed_time:.2f} seconds.")


@task(name="transfer_data_to_data832")
def transfer_data_to_data832(
        file_path: str,
        transfer_client: TransferClient,
        source_endpoint: GlobusEndpoint,
        data832: GlobusEndpoint) -> bool:
    """
    Transfer data to data832 endpoints.

    Args:
        file_path (str): Path to the file that needs to be transferred.
        transfer_client (TransferClient): TransferClient instance.
        source_endpoint (GlobusEndpoint): Source endpoint.
        data832 (GlobusEndpoint): Destination endpoint.

    Returns:
        bool: Whether the transfer was successful.

    """
    logger = get_run_logger()

    # if source_file begins with "/", it will mess up os.path.join
    if file_path[0] == "/":
        file_path = file_path[1:]

    source_path = os.path.join(source_endpoint.root_path, file_path)
    dest_path = os.path.join(data832.root_path, file_path)

    start_time = time.time()

    try:
        success = start_transfer(
            transfer_client,
            source_endpoint,
            source_path,
            data832,
            dest_path,
            max_wait_seconds=600,
            logger=logger,
        )
        logger.info(f"{source_endpoint} to data832 globus task_id: {task}")
        return success
    except globus_sdk.services.transfer.errors.TransferAPIError as e:
        logger.error(f"Failed to submit transfer: {e}")
        return False
    finally:
        # Stop the timer and calculate the duration
        elapsed_time = time.time() - start_time
        logger.info(f"Transfer process took {elapsed_time:.2f} seconds.")


@task(name="schedule_prune_task")
def schedule_prune_task(path: str, location: str, schedule_days: datetime.timedelta) -> bool:
    """
    Schedules a Prefect flow to prune files from a specified location.

    Args:
        path (str): The file path to the folder containing the files.
        location (str): The server location (e.g., 'alcf832_raw') where the files will be pruned.
        schedule_days (int): The number of days after which the file should be deleted.
    """
    try:
        flow_name = f"delete {location}: {Path(path).name}"
        schedule_prefect_flow(
            deploymnent_name=f"prune_{location}/prune_{location}",
            flow_run_name=flow_name,
            parameters={"relative_path": path},
            duration_from_now=schedule_days
        )
        return True
    except Exception as e:
        logger = get_run_logger()
        logger.error(f"Failed to schedule prune task: {e}")
        return False


@task(name="schedule_pruning")
def schedule_pruning(
        alcf_raw_path: str = None,
        alcf_scratch_path_tiff: str = None,
        alcf_scratch_path_zarr: str = None,
        nersc_scratch_path_tiff: str = None,
        nersc_scratch_path_zarr: str = None,
        data832_scratch_path: str = None,
        one_minute: bool = False) -> bool:
    """
    This function schedules the deletion of files from specified locations on ALCF, NERSC, and data832.

    Args:
        alcf_raw_path (str, optional): The raw path of the h5 file on ALCF.
        alcf_scratch_path_tiff (str, optional): The scratch path for TIFF files on ALCF.
        alcf_scratch_path_zarr (str, optional): The scratch path for Zarr files on ALCF.
        nersc_scratch_path_tiff (str, optional): The scratch path for TIFF files on NERSC.
        nersc_scratch_path_zarr (str, optional): The scratch path for Zarr files on NERSC.
        data832_scratch_path (str, optional): The scratch path on data832.
        one_minute (bool, optional): Defaults to False. Whether to schedule the deletion after one minute.
    """
    logger = get_run_logger()

    pruning_config = JSON.load("pruning-config").value

    if one_minute:
        alcf_delay = datetime.timedelta(minutes=1)
        nersc_delay = datetime.timedelta(minutes=1)
        data832_delay = datetime.timedelta(minutes=1)
    else:
        alcf_delay = datetime.timedelta(days=pruning_config["delete_alcf832_files_after_days"])
        nersc_delay = datetime.timedelta(days=pruning_config["delete_nersc832_files_after_days"])
        data832_delay = datetime.timedelta(days=pruning_config["delete_data832_files_after_days"])

    # (path, location, days)
    delete_schedules = [
        (alcf_raw_path, "alcf832_raw", alcf_delay),
        (alcf_scratch_path_tiff, "alcf832_scratch", alcf_delay),
        (alcf_scratch_path_zarr, "alcf832_scratch", alcf_delay),
        (nersc_scratch_path_tiff, "nersc832_alsdev_scratch", nersc_delay),
        (nersc_scratch_path_zarr, "nersc832_alsdev_scratch", nersc_delay),
        (data832_scratch_path, "data832_scratch", data832_delay)
    ]

    for path, location, days in delete_schedules:
        if path:
            schedule_prune_task(path, location, days)
            logger.info(f"Scheduled delete from {location} at {days} days")
        else:
            logger.info(f"Path not provided for {location}, skipping scheduling of deletion task.")

    return True


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
        bool: The success status of the flow ('True', 'False').
    """
    logger = get_run_logger()

    # Initialize the Globus Compute Client
    gcc = Client()

    polaris_endpoint_id = Secret.load("globus-compute-endpoint")
    if polaris_endpoint_id is None:
        logger.error("Failed to load the secret 'globus-compute-endpoint'")
    gce = Executor(endpoint_id=polaris_endpoint_id.get(), client=gcc)

    reconstruction_func = Secret.load("globus-reconstruction-function")
    source_collection_endpoint = Secret.load("globus-iribeta-cgs-endpoint")
    destination_collection_endpoint = Secret.load("globus-iribeta-cgs-endpoint")

    # logger.info(f"Using compute_endpoint_id: {polaris_endpoint_id.get()}")
    logger.info(f"Using reconstruction_func: {reconstruction_func.get()}")
    # logger.info(f"Using source_collection_endpoint: {source_collection_endpoint.get()}")

    function_inputs = {"rundir": "/eagle/IRIBeta/als/bl832_test/raw",
                       "h5_file_name": file_name,
                       "folder_path": folder_name}

    # Define the json flow
    # class FlowInput(BaseModel):
    #     source: dict
    #     destination: dict
    #     recursive_tx: bool
    #     compute_endpoint_id: str
    #     compute_function_id: str
    #     compute_function_kwargs: dict

    # flow_input = FlowInput(

    flow_input = {
        "input": {
            "source": {
                "id": source_collection_endpoint.get(),
                "path": raw_path
            },
            "destination": {
                "id": destination_collection_endpoint.get(),
                "path": scratch_path
            },
            "recursive_tx": True,
            "compute_endpoint_id": polaris_endpoint_id.get(),
            "compute_function_id": reconstruction_func.get(),
            "compute_function_kwargs": function_inputs
        }
    }

    collection_ids = [flow_input["input"]["source"]["id"], flow_input["input"]["destination"]["id"]]

    # Flow ID (only generate once!)
    flow_id = Secret.load("globus-flow-id")

    logger.info(f"reconstruction_func: {reconstruction_func.get()}")
    logger.info(f"flow_id: {flow_id.get()}")

    # Start the timer
    start_time = time.time()

    # Run the flow
    flow_client = get_flows_client()
    specific_flow_client = get_specific_flow_client(flow_id.get(), collection_ids=collection_ids)

    success = False

    try:
        logger.info("Starting globus flow action")
        flow_action = specific_flow_client.run_flow(flow_input,
                                                    label="ALS run",
                                                    tags=["demo", "als", "tomopy"])
        flow_run_id = flow_action['action_id']
        logger.info(flow_action)
        logger.info(f'Flow action started with id: {flow_run_id}')
        logger.info(f"Monitor your flow here: https://app.globus.org/runs/{flow_run_id}")

        # Monitor flow status
        flow_status = flow_action['status']
        logger.info(f'Initial flow status: {flow_status}')
        while flow_status in ['ACTIVE', 'INACTIVE']:
            time.sleep(10)
            flow_action = flow_client.get_run(flow_run_id)
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
def process_new_832_ALCF_flow(folder_name: str,
                              file_name: str,
                              is_export_control: bool = False,
                              send_to_alcf: bool = True,
                              config=None) -> list:
    """
    Process and transfer a file from a source to the ALCF.

    Args:
        folder_name (str): The name of the project folder. Ex: "BLS-00564_dyparkinson"
        file_name (str): The name of the file to be processed. Ex: "20230224_132553_sea_shell"
        is_export_control (bool, optional): Defaults to False. Whether the file is export controlled.
        send_to_alcf (bool, optional): Defaults to True. Whether to send the file to the ALCF.

    Returns:
        list: Booleans indicating the success of each step (transfer to ALCF, recon, transfer to NERSC).

    """
    logger = get_run_logger()
    logger.info("Starting flow for new file processing and transfer.")
    if not config:
        config = Config832()

    # Send data from NERSC to ALCF, reconstructions run on ALCF and tiffs sent back to NERSC
    if not is_export_control and send_to_alcf:
        h5_file_name = file_name + '.h5'
        alcf_raw_path = f"bl832_test/raw/{folder_name}"
        alcf_scratch_path = f"bl832_test/scratch/{folder_name}"
        nersc_scratch_path = f"8.3.2/scratch/{folder_name}"
        scratch_path_tiff = folder_name + '/rec' + file_name + '/'
        scratch_path_zarr = folder_name + '/rec' + file_name + '.zarr/'

        # Step 1: Transfer data from NERSC to ALCF
        logger.info(f"Transferring {file_name} from NERSC to {alcf_raw_path} at ALCF")
        alcf_transfer_success = transfer_data_to_alcf(folder_name+'/'+h5_file_name,
                                                      config.tc,
                                                      config.nersc_test,
                                                      config.alcf832_raw)
        logger.info(f"Transfer status: {alcf_transfer_success}")
        if not alcf_transfer_success:
            logger.error("Transfer failed due to configuration or authorization issues.")
        else:
            logger.info("Transfer successful.")

        # Step 2: Run the Tomopy reconstruction flow
        logger.info(f"Running Tomopy reconstruction on {file_name} at ALCF")
        alcf_reconstruction_success = alcf_tomopy_reconstruction_flow(raw_path=alcf_raw_path,
                                                                      scratch_path=alcf_scratch_path,
                                                                      folder_name=folder_name,
                                                                      file_name=h5_file_name)
        if not alcf_reconstruction_success:
            logger.error("Reconstruction Failed.")
        else:
            logger.info("Reconstruction Successful.")

        # Step 3: Send reconstructed data (tiffs and zarr) to NERSC
        # Send reconstructed data (tiff) to NERSC
        logger.info(f"Transferring {file_name} from {alcf_raw_path} at ALCF to {nersc_scratch_path} at NERSC")
        logger.info(f"Reconstructed file path: {scratch_path_tiff}")
        nersc_transfer_success = transfer_data_to_nersc(scratch_path_tiff,
                                                        config.tc,
                                                        config.alcf832_scratch,
                                                        config.nersc832_alsdev_scratch)
        if not nersc_transfer_success:
            logger.error("Transfer failed due to configuration or authorization issues.")
        else:
            logger.info("Transfer successful.")

        # Send reconstructed data (zarr) to NERSC
        logger.info(f"Transferring {file_name} from {alcf_raw_path} at ALCF to {nersc_scratch_path} at NERSC")
        logger.info(f"Reconstructed file path: {scratch_path_zarr}")
        nersc_transfer_success = transfer_data_to_nersc(scratch_path_zarr,
                                                        config.tc,
                                                        config.alcf832_scratch,
                                                        config.nersc832_alsdev_scratch)
        if not nersc_transfer_success:
            logger.error("Transfer failed due to configuration or authorization issues.")
        else:
            logger.info("Transfer successful.")

        # Step 4: Send reconstructed data to data832/scratch
        # data832_scratch_path = f"8.3.2/scratch/{folder_name}"
        # logger.info(f"Transferring {file_name} from {alcf_raw_path} (ALCF) to {data832_scratch_path} (data832)")
        # data832_transfer_success = transfer_data_to_data832(reconstructed_file_path,
        #                                                     config.tc,
        #                                                     config.alcf832_scratch,
        #                                                     config.data832)
        # if not nersc_transfer_success:
        #     logger.error("Transfer failed due to configuration or authorization issues.")
        # else:
        #     logger.info("Transfer successful.")

        # Step 5: Schedule deletion of files from ALCF, NERSC, and data832
        logger.info("Scheduling deletion of files from ALCF, NERSC, and data832")
        alcf_transfer_success = True
        alcf_reconstruction_success = True
        nersc_transfer_success = True
        schedule_pruning(
            alcf_raw_path=f"{folder_name}/{h5_file_name}" if alcf_transfer_success else None,
            alcf_scratch_path_tiff=f"{scratch_path_tiff}" if alcf_reconstruction_success else None,
            alcf_scratch_path_zarr=f"{scratch_path_zarr}" if alcf_reconstruction_success else None,
            nersc_scratch_path_tiff=f"{scratch_path_tiff}" if nersc_transfer_success else None,
            nersc_scratch_path_zarr=f"{scratch_path_zarr}" if nersc_transfer_success else None,
            # data832_scratch_path = data832_scratch_path if data832_transfer_success else None
            one_minute=True  # Set to False for production durations
        )

        # Step 6: ingest into scicat ... todo
        logger.info(
            f"alcf_transfer_success: {alcf_transfer_success}, "
            f"alcf_reconstruction_success: {alcf_reconstruction_success}, "
            f"nersc_transfer_success: {nersc_transfer_success}"
        )

        return [alcf_transfer_success, alcf_reconstruction_success, nersc_transfer_success]

    else:
        logger.info("Export control is enabled or send_to_alcf is set to False. No action taken.")
        return [False, False, False]


if __name__ == "__main__":
    folder_name = str('BLS-00564_dyparkinson')
    file_name = str('20230224_132553_sea_shell')
    flow_success = process_new_832_ALCF_flow(folder_name=folder_name,
                                             file_name=file_name,
                                             is_export_control=False,
                                             send_to_alcf=True)
    print(flow_success)
