from concurrent.futures import Future
import datetime
import os
from pathlib import Path
import time

from globus_compute_sdk import Client, Executor
from globus_compute_sdk.serialize import CombinedCode
import globus_sdk
from globus_sdk import TransferClient
from prefect import flow, task, get_run_logger
from prefect.blocks.system import JSON, Secret

from orchestration.flows.bl832.config import Config832
from orchestration.globus.transfer import GlobusEndpoint, start_transfer
from orchestration.prefect import schedule_prefect_flow


@task(name="transfer_data_to_alcf")
def transfer_data_to_alcf(
    file_path: str,
    transfer_client: TransferClient,
    source_endpoint: GlobusEndpoint,
    destination_endpoint: GlobusEndpoint
) -> bool:
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


@task(name="transfer_data_to_data832")
def transfer_data_to_data832(
    file_path: str,
    transfer_client: TransferClient,
    source_endpoint: GlobusEndpoint,
    data832: GlobusEndpoint
) -> bool:
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
def schedule_prune_task(
    path: str,
    location: str,
    schedule_days: datetime.timedelta,
    source_endpoint=None,
    check_endpoint=None
) -> bool:
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
            deployment_name=f"prune_{location}/prune_{location}",
            flow_run_name=flow_name,
            parameters={
                "relative_path": path,
                "source_endpoint": source_endpoint,
                "check_endpoint": check_endpoint
            },
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
    data832_raw_path: str = None,
    data832_scratch_path_tiff: str = None,
    data832_scratch_path_zarr: str = None,
    one_minute: bool = False,
    config=None
) -> bool:
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

    # (path, location, days, source_endpoint, check_endpoint)
    delete_schedules = [
        (alcf_raw_path, "alcf832_raw", alcf_delay, config.alcf832_raw, config.data832_raw),
        (alcf_scratch_path_tiff, "alcf832_scratch", alcf_delay, config.alcf832_scratch, config.data832_scratch),
        (alcf_scratch_path_zarr, "alcf832_scratch", alcf_delay, config.alcf832_scratch, config.data832_scratch),
        (nersc_scratch_path_tiff, "nersc832_alsdev_scratch", nersc_delay, config.nersc832_alsdev_scratch, None),
        (nersc_scratch_path_zarr, "nersc832_alsdev_scratch", nersc_delay, config.nersc832_alsdev_scratch, None),
        (data832_raw_path, "data832_raw", data832_delay, config.data832_raw, None),
        (data832_scratch_path_tiff, "data832_scratch", data832_delay, config.data832_scratch, None),
        (data832_scratch_path_zarr, "data832_scratch", data832_delay, config.data832_scratch, None)
    ]

    for path, location, days, source_endpoint, check_endpoint in delete_schedules:
        if path:
            schedule_prune_task(path, location, days, source_endpoint, check_endpoint)
            logger.info(f"Scheduled delete from {location} at {days} days")
        else:
            logger.info(f"Path not provided for {location}, skipping scheduling of deletion task.")

    return True


@task(name="wait_for_globus_compute_future")
def wait_for_globus_compute_future(
    future: Future,
    task_name: str,
    check_interval=20
) -> bool:
    """
    Wait for a Globus Compute task to complete, assuming that if future.done() is False, the task is running.

    Args:
        future: The future object returned from the Globus Compute Executor submit method.
        task_name: A descriptive name for the task being executed (used for logging).
        check_interval: The interval (in seconds) between status checks.

    Returns:
        bool: True if the task completed successfully, False otherwise.
    """
    logger = get_run_logger()
    start_time = time.time()
    success = False

    try:
        previous_state = None
        while not future.done():
            # Check if the task was cancelled
            if future.cancelled():
                logger.warning(f"The {task_name} task was cancelled.")
                return False
            # Assume the task is running if not done and not cancelled
            elif previous_state != 'running':
                logger.info(f"The {task_name} task is running...")
                previous_state = 'running'

            time.sleep(check_interval)  # Wait before the next status check

        # Task is done, check if it was cancelled or raised an exception
        if future.cancelled():
            logger.warning(f"The {task_name} task was cancelled after completion.")
            return False

        exception = future.exception()
        if exception:
            logger.error(f"The {task_name} task raised an exception: {exception}")
            return False

        # Task completed successfully
        result = future.result()
        logger.info(f"The {task_name} task completed successfully with result: {result}")
        success = True

    except Exception as e:
        logger.error(f"An error occurred while waiting for the {task_name} task: {str(e)}")
        success = False

    finally:
        # Log the total time taken for the task
        elapsed_time = time.time() - start_time
        logger.info(f"Total duration of the {task_name} task: {elapsed_time:.2f} seconds.")

    return success


@flow(name="alcf_globus_compute_reconstruction")
def alcf_globus_compute_reconstruction(
    folder_name: str,
    file_name: str
) -> str:
    """
    Tomopy reconstruction code that is executed using Globus Compute

    Args:
        rundir (str): the directory on the eagle file system (ALCF) where the input data are located
        script_path (str): the path to the script that will run reconstruction
        h5_file_name (str): the name of the h5 file to be reconstructed
        folder_path (str): the path to the folder where the h5 file is located

    Returns:
        str: confirmation message regarding reconstruction and time to completion
    """
    iribeta_rundir = "/eagle/IRIBeta/als/bl832/raw"
    iribeta_recon_script = "/eagle/IRIBeta/als/example/globus_reconstruction.py"

    # iri_als_bl832_rundir = "/eagle/IRI-ALS-832/data/raw"
    # iri_als_bl832_recon_script = "/eagle/IRI-ALS-832/scripts/globus_reconstruction.py"

    gcc = Client(code_serialization_strategy=CombinedCode())

    with Executor(endpoint_id=Secret.load("globus-compute-endpoint").get(), client=gcc) as fxe:
        logger = get_run_logger()
        logger.info(f"Running Tomopy reconstruction on {file_name} at ALCF")
        future = fxe.submit(reconstruction_wrapper,
                            iribeta_rundir,
                            iribeta_recon_script,
                            file_name,
                            folder_name)
        result = wait_for_globus_compute_future(future, "reconstruction", check_interval=10)
        return result


def reconstruction_wrapper(
    rundir="/eagle/IRI-ALS-832/data/raw",
    script_path="/eagle/IRI-ALS-832/scripts/globus_reconstruction.py",
    h5_file_name=None,
    folder_path=None
) -> str:
    import os
    import subprocess
    import time

    rec_start = time.time()

    # Move to directory where data are located
    os.chdir(rundir)

    # Run reconstruction.py
    command = f"python {script_path} {h5_file_name} {folder_path}"
    recon_res = subprocess.run(command.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    rec_end = time.time()

    print(f"Reconstructed data in {folder_path}/{h5_file_name} in {rec_end-rec_start} seconds;\n {recon_res}")

    return (
        f"Reconstructed data specified in {folder_path} / {h5_file_name} in {rec_end-rec_start} seconds;\n"
        f"{recon_res}"
    )


@flow(name="alcf_globus_compute_tiff_to_zarr")
def alcf_globus_compute_tiff_to_zarr(
    raw_path: str,
    tiff_scratch_path
) -> str:
    """
    Tiff to Zarr code that is executed using Globus Compute

    Args:
        rundir (str): the directory on the eagle file system (ALCF) where the input data are located
        script_path (str): the path to the script that will run reconstruction
        h5_file_name (str): the name of the h5 file to be reconstructed
        folder_path (str): the path to the folder where the h5 file is located

    Returns:
        str: confirmation message regarding reconstruction and time to completion
    """
    iribeta_rundir = "/eagle/IRIBeta/als/bl832/raw"
    iribeta_conversion_script = "/eagle/IRIBeta/als/example/tiff_to_zarr.py"

    # iri_als_bl832_rundir = "/eagle/IRI-ALS-832/data/raw"
    # iri_als_bl832_conversion_script = "/eagle/IRI-ALS-832/scripts/tiff_to_zarr.py"

    gcc = Client(code_serialization_strategy=CombinedCode())

    with Executor(endpoint_id=Secret.load("globus-compute-endpoint").get(), client=gcc) as fxe:
        logger = get_run_logger()
        logger.info(f"Running Tiff to Zarr on {raw_path} at ALCF")
        future = fxe.submit(tiff_to_zarr_wrapper,
                            iribeta_rundir,
                            iribeta_conversion_script,
                            tiff_scratch_path,
                            raw_path)
        result = wait_for_globus_compute_future(future, "tiff to zarr conversion", check_interval=10)
        return result


def tiff_to_zarr_wrapper(
    rundir="/eagle/IRI-ALS-832/data/raw",
    script_path="/eagle/IRI-ALS-832/scripts/tiff_to_zarr.py",
    recon_path=None,
    raw_path=None
) -> str:
    """
    Python function that wraps around the application call for Tiff to Zarr on ALCF

    Args:
        rundir (str): the directory on the eagle file system (ALCF) where the input data are located
        script_path (str): the path to the script that will convert the tiff files to zarr
        recon_path (str): the path to the reconstructed data
        raw_path (str): the path to the raw data
    Returns:
        str: confirmation message
    """
    import os
    import subprocess

    # Move to directory where data are located
    os.chdir(rundir)

    # Convert tiff files to zarr
    command = (f"python {script_path} {recon_path} --raw_directory {raw_path}")
    zarr_res = subprocess.run(command.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    return (
        f"Converted tiff files to zarr;\n {zarr_res}"
    )


@flow(name="alcf_recon_flow")
def alcf_recon_flow(
    folder_name: str,
    file_name: str,
    is_export_control: bool = False,
    send_to_alcf: bool = True,
    config=None
) -> list:
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

    # Send data from data832 to ALCF, reconstructions run on ALCF and tiffs sent back to data832
    if not is_export_control and send_to_alcf:
        h5_file_name = file_name + '.h5'

        alcf_raw_path = f"bl832/raw/{folder_name}"

        data832_raw_path = f"{folder_name}/{h5_file_name}"
        data832_scratch_path = f"{folder_name}"

        # nersc_scratch_path = f"8.3.2/scratch/{folder_name}"

        scratch_path_tiff = folder_name + '/rec' + file_name + '/'
        scratch_path_zarr = folder_name + '/rec' + file_name + '.zarr/'

        # Step 1: Transfer data from data832 to ALCF
        logger.info(f"Transferring {file_name} from data832 to {alcf_raw_path} at ALCF")
        alcf_transfer_success = transfer_data_to_alcf(
            data832_raw_path,
            config.tc,
            config.data832_raw,
            config.alcf832_raw)
        logger.info(f"Transfer status: {alcf_transfer_success}")
        if not alcf_transfer_success:
            logger.error("Transfer failed due to configuration or authorization issues.")
            raise ValueError("Transfer to ALCF Failed")
        else:
            logger.info("Transfer to ALCF Successful.")

            # Step 2A: Run the Tomopy Reconstruction Globus Flow
            logger.info(f"Running Tomopy reconstruction on {file_name} at ALCF")
            alcf_reconstruction_success = alcf_globus_compute_reconstruction(
                folder_name=folder_name,
                file_name=h5_file_name)
            if not alcf_reconstruction_success:
                logger.error("Reconstruction Failed.")
                raise ValueError("Reconstruction at ALCF Failed")
            else:
                logger.info("Reconstruction Successful.")

                # Step 2B: Run the Tiff to Zarr Globus Flow
                logger.info(f"Running Tiff to Zarr on {file_name} at ALCF")
                raw_path = f"/eagle/IRIBeta/als/{alcf_raw_path}/{h5_file_name}"
                tiff_scratch_path = f"/eagle/IRIBeta/als/bl832/scratch/{folder_name}/rec{file_name}/"
                alcf_tiff_to_zarr_success = alcf_globus_compute_tiff_to_zarr(
                    raw_path=raw_path,
                    tiff_scratch_path=tiff_scratch_path)
                if not alcf_tiff_to_zarr_success:
                    logger.error("Tiff to Zarr Failed.")
                    raise ValueError("Tiff to Zarr at ALCF Failed")
                else:
                    logger.info("Tiff to Zarr Successful.")

        if alcf_reconstruction_success:
            # Step 3: Send reconstructed data (tiffs and zarr) to data832
            # Transfer A: Send reconstructed data (tiff) to data832
            logger.info(f"Transferring {file_name} from {alcf_raw_path} "
                        f"at ALCF to {data832_scratch_path} at data832")
            logger.info(f"Reconstructed file path: {scratch_path_tiff}")
            data832_tiff_transfer_success = transfer_data_to_data832(
                scratch_path_tiff,
                config.tc,
                config.alcf832_scratch,
                config.data832_scratch)
            if not data832_tiff_transfer_success:
                logger.error("Transfer failed due to configuration or authorization issues.")
            else:
                logger.info("Transfer successful.")

        if alcf_tiff_to_zarr_success:
            # Transfer B: Send reconstructed data (zarr) to data832
            logger.info(f"Transferring {file_name} from {alcf_raw_path} "
                        f"at ALCF to {data832_scratch_path} at data832")
            logger.info(f"Reconstructed file path: {scratch_path_zarr}")
            data832_zarr_transfer_success = transfer_data_to_data832(
                scratch_path_zarr,
                config.tc,
                config.alcf832_scratch,
                config.data832_scratch)
            if not data832_zarr_transfer_success:
                logger.error("Transfer failed due to configuration or authorization issues.")
            else:
                logger.info("Transfer successful.")

        # Step 4: Schedule deletion of files from ALCF, NERSC, and data832
        logger.info("Scheduling deletion of files from ALCF, NERSC, and data832")
        nersc_transfer_success = False
        # alcf_transfer_success = True
        # alcf_reconstruction_success = True
        # alcf_tiff_to_zarr_success = True
        # data832_tiff_transfer_success = True
        # data832_zarr_transfer_success = True

        schedule_pruning(
            alcf_raw_path=f"{folder_name}/{h5_file_name}" if alcf_transfer_success else None,
            alcf_scratch_path_tiff=f"{scratch_path_tiff}" if alcf_reconstruction_success else None,
            alcf_scratch_path_zarr=f"{scratch_path_zarr}" if alcf_tiff_to_zarr_success else None,
            nersc_scratch_path_tiff=f"{scratch_path_tiff}" if nersc_transfer_success else None,
            nersc_scratch_path_zarr=f"{scratch_path_zarr}" if nersc_transfer_success else None,
            data832_raw_path=f"{folder_name}/{h5_file_name}" if alcf_transfer_success else None,
            data832_scratch_path_tiff=f"{scratch_path_tiff}" if data832_tiff_transfer_success else None,
            data832_scratch_path_zarr=f"{scratch_path_zarr}" if data832_zarr_transfer_success else None,
            one_minute=True,  # Set to False for production durations
            config=config
        )

        # Step 5: ingest into scicat ... todo

        logger.info(
            f"alcf_transfer_success: {alcf_transfer_success}, "
            f"alcf_reconstruction_success: {alcf_reconstruction_success}, "
            f"alcf_tiff_to_zarr_success: {alcf_tiff_to_zarr_success}, "
            # f"nersc_transfer_success: {nersc_transfer_success}"
            f"data832_tiff_transfer_success: {data832_tiff_transfer_success}, "
            f"data832_zarr_transfer_success: {data832_zarr_transfer_success}"

        )

        return [alcf_transfer_success,
                alcf_reconstruction_success,
                alcf_tiff_to_zarr_success,
                data832_tiff_transfer_success,
                data832_zarr_transfer_success]

    else:
        logger.info("Export control is enabled or send_to_alcf is set to False. No action taken.")
        return [False, False, False, False, False]


if __name__ == "__main__":
    folder_name = 'dabramov'
    file_name = '20240425_104614_nist-sand-30-100_27keV_z8mm_n2625'
    flow_success = alcf_recon_flow(
        folder_name=folder_name,
        file_name=file_name,
        is_export_control=False,
        send_to_alcf=True)
    print(flow_success)
