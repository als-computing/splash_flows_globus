# import datetime
import os
from pathlib import Path
import uuid

from globus_sdk import TransferClient
from prefect import flow, task, get_run_logger
from orchestration.flows.bl7012.config import Config7012
from orchestration.globus import GlobusEndpoint, start_transfer


API_KEY = os.getenv("API_KEY")


@task(name="transfer_to_different_endpoints")
def transfer_data_to_nersc(
    file_path: str,
    transfer_client: TransferClient,
    source_endpoint: GlobusEndpoint,
    destination_endpoint: GlobusEndpoint,
):
    logger = get_run_logger()

    # Logging the original source paths
    logger.info(f"Requested relative source path: {file_path}")

    # if source_file begins with "/", it will mess up os.path.join
    file_path = file_path[1:] if file_path[0] == "/" else file_path
    source_path = os.path.join(source_endpoint.root_path, file_path)
    dest_path = os.path.join(destination_endpoint.root_path, file_path)

    # Logging the full source and destination paths
    logger.info(f"Full source path at {source_endpoint.name}: {source_path}")
    logger.info(f"Full destination path at {destination_endpoint.name}: {dest_path}")

    # Start globus transfer
    logger.info(
        f"Transferring {source_path} {source_endpoint.name} to {destination_endpoint.name}"
    )

    success = start_transfer(
        transfer_client,
        source_endpoint,
        source_path,
        destination_endpoint,
        dest_path,
        max_wait_seconds=600,
        logger=logger,
    )

    return success


@task(name="transfer_data_within_single_endpoint")
def transfer_data_within_single_endpoint(
    source_path: str,
    dest_path: str,
    transfer_client: TransferClient,
    globus_endpoint: GlobusEndpoint,
):
    logger = get_run_logger()

    # Logging the original source and destination paths
    logger.info(f"Requested relative source path: {source_path}")
    logger.info(f"Requested relative destination path: {dest_path}")

    # Remove the leading "/" in paths in present
    source_path = source_path[1:] if source_path[0] == "/" else source_path
    dest_path = dest_path[1:] if dest_path[0] == "/" else dest_path
    source_path = os.path.join(globus_endpoint.root_path, source_path)
    dest_path = os.path.join(globus_endpoint.root_path, dest_path)

    # Logging the full source and destination paths
    logger.info(f"Full source path at: {source_path}")
    logger.info(f"Full destination path at: {dest_path}")

    # Start globus transfer
    logger.info(f"Transferring {source_path} to {dest_path} at {globus_endpoint.name}")
    success = start_transfer(
        transfer_client,
        globus_endpoint,
        source_path,
        globus_endpoint,
        dest_path,
        max_wait_seconds=600,
        logger=logger,
    )

    return success


@flow(name="test_cosmicDTN_transfers")
def test_transfers_7012(file_path: str = "datamovement_test/test.txt"):
    logger = get_run_logger()
    config = Config7012()
    logger.info(f"{str(uuid.uuid4())}{file_path}")

    # copy file to a uniquely-named file in the same folder
    file = Path(file_path)
    new_file = str(file.with_name(f"test_{str(uuid.uuid4())}.txt"))
    logger.info(new_file)
    print(new_file)

    task = transfer_data_within_single_endpoint(
        file_path, new_file, config.tc, config.nersc7012
    )
    logger.info(
        f"File successfully transferred from {file_path} to {new_file}. Task {task}"
    )
    return


<<<<<<< HEAD
@flow(name="process_newfile_7012_ptycho4")
def process_new_file_ptycho4(file_path: str):
=======
@flow(name="process_newfile_7012")
def process_new_file(file_path: str):
>>>>>>> 369f16cf4e4e4a08593a033a7ed5299245f13277
    logger = get_run_logger()
    logger.info("Starting flow")
    config = Config7012()

    # Transferring data from cosmicDTN to NERSC
    task = transfer_data_to_nersc(
        file_path, config.tc, config.data7012, config.nersc7012
    )
    logger.info(f"File successfully transferred from cosmicDTN to NERSC. Task {task}")

    # Next task is: start ptycho reconstruction ...

    return task


if __name__ == "__main__":
    import sys
    import os
    import dotenv

    dotenv.load_dotenv()
    process_new_file(sys.argv[1])
    # test_transfers_7012()
