from prefect import flow, task, get_run_logger
import logging
import os
from globus_sdk import TransferClient


from ..globus import (
    activate_transfer_endpoint,
    build_apps,
    build_endpoints,
    get_config,
    GlobusEndpoint,
    init_transfer_client,
    start_transfer
)

from data_mover_ng import JobObserver

logger = logging.getLogger("data_mover.new_file_832")

@task
def transfer_spot_to_data(
        source_file: str, 
        transfer_client: TransferClient,
        spot832: GlobusEndpoint,
        data832: GlobusEndpoint):
    # logger = get_run_logger()
    logger.info(f"Transferring {source_file} spot832 to data832")
    task_id = start_transfer(
        transfer_client,
        spot832,
        source_file,
        data832,
        dest_folder)
    
    return task_id

@task
def transfer_data_to_nersc(
        source_file: str, 
        transfer_client: TransferClient,
        data832: GlobusEndpoint,
        nersc: GlobusEndpoint):
    # logger = get_run_logger()
    logger.info(f"Transferring {source_file} data832 to nersc")
    relative_path = os.path.relpath(source_file, "/data")
    dest_path = os.path.join(nersc.root_path, "8.3.2", relative_path)
    task = start_transfer(
        transfer_client,
        data832,
        source_file,
        nersc,
        dest_path,
        label="",
        wait_for_task=True)
    return task


@flow(name="new_832_file")
def process(job_observer: JobObserver, file_path: str):
    # logger = get_run_logger()

    config = get_config()
    endpoints = build_endpoints(config)
    apps = build_apps(config)
    tc = init_transfer_client(apps['als_transfer'])
    spot832 = endpoints['spot832']   
    data832 = endpoints['data832']
    nersc = endpoints['nersc']
    try:
        transfer_spot_to_data(
            tc,
            spot832,
            file_path,
            data832,
            ""
        )
        logger.info(f"Transferring {file_path} to spot to data")
    except Exception as e:
        logger.exception("Error transfering from spot832 to data832")

    try:
        task = transfer_data_to_nersc(
            file_path,
            tc,
            data832,
            nersc
        )
    except Exception as e:
        logger.exception("Error transfering from data832 to NERSC")

if __name__ == "__main__":
    process("/data/raw/dmcreynolds/test/test.txt")