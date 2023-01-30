import logging
import os
import uuid
from pathlib import Path
from typing import Mapping

from globus_sdk import TransferClient
from prefect import flow, task, get_run_logger

from prefect.client import get_client

from orchestration import scicat
from orchestration.globus import GlobusEndpoint, start_transfer
from orchestration.flows.bl832.config import Config832


API_KEY = os.getenv("API_KEY")




@task(name="transfer_spot_to_data")
def transfer_spot_to_data(
        file_path: str,
        transfer_client: TransferClient,
        spot832: GlobusEndpoint,
        data832: GlobusEndpoint):
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
        wait_seconds=600,
        logger=logger,
)
    logger.info(f"spot832 to data832 globus task_id: {task}")
    return success


@task(name="transfer_data_to_nersc")
def transfer_data_to_nersc(
        file_path: str, 
        transfer_client: TransferClient,
        data832: GlobusEndpoint,
        nersc: GlobusEndpoint):
    logger = get_run_logger()

    # if source_file begins with "/", it will mess up os.path.join
    if file_path[0] == "/":
        file_path = file_path[1:]
    source_path = os.path.join(data832.root_path, file_path)
    dest_path = os.path.join(nersc.root_path, "8.3.2", file_path)

    logger.info(f"Transferring {dest_path} data832 to nersc")

    success = start_transfer(
        transfer_client,
        data832,
        source_path,
        nersc,
        dest_path,
        wait_seconds=600,
        logger=logger)

    return success

@task(name="test_scicat")
def test_scicat(config: Config832):
    logger = get_run_logger()
    block = Secret.load("scicat-token")
    token = block.get()
    scicat.test(
        config.scicat["jobs_api_url"], 
        token, 
        logger)


@task(name="ingest_scicat")
def ingest_scicat(config: Config832, relative_path):
    logger = get_run_logger()
    block = Secret.load("scicat-token")
    token = block.get()

    # relative path: raw/...
    # ingestor api maps /globa/cfs/cdirs/als/data_mover to /data_mover
    # so we want to prepend /data_mover/8.3.2
    breakpoint() 
    if relative_path[0] == "/":
        relative_path = relative_path[1:]
    ingest_path = os.path.join("/data_mover/8.3.2", relative_path)
    logger.info(f"Sending ingest job to {config.scicat['jobs_api_url']} for file {ingest_path}")
    response = scicat.submit_ingest(
        config.scicat["jobs_api_url"], 
        ingest_path,
        token,
        "als832_dx_3",
        logger=logger)
    logger.info(response)


@flow(name="new_832_file_flow")
async def process_new_832_file(file_path: str):
    logger = get_run_logger()
    config = Config832()

    # paths come in from the app on spot832 as /global/raw/...
    # remove 'global' so that all paths start with 'raw', which is common
    # to all 3 systems.

    relative_path = file_path.split('/global')[1]
    success = transfer_spot_to_data(
        relative_path,
        config.tc,
        config.spot832,
        config.data832)
    
    logger.info(f"Transferring {file_path} to spot to data")
    success = transfer_data_to_nersc(
        relative_path,
        config.tc,
        config.data832,
        config.nersc)
    logger.info(f"File successfully transferred from data832 to NERSC {file_path}. Task {task}")

    ingest_scicat(config, relative_path)

    try:
        async with get_client() as client:
            flow_run_model = await client.create_flow_run_from_deployment(
                parameters={"file_path": file_path}, deployment_id=deployment_id, 
            )
            logger.info(f"Created flow run {flow_run_model.name}!")
    except BaseException as err:
        logger.error(f"{err}")
        raise

    return success

@flow(name="test_832_transfers")
def test_transfers(file_path: str = "/raw/transfer_tests/test.txt"):
    logger = get_run_logger()
    config = Config832()
    test_scicat(config)
    logger.info(f"{str(uuid.uuid4())}{file_path}")
    # copy file to a uniquely-named file in the same folder
    file = Path(file_path)
    new_file = str(file.with_name(f"test_{str(uuid.uuid4())}.txt"))
    logger.info(new_file)
    success = start_transfer(
        config.tc,
        config.spot832,
        file_path,
        config.spot832,
        new_file,
        logger=logger)
    logger.info(success)
    spot832_path = transfer_spot_to_data(
        new_file,
        config.tc,
        config.spot832,
        config.data832)
    logger.info(f"Transferred {spot832_path} to spot to data")


    task = transfer_data_to_nersc(
        new_file,
        config.tc,
        config.data832,
        config.nersc)
    logger.info(f"File successfully transferred from data832 to NERSC {spot832_path}. Task {task}")



# if __name__ == "__main__":
#     process("/data/raw/dmcreynolds/test/test.txt")