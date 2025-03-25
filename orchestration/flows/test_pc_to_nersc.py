import os
from prefect import flow, task, get_run_logger
from globus_sdk import TransferClient

from orchestration.globus.transfer import GlobusEndpoint, start_transfer
from orchestration.config import get_config

@task(name="initialize_transfer_client")
def initialize_transfer_client():
    config = get_config()
    apps = config["globus"]["globus_apps"]
    from orchestration.globus.transfer import init_transfer_client
    return init_transfer_client(apps["als_transfer"])

@task(name="transfer_pc_to_nersc")
def transfer_pc_to_nersc(
    file_path: str,
    transfer_client: TransferClient,
    source_endpoint: GlobusEndpoint,
    destination_endpoint: GlobusEndpoint,
):
    logger = get_run_logger()
    
    # Format paths
    if file_path.startswith('/'):
        file_path = file_path[1:]
    
    source_path = os.path.join(source_endpoint.root_path, file_path)
    dest_path = os.path.join(destination_endpoint.root_path, os.path.basename(file_path))
    
    # Log details
    logger.info(f"Source endpoint: {source_endpoint.name} ({source_endpoint.uuid})")
    logger.info(f"Source path: {source_path}")
    logger.info(f"Destination endpoint: {destination_endpoint.name} ({destination_endpoint.uuid})")
    logger.info(f"Destination path: {dest_path}")
    
    # Start transfer
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
        logger.info("Transfer completed successfully!")
    else:
        logger.error("Transfer failed!")
    
    return success

@flow(name="test_pc_to_nersc_flow")
def test_pc_to_nersc_flow():
    file_path = "/Users/xiaoyachong/Documents/3Globus/grafana/training/iris_model.pkl"
    logger = get_run_logger()
    logger.info(f"Starting transfer of {file_path} to NERSC")
    
    # Get config and endpoints
    config = get_config()
    endpoints = config["globus"]["globus_endpoints"]
    
    # Initialize transfer client
    tc = initialize_transfer_client()
    
    # Set up endpoints from config
    source_endpoint = GlobusEndpoint(
        uuid=endpoints["my_personal_endpoint"]["uuid"],
        uri=endpoints["my_personal_endpoint"]["uri"],
        root_path=endpoints["my_personal_endpoint"]["root_path"],
        name=endpoints["my_personal_endpoint"]["name"]
    )
    
    destination_endpoint = GlobusEndpoint(
        uuid=endpoints["nersc_destination"]["uuid"], 
        uri=endpoints["nersc_destination"]["uri"],
        root_path=endpoints["nersc_destination"]["root_path"],
        name=endpoints["nersc_destination"]["name"]
    )
    
    # Execute transfer
    result = transfer_pc_to_nersc(
        file_path=file_path,
        transfer_client=tc,
        source_endpoint=source_endpoint,
        destination_endpoint=destination_endpoint
    )
    
    return result

