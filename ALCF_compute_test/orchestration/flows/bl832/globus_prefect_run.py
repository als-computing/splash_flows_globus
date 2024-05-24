# from globus_prefect_test.orchestration.globus_flow_alcf_tomopy import reconstruction_wrapper

from dotenv import load_dotenv
from globus_compute_sdk import Client, Executor
import globus_sdk
# from globus_sdk import TransferClient
# from orchestration import globus
from orchestration.flows.bl832.config import Config832
from globus_prefect_test.orchestration.globus_flows_utils import get_flows_client, get_specific_flow_client
# from orchestration.globus import GlobusEndpoint, start_transfer
import os
from pathlib import Path
from prefect import flow, task, get_run_logger
import time
import uuid


# Load environment variables
load_dotenv()

# Set the client ID and fetch client secret from environment
CLIENT_ID = os.getenv('GLOBUS_CLIENT_ID')
CLIENT_SECRET = os.getenv('GLOBUS_CLIENT_SECRET')

config = Config832()

confidential_client = globus_sdk.ConfidentialAppAuthClient(
    client_id=CLIENT_ID, client_secret=CLIENT_SECRET
)
# SCOPES = ["urn:globus:auth:scope:transfer.api.globus.org:all"]

SCOPES = [
        globus_sdk.FlowsClient.scopes.manage_flows,
        globus_sdk.FlowsClient.scopes.run_status,
    ]

# SCOPES = ['urn:globus:auth:scope:transfer.api.globus.org:all[*https://auth.globus.org/scopes/55c3adf6-31f1-4647-9a38-52591642f7e7/data_access]']
print(SCOPES)
cc_authorizer = globus_sdk.ClientCredentialsAuthorizer(confidential_client, SCOPES)
tc = globus_sdk.TransferClient(authorizer=cc_authorizer)

@flow(name="reconstruction_wrapper")
def reconstruction_wrapper(rundir, parametersfile="inputOneSliceOfEach.txt"):
    """
    Python function that wraps around the application call for Tomopy reconstruction on ALCF

    Args:
        rundir (str): the directory on the eagle file system (ALCF) where the input data are located
        parametersfile (str, optional): Defaults to "inputOneSliceOfEach.txt", which is already on ALCF

    Returns:
        str: confirmation message regarding reconstruction and time to completion
    """
    import time
    import os
    import subprocess
    start = time.time()

    # Move to directory where data are located
    os.chdir(rundir)

    # Run reconstruction.py
    command = f"python /eagle/IRIBeta/als/example/reconstruction.py {parametersfile}"
    res = subprocess.run(command.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    end = time.time()
    
    return f"Reconstructed data specified in {parametersfile} in {end-start} seconds;\n {res}"

@flow(name="alcf_tomopy_reconstruction_flow")
def alcf_tomopy_reconstruction_flow():
    logger = get_run_logger()
    config = Config832()
    
    # Initialize the Globus Compute Client
    gcc = Client()
    polaris_endpoint_id = os.getenv("GLOBUS_COMPUTE_ENDPOINT") # COMPUTE endpoint, not TRANSFER endpoint
    gce = Executor(endpoint_id=polaris_endpoint_id, client=gcc)

    # Register and submit the function
    # reconstruction_func = gcc.register_function(reconstruction_wrapper)
    # logger.info(f"Function registered with ID: {reconstruction_func}")
    # future = gce.submit_to_registered_function(args=["/eagle/IRIBeta/als/example"], function_id=reconstruction_func)
    
    # # Wait for the result and log it
    # try:
    #     result = future.result()
    #     logger.info(f"Reconstruction completed with result: {result}")
    # except Exception as e:
    #     logger.error(f"Error during function submission or execution: {e}")
    #     return  # Exit if there is an error during reconstruction

    # reconstruction_func = "d8645197-0bff-4b6f-a02d-e1df9841ed90"
    reconstruction_func = "ebec3c4f-b052-4137-ba82-5d91928e16dc"
    collection_endpoint = "55c3adf6-31f1-4647-9a38-52591642f7e7"
    # collection_endpoint = "cf333f97-ff8c-40f7-b25a-21fe726f1ea0"
    # Where the data will transfer to after reconstruction (ALCF Home)
    alcfhome_transfer_endpoint_id = config.alcf_home832.uuid
    destination_path_on_alcfhome = config.alcf_home832.root_path

    # Where the raw data are located to be reconstructed (ALCF Eagle)
    eagle_transfer_endpoint_id = config.alcf_eagle832.uuid
    source_path_on_eagle = config.alcf_eagle832.root_path

    function_inputs = {"rundir": "/eagle/IRIBeta/als/example"}

    # Define the json flow
    flow_input = {
        "input": {
        "source": {
            "id": collection_endpoint,
            "path": "/example"
        },
        "destination": {
            "id": collection_endpoint, #eagle_transfer_endpoint_id,
            "path": "/bl832/" #source_path_on_eagle

            # "id": alcfhome_transfer_endpoint_id,
            # "path": destination_path_on_alcfhome
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
    # flow_id = "91ce59ba-cbf1-45e3-b4f3-c86aeaacda42"

    # Run the flow
    fc = get_flows_client()
    # run_client = get_specific_flow_client(flow_id, collection_ids=collection_ids)
    flow_client = get_specific_flow_client(flow_id, collection_ids=collection_ids)

    try:
        flow_action = flow_client.run_flow(flow_input, label="ALS run", tags=["demo", "als", "tomopy"])
        flow_run_id = flow_action['action_id']
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
def process_new_832_file(file_path: str, is_export_control=False, send_to_nersc=False, send_to_alcf=True):
    """
    Process and transfer a file from a source to the ALCF.
    Args:
        file_path (str): Path to the file that needs to be processed.
        is_export_control (bool, optional): Defaults to False. Whether the file is export controlled.
        send_to_nersc (bool, optional): Defaults to False. Whether to send the file to NERSC.
        send_to_alcf (bool, optional): Defaults to True. Whether to send the file to the ALCF.
    """
    logger = get_run_logger()
    logger.info("Starting flow for new file processing and transfer.")

    config = Config832()
    
    # Send data to ALCF (default is False) and process it using Tomopy
    if not is_export_control and send_to_alcf:
        # Call the task to transfer data
        # transfer_success = transfer_data_to_alcf(file_path, transfer_client, config.nersc832, config.alcf832)
        # if not transfer_success:
        #     logger.error("Transfer failed due to configuration or authorization issues.")
        # else:
        #     logger.info("Transfer successful.")

        alcf_tomopy_reconstruction_flow()
        
if __name__ == "__main__":
    file = Path("/Users/david/Documents/code/test.txt")
    new_file = str(file.with_name(f"test_{str(uuid.uuid4())}.txt"))
    process_new_832_file(new_file)