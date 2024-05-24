# import os
# import time
# from prefect import flow, task, get_run_logger
# from globus_compute_sdk import Client, Executor
# from globus_compute_sdk.serialize import CombinedCode

# from orchestration.globus_flows_utils import get_flows_client, get_specific_flow_client
# from orchestration.flows.bl832.config import Config832
# from globus_prefect_test.orchestration.globus_tomopy_flow_init import reconstruction_wrapper

# @flow(name="reconstruction_wrapper")
# def reconstruction_wrapper(rundir, parametersfile="inputOneSliceOfEach.txt"):
#     """
#     Python function that wraps around the application call for Tomopy reconstruction on ALCF

#     Args:
#         rundir (str): the directory on the eagle file system (ALCF) where the input data are located
#         parametersfile (str, optional): Defaults to "inputOneSliceOfEach.txt", which is already on ALCF

#     Returns:
#         str: confirmation message regarding reconstruction and time to completion
#     """
#     import time
#     import os
#     import subprocess
#     start = time.time()

#     # Move to directory where data are located
#     os.chdir(rundir)

#     # Run reconstruction.py
#     command = f"python /eagle/IRIBeta/als/example/reconstruction.py {parametersfile}"
#     res = subprocess.run(command.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

#     end = time.time()
    
#     return f"Reconstructed data specified in {parametersfile} in {end-start} seconds;\n {res}"

# @flow(name="alcf_tomopy_reconstruction_flow")
# def alcf_tomopy_reconstruction_flow():
#     logger = get_run_logger()
#     config = Config832()
    
#     # Initialize the Globus Compute Client with CombinedCode serialization strategy
#     gcc = Client(code_serialization_strategy=CombinedCode())
#     polaris_endpoint_id = os.getenv("GLOBUS_COMPUTE_ENDPOINT") # COMPUTE endpoint, not TRANSFER endpoint
#     gce = Executor(endpoint_id=polaris_endpoint_id, client=gcc)

#     # Register and submit the function
#     reconstruction_func = gcc.register_function(reconstruction_wrapper)
#     logger.info(f"Function registered with ID: {reconstruction_func}")
#     future = gce.submit_to_registered_function(args=["/eagle/IRIBeta/als/example"], function_id=reconstruction_func)
    
#     # Wait for the result and log it
#     try:
#         result = future.result()
#         logger.info(f"Reconstruction completed with result: {result}")
#     except Exception as e:
#         logger.error(f"Error during function submission or execution: {e}")
#         return  # Exit if there is an error during reconstruction

#     # Where the data will transfer to after reconstruction (NERSC)
#     alcfhome_transfer_endpoint_id = config.alcf_home832.uuid
#     destination_path_on_alcfhome = config.alcf_home832.root_path

#     # Where the raw data are located to be reconstructed (ALCF)
#     eagle_transfer_endpoint_id = config.alcf_eagle832.uuid
#     source_path_on_eagle = config.alcf_eagle832.root_path

#     function_inputs = {"rundir": "/eagle/IRIBeta/als/example"}

#     # Define the json flow
#     flow_input = {
#         "input": {
#         "source": {
#             "id": eagle_transfer_endpoint_id,
#             "path": source_path_on_eagle
#         },
#         "destination": {
#             "id": alcfhome_transfer_endpoint_id,
#             "path": destination_path_on_alcfhome
#         },
#         "recursive_tx": True,
#         "compute_endpoint_id": polaris_endpoint_id,
#         "compute_function_id": reconstruction_func,
#         "compute_function_kwargs": function_inputs
#         }
#     }
#     collection_ids = [flow_input["input"]["source"]["id"], flow_input["input"]["destination"]["id"]]

#     # Maybe this is the missing link:
#     # flow_run_id = flow_action['action_id'] 

#     # Flow ID (only generate once!)
#     flow_id = os.getenv("GLOBUS_FLOW_ID")

#     # Run the flow
#     fc = get_flows_client()
#     run_client = get_specific_flow_client(flow_id, collection_ids=collection_ids)
#     try:
#         flow_action = run_client.run_flow(flow_input, label="ALS run", tags=["demo", "als", "tomopy"])
#         flow_run_id = flow_action['action_id']
#         logger.info(f'Flow action started with id: {flow_run_id}')
#         logger.info(f"Monitor your flow here: https://app.globus.org/runs/{flow_run_id}")

#         # Monitor flow status
#         flow_status = flow_action['status']
#         logger.info(f'Initial flow status: {flow_status}')
#         while flow_status in ['ACTIVE', 'INACTIVE']:
#             time.sleep(10)
#             flow_action = fc.get_run(flow_run_id)
#             flow_status = flow_action['status']
#             logger.info(f'Updated flow status: {flow_status}')
#             # Log additional details about the flow status
#             logger.info(f'Flow action details: {flow_action}')

#         if flow_status != 'SUCCEEDED':
#             logger.error(f'Flow failed with status: {flow_status}')
#             # Log additional details about the failure
#             logger.error(f'Flow failure details: {flow_action}')
#         else:
#             logger.info(f'Flow completed successfully with status: {flow_status}')
#     except Exception as e:
#         logger.error(f"Error running flow: {e}")
