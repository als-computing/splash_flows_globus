from dotenv import load_dotenv, set_key
from globus_compute_sdk import Client, Executor
from orchestration.globus_flows_utils import get_flows_client, get_specific_flow_client


"""
init.py only needs to be run once to authenticate the polaris (alcf) endpoint ID on the target machine.
Additionally, it sets up the functions that will be used to transfer data.
"""

def reconstruction_wrapper(rundir, parametersfile="inputOneSliceOfEach.txt"):
    
    """
    Python function that wraps around the application call for Tomopy reconstruction on ALCF

    Args:
        rundir (str): the directory on the eagle file system (ALCF) where the input data are located
        parametersfile (str, optional): Defaults to "inputOneSliceOfEach.txt", which is already on ALCF

    Returns:
        str: confirmation message regarding reconstruction and time to completion
    """
    import os
    import subprocess
    import time
    try:
        start = time.time()

        # Move to directory where data are located
        os.chdir(rundir)

        # Run reconstruction.py
        command = f"python /eagle/IRIBeta/als/example/reconstruction.py {parametersfile}"
        res = subprocess.run(command.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        end = time.time()
        return f"Reconstructed data specified in {parametersfile} in {end-start:.2f} seconds;\n {res.stdout.decode()}"

    except Exception as e:
        return f"Error during reconstruction: {str(e)}"

def create_flow_definition():
    flow_definition = {
        "Comment": "Run Reconstruction and transfer results",
        "StartAt": "Reconstruction",
        "States": {
            "Reconstruction": {
                "Comment": "Reconstruction with Tomopy",
                "Type": "Action",
                "ActionUrl": "https://compute.actions.globus.org/fxap",
                "Parameters": {
                    "endpoint.$": "$.input.compute_endpoint_id",
                    "function.$": "$.input.compute_function_id",
                    "kwargs.$": "$.input.compute_function_kwargs"
                },
                "ResultPath": "$.ReconstructionOutput",
                "WaitTime": 3600,
                "Next": "Transfer_Out"
            },
            "Transfer_Out": {
                "Comment": "Transfer files",
                "Type": "Action",
                "ActionUrl": "https://actions.automate.globus.org/transfer/transfer",
                "Parameters": {
                    "source_endpoint_id.$": "$.input.source.id",
                    "destination_endpoint_id.$": "$.input.destination.id",
                    "transfer_items": [
                        {
                            "source_path.$": "$.input.source.path",
                            "destination_path.$": "$.input.destination.path",
                            "recursive.$": "$.input.recursive_tx"
                        }
                    ]
                },
                "ResultPath": "$.TransferFiles",
                "WaitTime": 300,
                "End": True
            },
        }
    }
    return flow_definition


if __name__ == "__main__":
    gc = Client()
    dotenv_file = load_dotenv()
    polaris_endpoint_id = "3e0b1459-e007-4c70-a5db-ea625a4cb3bf"
    gce = Executor(endpoint_id=polaris_endpoint_id)
    future = gce.submit(reconstruction_wrapper, "/eagle/IRIBeta/als/example")
    # print(future.result())
    reconstruction_func = gc.register_function(reconstruction_wrapper)
    print(reconstruction_func)
    future = gce.submit_to_registered_function(args=["/eagle/IRIBeta/als/example"], function_id=reconstruction_func)
    future.result()
    flow_definition = create_flow_definition()
    fc = get_flows_client()
    flow = fc.create_flow(definition=flow_definition, title="Reconstruction flow", input_schema={})
    flow_id = flow['id']
    print(flow)
    flow_scope = flow['globus_auth_scope']
    print(f'Newly created flow with id:\n{flow_id}\nand scope:\n{flow_scope}')
    # set_key(dotenv_file, "flow_id", str(flow_id))