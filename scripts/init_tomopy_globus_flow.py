from globus_compute_sdk import Client, Executor
from prefect.blocks.system import Secret
from prefect import task, flow, get_run_logger

from check_globus_compute import check_globus_compute_status
from orchestration.globus.flows import get_flows_client


@task
def get_polaris_endpoint_id() -> str:
    """
    Get the UUID of the Polaris endpoint on ALCF.

    :return: str - UUID of the Polaris endpoint.
    """
    compute_endpoint_id = Secret.load("globus-compute-endpoint").get()
    check_globus_compute_status(compute_endpoint_id)
    return compute_endpoint_id


def reconstruction_wrapper(rundir, h5_file_name, folder_path):
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

    rec_start = time.time()

    # Move to directory where data are located
    os.chdir(rundir)

    # Run reconstruction.py
    command = f"python /eagle/IRIBeta/als/example/globus_reconstruction.py {h5_file_name} {folder_path}"
    recon_res = subprocess.run(command.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    rec_end = time.time()

    print(f"Reconstructed data in {folder_path}/{h5_file_name} in {rec_end-rec_start} seconds;\n {recon_res}")

    return (
        f"Reconstructed data specified in {folder_path} / {h5_file_name} in {rec_end-rec_start} seconds;\n"
        f"{recon_res}"
    )


def create_flow_definition():
    flow_definition = {
        "Comment": "Run Reconstruction",
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
                "End": True

            },
        }
    }
    return flow_definition


@flow(name="setup-reconstruction-flow")
def setup_reconstruction_flow() -> None:
    logger = get_run_logger()

    # Login to Globus Compute Endpoint
    gc = Client()
    gce = Executor(endpoint_id=get_polaris_endpoint_id())
    print(gce)

    reconstruction_func = gc.register_function(reconstruction_wrapper)

    flows_client = get_flows_client()
    flow = flows_client.create_flow(definition=create_flow_definition(),
                                    title="Reconstruction flow",
                                    input_schema={})
    flow_id = flow['id']

    # flow_scope = flow['globus_auth_scope']
    # logger.info(f'Flow scope:\n{flow_scope}')
    logger.info(f"Registered function UUID: {reconstruction_func}")
    logger.info(f"Flow UUID: {flow_id}")


if __name__ == "__main__":
    setup_reconstruction_flow()