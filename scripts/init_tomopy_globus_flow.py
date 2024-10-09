from globus_compute_sdk import Client, Executor
from prefect.blocks.system import JSON, Secret
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


def reconstruction_wrapper(rundir="/eagle/IRI-ALS-832/data/raw",
                           script_path="/eagle/IRI-ALS-832/scripts/globus_reconstruction.py",
                           h5_file_name=None,
                           folder_path=None) -> str:
    """
    Python function that wraps around the application call for Tomopy reconstruction on ALCF

    Args:
        rundir (str): the directory on the eagle file system (ALCF) where the input data are located
        script_path (str): the path to the script that will run reconstruction
        h5_file_name (str): the name of the h5 file to be reconstructed
        folder_path (str): the path to the folder where the h5 file is located

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
    command = f"python {script_path} {h5_file_name} {folder_path}"
    recon_res = subprocess.run(command.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    rec_end = time.time()

    print(f"Reconstructed data in {folder_path}/{h5_file_name} in {rec_end-rec_start} seconds;\n {recon_res}")

    return (
        f"Reconstructed data specified in {folder_path} / {h5_file_name} in {rec_end-rec_start} seconds;\n"
        f"{recon_res}"
    )


def create_flow_definition() -> dict:
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


@task(name="update_reconstruction_flow_in_prefect")
def update_reconstruction_flow_in_prefect(reconstruction_func: str, flow_id: str) -> None:
    # Create JSON block with flow_id
    flow_json = JSON(value={"flow_id": flow_id})
    flow_json.save(name="globus-reconstruction-flow-id", overwrite=True)

    # Create JSON block with conversion_func
    func_json = JSON(value={"reconstruction_func": reconstruction_func})
    func_json.save(name="globus-reconstruction-function", overwrite=True)


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

    logger.info(f"Registered function UUID: {reconstruction_func}")
    logger.info(f"Flow UUID: {flow_id}")

    update_reconstruction_flow_in_prefect(reconstruction_func, flow_id)


if __name__ == "__main__":
    setup_reconstruction_flow()
