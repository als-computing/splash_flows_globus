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


def conversion_wrapper(rundir, h5_file_name, folder_path):
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

    start = time.time()

    # Move to directory where data are located
    os.chdir(rundir)

    # Convert tiff files to zarr
    file_name = h5_file_name[:-3] if h5_file_name.endswith('.h5') else h5_file_name
    command = (
        f"python /eagle/IRIBeta/als/example/tiff_to_zarr.py "
        f"/eagle/IRIBeta/als/bl832/scratch/{folder_path}/rec{file_name}/"
    )
    zarr_res = subprocess.run(command.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    end = time.time()

    return (
        f"Converted tiff files to zarr in {end-start} seconds;\n {zarr_res}"
    )


def create_flow_definition():
    flow_definition = {
        "Comment": "Run Tiff to Zarr conversion",
        "StartAt": "TiffToZarr",
        "States": {
            "TiffToZarr": {
                "Comment": "Convert reconstructed Tiff files to Zarr",
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


@flow(name="setup-tiff-to-zarr-flow")
def setup_tiff_to_zarr_flow() -> None:
    logger = get_run_logger()

    # Login to Globus Compute Endpoint
    gc = Client()
    gce = Executor(endpoint_id=get_polaris_endpoint_id())
    print(gce)

    reconstruction_func = gc.register_function(conversion_wrapper)

    flows_client = get_flows_client()
    flow = flows_client.create_flow(definition=create_flow_definition(),
                                    title="Tiff to Zarr flow",
                                    input_schema={})
    flow_id = flow['id']

    # flow_scope = flow['globus_auth_scope']
    # logger.info(f'Flow scope:\n{flow_scope}')
    logger.info(f"Registered function UUID: {reconstruction_func}")
    logger.info(f"Flow UUID: {flow_id}")


if __name__ == "__main__":
    setup_tiff_to_zarr_flow()
