from dotenv import load_dotenv
import os

from prefect import flow, task, get_run_logger

from orchestration.flows.bl832.alcf import transfer_data_to_data832
from orchestration.flows.bl832.config import Config832
from orchestration.nersc import NerscClient


@task(name="create_nersc_client")
def create_nersc_client():
    load_dotenv()
    logger = get_run_logger()

    # Get paths to NERSC client ID and SFAPI key
    # Note: These paths are set in the .env file
    # We should consider moving these to the Prefect Secrets Manager
    # We should also consider how to handle the short SFAPI key expiration time (2 days)
    client_id_path = os.getenv("PATH_NERSC_CLIENT_ID")
    sfapi_key_path = os.getenv("PATH_NERSC_PRI_KEY")

    # Log paths for debugging (ensure no sensitive info is printed)
    logger.info(f"Client ID Path: {client_id_path}")
    logger.info(f"SFAPI Key Path: {sfapi_key_path}")

    # Verify that the paths are not None
    if not client_id_path or not sfapi_key_path:
        logger.error("Environment variables for NERSC credentials are not set.")
        raise ValueError("Missing NERSC credentials paths.")

    # Check if files exist
    if not os.path.isfile(client_id_path):
        logger.error(f"Client ID file not found at {client_id_path}")
        raise FileNotFoundError(f"Client ID file not found at {client_id_path}")
    if not os.path.isfile(sfapi_key_path):
        logger.error(f"SFAPI Key file not found at {sfapi_key_path}")
        raise FileNotFoundError(f"SFAPI Key file not found at {sfapi_key_path}")

    try:
        client = NerscClient(client_id_path, sfapi_key_path)
    except Exception as e:
        logger.error(f"Failed to create NERSC client: {e}")
        raise e

    return client


@task(name="submit_recon_job_script")
def submit_recon_job_script(
    client: NerscClient,
    # file_path: str = None,
) -> bool:
    logger = get_run_logger()
    if client is None:
        logger.error("NERSC client is required for job submission.")
        raise ValueError("NERSC client is required for job submission.")
    # if file_path is None:
    #     logger.error("File path is required for job submission.")
    #     raise ValueError("File path is required for job submission.")

    load_dotenv()
    username = os.getenv("NERSC_USERNAME")
    password = os.getenv("NERSC_PASSWORD")

    user = client.user()

    home_path = f"/global/homes/{user.name[0]}/{user.name}"
    scratch_path = f"/pscratch/sd/{user.name[0]}/{user.name}"
    logger.info(home_path)
    logger.info(scratch_path)

    try:
        logger.info(f"Creating directory: {scratch_path}/prefect-recon-test")
        client.perlmutter.run(f"mkdir -p {scratch_path}/prefect-recon-test")
        logger.info("Directory created successfully.")
    except Exception as e:
        logger.error(f"Failed to create directory: {e}")
        raise e

    # Need to update this script:
        # take in the file path
        # ignore input.txt
        # run the reconstruction script with the file path
        # run tiff to zarr after reconstruction

    job_script = f"""#!/bin/bash
#SBATCH -q debug
#SBATCH -A als
#SBATCH -C cpu
#SBATCH --job-name=tomorecon_nersc_mpi_hdf5_1-0
#SBATCH --output={scratch_path}/nerscClient-test/%x_%j.out
#SBATCH --error={scratch_path}/nerscClient-test/%x_%j.err
#SBATCH -N 1
#SBATCH --ntasks-per-node 1
#SBATCH --cpus-per-task 64
#SBATCH --time=00:15:00
#SBATCH --exclusive

date
srun podman-hpc login registry.nersc.gov --username {username} --password {password}
srun podman-hpc run
--volume {home_path}/tomo_recon_repo/microct/legacy/reconstruction.py:/alsuser/reconstruction.py
--volume {home_path}/tomo_recon_repo/microct/legacy/input.txt:/alsuser/input.txt
--volume {scratch_path}/microctdata:/alsdata
--volume {scratch_path}/microctdata:/alsuser/ registry.nersc.gov/als/tomorecon_nersc_mpi_hdf5@sha256:cc098a2cfb6b1632ea872a202c66cb7566908da066fd8f8c123b92fa95c2a43c python reconstruction.py input.txt
date
"""
# srun podman-hpc run --volume {home_path}/tomo_recon_repo/microct/legacy/reconstruction.py:/alsuser/reconstruction.py --volume {home_path}/tomo_recon_repo/microct/legacy/input.txt:/alsuser/input.txt --volume {scratch_path}/microctdata:/alsdata localhost/tomorecon_nersc_mpi_hdf5:1.0 python reconstruction.py input.txt

    try:
        logger.info("Submitting reconstruction job script to Perlmutter.")
        job = client.perlmutter.submit_job(job_script)
        job.complete()  # waits for job completion
        logger.info("Reconstruction job completed successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to submit or complete reconstruction job: {e}")
        return False


@task(name="submit_tiff_to_zarr_job_script")
def submit_tiff_to_zarr_job_script(
    client: NerscClient,
    script_path: str = "tiff_to_zarr.py",
    recon_path: str = None,
    raw_path: str = None
) -> bool:
    logger = get_run_logger()
    if client is None:
        logger.error("NERSC client is required for job submission.")
        raise ValueError("NERSC client is required for job submission.")

    load_dotenv()
    username = os.getenv("NERSC_USERNAME")
    password = os.getenv("NERSC_PASSWORD")

    user = client.user()

    home_path = f"/global/homes/{user.name[0]}/{user.name}"
    scratch_path = f"/pscratch/sd/{user.name[0]}/{user.name}"
    logger.info(home_path)
    logger.info(scratch_path)

    try:
        logger.info(f"Creating directory: {scratch_path}/prefect-recon-test")
        client.perlmutter.run(f"mkdir -p {scratch_path}/prefect-recon-test")
        logger.info("Directory created successfully.")
    except Exception as e:
        logger.error(f"Failed to create directory: {e}")
        raise e

    # Need to update this script:
        # take in the file path
        # ignore input.txt
        # run the reconstruction script with the file path
        # run tiff to zarr after reconstruction

    job_script = f"""#!/bin/bash
#SBATCH -q debug
#SBATCH -A als
#SBATCH -C cpu
#SBATCH --job-name=tomorecon_nersc_mpi_hdf5_1-0
#SBATCH --output={scratch_path}/nerscClient-test/%x_%j.out
#SBATCH --error={scratch_path}/nerscClient-test/%x_%j.err
#SBATCH -N 1
#SBATCH --ntasks-per-node 1
#SBATCH --cpus-per-task 64
#SBATCH --time=00:15:00
#SBATCH --exclusive

date
srun podman-hpc login registry.nersc.gov --username {username} --password {password}
srun podman-hpc run --volume {home_path}/tomo_recon_repo/microct/legacy/tiff_to_zarr.py:/alsuser/tiff_to_zarr.py \
--volume {home_path}/tomo_recon_repo/microct/legacy/input.txt:/alsuser/input.txt \
--volume {scratch_path}/microctdata:/alsdata \
--volume {scratch_path}/microctdata:/alsuser/ \
registry.nersc.gov/als/tomorecon_nersc_mpi_hdf5@sha256:cc098a2cfb6b1632ea872a202c66cb7566908da066fd8f8c123b92fa95c2a43c \
bash -c "python -m pip show ngff_zarr || python -m pip install ngff_zarr && python -m pip show dask_image || python -m pip install dask_image && python {script_path} {recon_path} --raw_file {raw_path}"
date
"""
    try:
        logger.info("Submitting Tiff to Zarr job script to Perlmutter.")
        job = client.perlmutter.submit_job(job_script)
        logger.info(f"jobid={job.job_id}")
        job.complete()  # waits for job completion
        logger.info("Tiff to Zarr job completed successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to submit or complete Tiff to Zarr job: {e}")
        return False


@flow(name="nersc_recon_flow")
def nersc_recon_flow(
    file_path: str = "",
    is_export_control: bool = False,
    config=None,
):
    logger = get_run_logger()
    logger.info("Starting NERSC flow for new file processing and transfer.")
    if not config:
        config = Config832()

    if not is_export_control:
        logger.info("File is not export controlled, will run reconstruction at NERSC.")

        # Step 1: Check if file exists at NERSC
        # Data should already have been transferred to NERSC in new_832_file_flow (move.py)
        # transfer_client = config.tc

        # nersc_raw_path = file_path.split("/global")[1]
        # directory_path = os.path.dirname(os.path.join(config.nersc832.root_path, nersc_raw_path))
        # file_name = os.path.basename(nersc_raw_path)

        # # List the directory contents and check if the file exists
        # file_exists = any(
        #     item["name"] == file_name and item["type"] == "file"
        #     for item in transfer_client.operation_ls(
        #         endpoint_id=config.nersc832.uuid,
        #         path=directory_path
        #     )["DATA"]
        # )

        file_exists = True

        # If file exists, run submit_job_script
        if file_exists:
            # logger.info(f"File {file_name} found at NERSC.")
            # Creating a sfapi client object
            client = create_nersc_client()
            logger.info("NERSC SFAPI Client created")

            # Step 2A: If raw h5 file exists on NERSC, run submit_job_script
            # Update submit_job_script to take in the file path
            # Job submission step
            nersc_reconstruction_success = submit_recon_job_script(
                client=client
            )
            # file_path=nersc_raw_path)
            if not nersc_reconstruction_success:
                logger.error("Reconstruction Failed.")
                raise ValueError("Reconstruction at NERSC Failed")

            else:
                logger.info("Reconstruction Successful.")
                # Step 2B: Run tiff_to_zarr after reconstruction
                # user = client.user()
                nersc_tiff_scratch_path = "rec20230606_151124_jong-seto_fungal-mycelia_roll-AQ_fungi1_fast"
                nersc_raw_path = "20230606_151124_jong-seto_fungal-mycelia_roll-AQ_fungi1_fast.h5"
                nersc_tiff_to_zarr_success = submit_tiff_to_zarr_job_script(
                    client=client,
                    recon_path=nersc_tiff_scratch_path,
                    raw_path=nersc_raw_path)
                if not nersc_tiff_to_zarr_success:
                    logger.error("Tiff to Zarr Failed.")
                    raise ValueError("Tiff to Zarr at ALCF Failed")
                else:
                    logger.info("Tiff to Zarr Successful.")

        else:
            logger.error(f"File {file_name} not found at NERSC.")
            return

        # if nersc_reconstruction_success:
        #     # Step 3A: Send reconstructed data (tiff) to data832
        #     # transfer reconstructions to data832
        #     nersc_tiff_scratch_path = ""
        #     transfer_data_to_data832(
        #         file_path=nersc_tiff_scratch_path,
        #         transfer_client=config.tc,
        #         source_endpoint=config.nersc832,
        #         data832=config.data832
        #     )

        # if nersc_tiff_to_zarr_success:
        #     # Step 3B: Send zarr data to data832
        #     nersc_zarr_scratch_path = ""
        #     transfer_data_to_data832(
        #         file_path=nersc_zarr_scratch_path,
        #         transfer_client=config.tc,
        #         source_endpoint=config.nersc832,
        #         data832=config.data832
        #     )

        # Step 4 Schedule file deletion

    else:
        logger.info("File is export controlled, not running reconstruction at NERSC.")
        return

    return


if __name__ == "__main__":
    nersc_recon_flow()
