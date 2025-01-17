import datetime
from dotenv import load_dotenv
import json
import logging
import os
from pathlib import Path
import re
import time

from authlib.jose import JsonWebKey
# from globus_sdk import TransferClient
from prefect import flow  # , task
from prefect.blocks.system import JSON
from sfapi_client import Client
from sfapi_client.compute import Machine

from orchestration.flows.bl832.config import Config832
from orchestration.flows.bl832.job_controller import get_controller, HPC, TomographyHPCController
from orchestration.globus.transfer import GlobusEndpoint
from orchestration.transfer_controller import get_transfer_controller, CopyMethod
from orchestration.prefect import schedule_prefect_flow

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()


# @task(name="transfer_data_at_nersc")
# def transfer_data_at_nersc(
#     file_path: str,
#     transfer_client: TransferClient,
#     nersc_source: GlobusEndpoint,
#     nersc_destination: GlobusEndpoint,
# ):
#     # if source_file begins with "/", it will mess up os.path.join
#     if file_path[0] == "/":
#         file_path = file_path[1:]
#     source_path = os.path.join(nersc_source.root_path, file_path)
#     dest_path = os.path.join(nersc_destination.root_path, file_path)

#     logger.info(f"Transferring {dest_path} data832 to nersc")

#     success = start_transfer(
#         transfer_client,
#         nersc_source,
#         source_path,
#         nersc_destination,
#         dest_path,
#         max_wait_seconds=600,
#         logger=logger,
#     )

#     return success


class NERSCTomographyHPCController(TomographyHPCController):
    """
    Implementation for a NERSC-based tomography HPC controller.

    Submits reconstruction and multi-resolution jobs to NERSC via SFAPI.
    """

    def __init__(
        self,
        client: Client,
        config: Config832
    ) -> None:
        self.client = client
        self.config = config

    @staticmethod
    def create_sfapi_client() -> Client:
        """Create and return an NERSC client instance"""

        client_id_path = os.getenv("PATH_NERSC_CLIENT_ID")
        client_secret_path = os.getenv("PATH_NERSC_PRI_KEY")

        if not client_id_path or not client_secret_path:
            logger.error("NERSC credentials paths are missing.")
            raise ValueError("Missing NERSC credentials paths.")
        if not os.path.isfile(client_id_path) or not os.path.isfile(client_secret_path):
            logger.error("NERSC credential files are missing.")
            raise FileNotFoundError("NERSC credential files are missing.")

        client_id = None
        client_secret = None
        with open(client_id_path, "r") as f:
            client_id = f.read()

        with open(client_secret_path, "r") as f:
            client_secret = JsonWebKey.import_key(json.loads(f.read()))

        try:
            client = Client(client_id, client_secret)
            logger.info("NERSC client created successfully.")
            return client
        except Exception as e:
            logger.error(f"Failed to create NERSC client: {e}")
            raise e

    def reconstruct(
        self,
        file_path: str = "",
    ) -> bool:
        """
        Use NERSC for tomography reconstruction
        """
        logger.info("Starting NERSC reconstruction process.")

        user = self.client.user()

        raw_path = self.config.nersc832_alsdev_raw.root_path
        logger.info(f"{raw_path=}")

        recon_image = self.config.ghcr_images832["recon_image"]
        logger.info(f"{recon_image=}")

        recon_scripts_dir = self.config.nersc832_alsdev_recon_scripts.root_path
        logger.info(f"{recon_scripts_dir=}")

        scratch_path = self.config.nersc832_alsdev_scratch.root_path
        logger.info(f"{scratch_path=}")

        pscratch_path = f"/pscratch/sd/{user.name[0]}/{user.name}"
        logger.info(f"{pscratch_path=}")

        path = Path(file_path)
        folder_name = path.parent.name
        if not folder_name:
            folder_name = ""

        file_name = f"{path.stem}.h5"

        logger.info(f"File name: {file_name}")
        logger.info(f"Folder name: {folder_name}")

        # IMPORTANT: job script must be deindented to the leftmost column or it will fail immediately
        # Note: If q=debug, there is no minimum time limit
        # However, if q=preempt, there is a minimum time limit of 2 hours. Otherwise the job won't run.

        job_script = f"""#!/bin/bash
#SBATCH -q realtime
#SBATCH -A als
#SBATCH -C cpu
#SBATCH --job-name=tomo_recon_{folder_name}_{file_name}
#SBATCH --output={pscratch_path}/tomo_recon_logs/%x_%j.out
#SBATCH --error={pscratch_path}/tomo_recon_logs/%x_%j.err
#SBATCH -N 1
#SBATCH --ntasks-per-node 1
#SBATCH --cpus-per-task 64
#SBATCH --time=0:15:00
#SBATCH --exclusive

date
echo "Creating directory {pscratch_path}/8.3.2/raw/{folder_name}"
mkdir -p {pscratch_path}/8.3.2/raw/{folder_name}
mkdir -p {pscratch_path}/8.3.2/scratch/{folder_name}

echo "Copying file {raw_path}/{folder_name}/{file_name} to {pscratch_path}/8.3.2/raw/{folder_name}/"
cp {raw_path}/{folder_name}/{file_name} {pscratch_path}/8.3.2/raw/{folder_name}
if [ $? -ne 0 ]; then
    echo "Failed to copy data to pscratch."
    exit 1
fi

chmod -R 2775 {pscratch_path}/8.3.2

echo "Verifying copied files..."
ls -l {pscratch_path}/8.3.2/raw/{folder_name}/

echo "Running reconstruction container..."
srun podman-hpc run \
--volume {recon_scripts_dir}/sfapi_reconstruction.py:/alsuser/sfapi_reconstruction.py \
--volume {pscratch_path}/8.3.2:/alsdata \
--volume {pscratch_path}/8.3.2:/alsuser/ \
{recon_image} \
bash -c "python sfapi_reconstruction.py {file_name} {folder_name}"
date
"""

        try:
            logger.info("Submitting reconstruction job script to Perlmutter.")
            perlmutter = self.client.compute(Machine.perlmutter)
            job = perlmutter.submit_job(job_script)
            logger.info(f"Submitted job ID: {job.jobid}")

            try:
                job.update()
            except Exception as update_err:
                logger.warning(f"Initial job update failed, continuing: {update_err}")

            time.sleep(60)
            logger.info(f"Job {job.jobid} current state: {job.state}")

            job.complete()  # Wait until the job completes
            logger.info("Reconstruction job completed successfully.")
            return True

        except Exception as e:
            logger.info(f"Error during job submission or completion: {e}")
            match = re.search(r"Job not found:\s*(\d+)", str(e))

            if match:
                jobid = match.group(1)
                logger.info(f"Attempting to recover job {jobid}.")
                try:
                    job = self.client.perlmutter.job(jobid=jobid)
                    time.sleep(30)
                    job.complete()
                    logger.info("Reconstruction job completed successfully after recovery.")
                    return True
                except Exception as recovery_err:
                    logger.error(f"Failed to recover job {jobid}: {recovery_err}")
                    return False
            else:
                # Unknown error: cannot recover
                return False

    def build_multi_resolution(
        self,
        file_path: str = "",
    ) -> bool:
        """Use NERSC to make multiresolution version of tomography results."""

        logger.info("Starting NERSC multiresolution process.")

        user = self.client.user()

        multires_image = self.config.ghcr_images832["multires_image"]
        logger.info(f"{multires_image=}")

        recon_scripts_dir = self.config.nersc832_alsdev_recon_scripts.root_path
        logger.info(f"{recon_scripts_dir=}")

        scratch_path = self.config.nersc832_alsdev_scratch.root_path
        logger.info(f"{scratch_path=}")

        pscratch_path = f"/pscratch/sd/{user.name[0]}/{user.name}"
        logger.info(f"{pscratch_path=}")

        path = Path(file_path)
        folder_name = path.parent.name
        file_name = path.stem

        recon_path = f"scratch/{folder_name}/rec{file_name}/"
        logger.info(f"{recon_path=}")

        raw_path = f"raw/{folder_name}/{file_name}.h5"
        logger.info(f"{raw_path=}")

        # IMPORTANT: job script must be deindented to the leftmost column or it will fail immediately
        job_script = f"""#!/bin/bash
#SBATCH -q realtime
#SBATCH -A als
#SBATCH -C cpu
#SBATCH --job-name=tomo_multires_{folder_name}_{file_name}
#SBATCH --output={pscratch_path}/tomo_recon_logs/%x_%j.out
#SBATCH --error={pscratch_path}/tomo_recon_logs/%x_%j.err
#SBATCH -N 1
#SBATCH --ntasks-per-node 1
#SBATCH --cpus-per-task 64
#SBATCH --time=0:15:00
#SBATCH --exclusive

date

echo "Running multires container..."
srun podman-hpc run \
--volume {recon_scripts_dir}/tiff_to_zarr.py:/alsuser/tiff_to_zarr.py \
--volume {pscratch_path}/8.3.2:/alsdata \
--volume {pscratch_path}/8.3.2:/alsuser/ \
{multires_image} \
bash -c "python tiff_to_zarr.py {recon_path} --raw_file {raw_path}"

date
"""
        try:
            logger.info("Submitting Tiff to Zarr job script to Perlmutter.")
            perlmutter = self.client.compute(Machine.perlmutter)
            job = perlmutter.submit_job(job_script)
            logger.info(f"Submitted job ID: {job.jobid}")

            try:
                job.update()
            except Exception as update_err:
                logger.warning(f"Initial job update failed, continuing: {update_err}")

            time.sleep(60)
            logger.info(f"Job {job.jobid} current state: {job.state}")

            job.complete()  # Wait until the job completes
            logger.info("Reconstruction job completed successfully.")

            # defining this GlobusEndpoint here rather than config.yaml since the root path depends on the SFAPI user name
            # using the same uuid from another endpoint because it's the same alsdev collection
            nersc832_pscratch_endpoint = GlobusEndpoint(
                uuid=self.config.nersc832_alsdev_scratch.uuid,
                uri=self.config.nersc832_alsdev_scratch.uri,
                root_path=f"{pscratch_path}/8.3.2/scratch",
                name="nersc832_pscratch"
            )

            # Working on a permission denied error when transferring
            relative_recon_path = os.path.relpath(recon_path, "scratch")

            transfer_controller = get_transfer_controller(
                transfer_type=CopyMethod.GLOBUS,
                config=self.config
            )

            transfer_controller.copy(
                file_path=relative_recon_path,
                source=nersc832_pscratch_endpoint,
                destination=self.config.nersc832_alsdev_scratch
            )

            transfer_controller.copy(
                file_path=f"{relative_recon_path}.zarr",
                source=nersc832_pscratch_endpoint,
                destination=self.config.nersc832_alsdev_scratch
            )

            # transfer_data_at_nersc(
            #     file_path=relative_recon_path,
            #     transfer_client=self.config.tc,
            #     nersc_source=nersc832_pscratch_endpoint,
            #     nersc_destination=self.config.nersc832_alsdev_scratch)
            # transfer_data_at_nersc(
            #     file_path=f"{relative_recon_path}.zarr",
            #     transfer_client=self.config.tc,
            #     nersc_source=nersc832_pscratch_endpoint,
            #     nersc_destination=self.config.nersc832_alsdev_scratch
            # )
            return True

        except Exception as e:
            logger.warning(f"Error during job submission or completion: {e}")
            match = re.search(r"Job not found:\s*(\d+)", str(e))

            if match:
                jobid = match.group(1)
                logger.info(f"Attempting to recover job {jobid}.")
                try:
                    job = self.client.perlmutter.job(jobid=jobid)
                    time.sleep(30)
                    job.complete()
                    logger.info("Reconstruction job completed successfully after recovery.")
                    return True
                except Exception as recovery_err:
                    logger.error(f"Failed to recover job {jobid}: {recovery_err}")
                    return False
            else:
                return False


def schedule_pruning(config: Config832, file_path: str) -> bool:
    # data832/scratch : 14 days
    # nersc/pscratch : 1 day
    # nersc832/scratch : never?

    pruning_config = JSON.load("pruning-config").value
    data832_delay = datetime.timedelta(days=pruning_config["delete_data832_files_after_days"])
    nersc832_delay = datetime.timedelta(days=pruning_config["delete_nersc832_files_after_days"])
    
    # Delete from data832_scratch
    try:
        source_endpoint = config.data832_scratch
        check_endpoint = config.nersc832_alsdev_scratch
        location = "data832_scratch"

        flow_name = f"delete {location}: {Path(file_path).name}"
        schedule_prefect_flow(
            deployment_name=f"prune_{location}/prune_{location}",
            flow_run_name=flow_name,
            parameters={
                "relative_path": file_path,
                "source_endpoint": source_endpoint,
                "check_endpoint": check_endpoint
            },
            duration_from_now=data832_delay
        )
    except Exception as e:
        logger.error(f"Failed to schedule prune task: {e}")

    # Delete from nersc832_pscratch
    try:
        source_endpoint = config.nersc832_alsdev_pscratch
        check_endpoint = None
        location = "nersc832_alsdev_pscratch"

        flow_name = f"delete {location}: {Path(file_path).name}"
        schedule_prefect_flow(
            deployment_name=f"prune_{location}/prune_{location}",
            flow_run_name=flow_name,
            parameters={
                "relative_path": file_path,
                "source_endpoint": source_endpoint,
                "check_endpoint": check_endpoint
            },
            duration_from_now=nersc832_delay
        )
    except Exception as e:
        logger.error(f"Failed to schedule prune task: {e}")


@flow(name="nersc_recon_flow")
def nersc_recon_flow(
    file_path: str,
    config: Config832,
) -> bool:
    """
    Perform tomography reconstruction on NERSC.

    :param file_path: Path to the file to reconstruct.
    """

    controller = get_controller(
        hpc_type=HPC.NERSC,
        config=config
    )
    nersc_reconstruction_success = controller.reconstruct(
        file_path=file_path,
    )
    nersc_multi_res_success = controller.build_multi_resolution(
        file_path=file_path,
    )

    # TODO: Transfer files to data832

    schedule_pruning(config=config, file_path=file_path)

    # TODO: Ingest into SciCat

    if nersc_reconstruction_success and nersc_multi_res_success:
        return True
    else:
        return False


if __name__ == "__main__":
    nersc_recon_flow(
        file_path="dabramov/20230606_151124_jong-seto_fungal-mycelia_roll-AQ_fungi1_fast.h5",
        config=Config832()
    )
