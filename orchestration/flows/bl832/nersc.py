from dotenv import load_dotenv
import logging
import os
from pathlib import Path
from prefect import flow
import time
from typing import Optional

from orchestration.flows.bl832.config import Config832
from orchestration.flows.bl832.job_controller import get_controller, HPC, TomographyHPCController
from orchestration.nersc import NerscClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()


class NERSCTomographyHPCController(TomographyHPCController):
    """
    Implementation for a NERSC-based tomography HPC controller.

    Submits reconstruction and multi-resolution jobs to NERSC via SFAPI.
    """

    def __init__(
        self,
        client: NerscClient = None,
        config: Optional[Config832] = None
    ) -> None:
        self.client = client

        if not config:
            self.config = Config832()
        else:
            self.config = config

    def create_nersc_client() -> NerscClient:
        """Create and return an NERSC client instance"""

        client_id_path = os.getenv("PATH_NERSC_CLIENT_ID")
        sfapi_key_path = os.getenv("PATH_NERSC_PRI_KEY")

        if not client_id_path or not sfapi_key_path:
            logger.error("NERSC credentials paths are missing.")
            raise ValueError("Missing NERSC credentials paths.")
        if not os.path.isfile(client_id_path) or not os.path.isfile(sfapi_key_path):
            logger.error("NERSC credential files are missing.")
            raise FileNotFoundError("NERSC credential files are missing.")

        try:
            return NerscClient(client_id_path, sfapi_key_path)
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

        # Can't use this long term in production. Need to find a better way to handle credentials.
        # Want to run this as the alsdev user
        # username = os.getenv("NERSC_USERNAME")
        # password = os.getenv("NERSC_PASSWORD")

        user = self.client.user()

        home_path = f"/global/homes/{user.name[0]}/{user.name}"
        scratch_path = f"/pscratch/sd/{user.name[0]}/{user.name}"
        logger.info(home_path)
        logger.info(scratch_path)

        image_name = self.config.harbor_images832["recon_image"]
        logger.info(image_name)
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

        # If the image has not been pulled before,
        # then you must login to Harbor first (hopefully we can get a robot account)
        # Looking into using github actions to build the image and host it on on github instead
        # srun podman-hpc login registry.nersc.gov --username {username} --password {password}

        job_script = f"""#!/bin/bash
#SBATCH -q debug
#SBATCH -A als
#SBATCH -C cpu
#SBATCH --job-name=tomo_recon_test-0
#SBATCH --output={scratch_path}/nerscClient-test/%x_%j.out
#SBATCH --error={scratch_path}/nerscClient-test/%x_%j.err
#SBATCH -N 1
#SBATCH --ntasks-per-node 1
#SBATCH --cpus-per-task 64
#SBATCH --time=0:15:00
#SBATCH --exclusive

date
srun podman-hpc run \
--volume {home_path}/tomo_recon_repo/microct/legacy/sfapi_reconstruction.py:/alsuser/sfapi_reconstruction.py \
--volume {scratch_path}/microctdata:/alsdata \
--volume {scratch_path}/microctdata:/alsuser/ \
registry.nersc.gov/als/{image_name} \
bash -c "python -m pip install numpy==1.23.2 && \
python sfapi_reconstruction.py {file_name} {folder_name}"
date
"""

        try:
            logger.info("Submitting reconstruction job script to Perlmutter.")
            job = self.client.perlmutter.submit_job(job_script)
            logging.info(job.jobid)
            job.update()
            time.sleep(60)  # Wait 60 seconds for job to register before checking status
            logging.info(job.state)
            job.complete()  # waits for job completion
            logger.info("Reconstruction job completed successfully.")
            return True
        except Exception as e:
            logger.error(f"Failed to submit or complete reconstruction job: {e}")
            return False

    def build_multi_resolution(
        self,
        file_path: str = "",
    ) -> bool:
        """Use NERSC to make multiresolution version of tomography results."""

        # username = os.getenv("NERSC_USERNAME")
        # password = os.getenv("NERSC_PASSWORD")

        user = self.client.user()

        home_path = f"/global/homes/{user.name[0]}/{user.name}"
        scratch_path = f"/pscratch/sd/{user.name[0]}/{user.name}"
        logger.info(home_path)
        logger.info(scratch_path)

        image_name = self.config.harbor_images832["multires_image"]

        # TODO: fix these paths

        path = Path(file_path)
        folder_name = path.parent.name
        file_name = path.stem

        recon_path = f"scratch/{folder_name}/rec{file_name}/"
        raw_path = f"{folder_name}/{file_name}.h5"

        # Need to update this script:
        # rebuild image with dependencies

        # IMPORTANT: job script must be deindented to the leftmost column or it will fail immediately
        job_script = f"""#!/bin/bash
#SBATCH -q debug
#SBATCH -A als
#SBATCH -C cpu
#SBATCH --job-name=tomo_multires_test-0
#SBATCH --output={scratch_path}/nerscClient-test/%x_%j.out
#SBATCH --error={scratch_path}/nerscClient-test/%x_%j.err
#SBATCH -N 1
#SBATCH --ntasks-per-node 1
#SBATCH --cpus-per-task 64
#SBATCH --time=0:15:00
#SBATCH --exclusive

date
srun podman-hpc run --volume {home_path}/tomo_recon_repo/microct/legacy/tiff_to_zarr.py:/alsuser/tiff_to_zarr.py \
--volume {home_path}/tomo_recon_repo/microct/legacy/input.txt:/alsuser/input.txt \
--volume {scratch_path}/microctdata:/alsdata \
--volume {scratch_path}/microctdata:/alsuser/ \
registry.nersc.gov/als/{image_name} \
bash -c "python -m pip show ngff_zarr || python -m pip install ngff_zarr && \
python -m pip show dask_image || python -m pip install dask_image && \
python tiff_to_zarr.py {recon_path} --raw_file {raw_path}"

date
"""
        try:
            logger.info("Submitting Tiff to Zarr job script to Perlmutter.")
            job = self.client.perlmutter.submit_job(job_script)
            time.sleep(30)  # Wait 30 seconds before checking job completion
            job.complete()  # waits for job completion
            logger.info("Tiff to Zarr job completed successfully.")
            return True
        except Exception as e:
            logger.error(f"Failed to submit or complete Tiff to Zarr job: {e}")
            return False


@flow(name="nersc_recon_flow")
def nersc_recon_flow(
    file_path: str,
) -> bool:
    """
    Perform tomography reconstruction on NERSC.

    :param file_path: Path to the file to reconstruct.
    """

    # To do: Implement file transfers, pruning, and other necessary steps

    controller = get_controller(HPC.NERSC)
    nersc_reconstruction_success = controller.reconstruct(
        file_path=file_path,
    )
    nersc_multi_res_success = controller.build_multi_resolution(
        file_path=file_path,
    )

    if nersc_reconstruction_success and nersc_multi_res_success:
        return True
    else:
        return False


if __name__ == "__main__":
    nersc_recon_flow(file_path="dabramov/20230606_151124_jong-seto_fungal-mycelia_roll-AQ_fungi1_fast.h5")
