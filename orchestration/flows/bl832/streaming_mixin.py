import datetime
import json
import pathlib
import time

from prefect import flow, task, get_run_logger
from sfapi_client import Client as SFAPI_Client
from sfapi_client.compute import Machine
from sfapi_client.jobs import JobState, TERMINAL_STATES
from sfapi_client.exceptions import SfApiError
from pathlib import Path

from pydantic import model_validator
from authlib.jose import JsonWebKey

from pydantic_settings import BaseSettings, SettingsConfigDict
from sfapi_client import Client


# We have this in case we want to test out the flow without
# the Tomography Controller, for sfapi client id/key
class NerscStreamingSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    PATH_NERSC_CLIENT_ID: Path = Path("~/.superfacility/client_id.txt")
    CLIENT_ID: str | None = None
    PATH_NERSC_PRI_KEY: Path = Path("~/.superfacility/client_secret.json")

    @model_validator(mode="after")
    def validate_sfapi_paths(self) -> "NerscStreamingSettings":
        self.PATH_NERSC_CLIENT_ID = (
            pathlib.Path(self.PATH_NERSC_CLIENT_ID).expanduser().resolve(strict=True)
        )
        self.PATH_NERSC_PRI_KEY = (
            pathlib.Path(self.PATH_NERSC_PRI_KEY).expanduser().resolve(strict=True)
        )
        if (
            not self.PATH_NERSC_CLIENT_ID.exists()
            or not self.PATH_NERSC_PRI_KEY.exists()
        ):
            raise FileNotFoundError("NERSC credential files are missing.")

        self.CLIENT_ID = self.PATH_NERSC_CLIENT_ID.read_text().strip()

        pri_key = self.PATH_NERSC_PRI_KEY.read_text()
        try:
            JsonWebKey.import_key(json.loads(pri_key))
        except Exception as e:
            raise ValueError(f"Failed to import NERSC private key: {e}")

        return self

    def create_sfapi_client(self) -> Client:
        try:
            pri_key = self.PATH_NERSC_PRI_KEY.read_text()
            client_secret = JsonWebKey.import_key(json.loads(pri_key))

            client = Client(self.CLIENT_ID, client_secret)  # type: ignore
            return client
        except Exception as e:
            raise RuntimeError(f"Failed to create NERSC client: {e}")


cfg = NerscStreamingSettings()


@task(name="monitor_streaming_job")
def monitor_streaming_job(
    client: SFAPI_Client, job_id: str, update_interval: int
) -> bool:
    logger = get_run_logger()
    perlmutter = client.compute(Machine.perlmutter)
    retries = 0
    max_retries = 3

    while True:
        try:
            job = perlmutter.job(jobid=job_id)
            job.update()
            status = job.state

        # noticed that there can be a delay sometimes in the job being found
        # so we retry a few times before giving up
        except SfApiError as e:
            if "Job not found" in e.message and retries < max_retries:
                retries += 1
                logger.warning(
                    f"Job {job_id} not found, retrying ({retries}/{max_retries})..."
                )
                time.sleep(update_interval)
                continue

        except Exception as e:
            logger.error(f"Error monitoring job {job_id}: {e}")
            return False

        if status == JobState.COMPLETED:
            logger.info(f"Streaming job {job_id} completed successfully.")
            return True
        elif status in TERMINAL_STATES:
            logger.info(f"Streaming job {job_id} failed with status: {status}")
            return False

        logger.info(f"Streaming job {job_id} running, status: {status}")
        time.sleep(update_interval)
        retries = 0


class NerscStreamingMixin:
    def start_streaming_service(
        self,
        client: SFAPI_Client,
        walltime: datetime.timedelta,
    ) -> str:
        logger = get_run_logger()
        logger.info("Starting NERSC streaming service process.")

        user = client.user()
        pscratch_path = f"/pscratch/sd/{user.name[0]}/{user.name}"
        home_dir = f"/global/homes/{user.name[0]}/{user.name}"
        DATA_DIR = f"{pscratch_path}/streaming_tomography_reconstructions"
        DOTENV_FILE = f"{home_dir}/gits/als-epics-streaming/operators/832/recon/.env"

        total_seconds = int(walltime.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60

        time_format = f"{hours}:{minutes:02d}:00"

        # Define the streaming job script
        job_script = f"""#!/bin/bash
#SBATCH -q debug
#SBATCH -A als
#SBATCH -C gpu
#SBATCH --job-name=streaming-tomo-recon
#SBATCH --output={pscratch_path}/streaming_logs/%x_%j.out
#SBATCH -N 1
#SBATCH --time={time_format}
#SBATCH --exclusive

date
echo "Creating log directory {pscratch_path}/streaming_logs"
mkdir -p {pscratch_path}/streaming_logs
mkdir -p {pscratch_path}/streaming_tomography_reconstructions

echo "Starting streaming container..."
podman-hpc run --rm \
    --volume {DOTENV_FILE}:/app/.env \
    --volume {DATA_DIR}:/mnt/outputs \
    --gpu \
    --shm-size=50G \
    samwelborn/als-tomocupy-stream-recon:latest
"""
        logger.info(job_script)

        try:
            logger.info("Submitting streaming job script to Perlmutter.")
            perlmutter = client.compute(Machine.perlmutter)
            job = perlmutter.submit_job(job_script)
            if job.jobid is None:
                raise ValueError("Did not get a jobid back...")
            logger.info(f"Streaming service job submitted with ID: {job.jobid}")
            return job.jobid

        except Exception as e:
            logger.error(f"Failed to submit streaming job: {e}")
            raise


@flow(name="nersc_streaming_flow")
def nersc_streaming_flow(
    client: SFAPI_Client | None = None,
    walltime: datetime.timedelta = datetime.timedelta(minutes=5),
    monitor_interval: int = 10,
) -> bool:
    logger = get_run_logger()
    logger.info(f"Starting NERSC streaming flow with {walltime} walltime")

    # Create a client if none was provided
    if client is None:
        logger.info("No client provided, creating one...")
        client = cfg.create_sfapi_client()

    job_id = NerscStreamingMixin().start_streaming_service(
        client=client, walltime=walltime
    )

    success = monitor_streaming_job.submit(
        client=client, job_id=job_id, update_interval=monitor_interval
    )

    logger.info(f"Monitoring job started with ID: {job_id}")

    return success.result()


if __name__ == "__main__":
    client = cfg.create_sfapi_client()
    nersc_streaming_flow(client=client)
