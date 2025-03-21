import datetime
import json
import pathlib
import time
from typing import cast

from prefect import Flow, flow, get_run_logger
from prefect.logging.loggers import flow_run_logger
from prefect.client.schemas.objects import FlowRun, State
from prefect.blocks.system import JSON
from prefect.runtime import flow_run
from sfapi_client import Client as SFAPI_Client
from sfapi_client.compute import Machine
from sfapi_client.jobs import JobState, TERMINAL_STATES, JobSacct
from sfapi_client.exceptions import SfApiError
from pathlib import Path

from pydantic import BaseModel, model_validator
from authlib.jose import JsonWebKey

from pydantic_settings import BaseSettings, SettingsConfigDict
from sfapi_client import Client


class SlurmJobBlock(BaseModel):
    job_id: str | None = None
    job_state: JobState | None = None
    timelimit: str | None = None
    elapsed: str | None = None


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


def monitor_streaming_job(
    client: SFAPI_Client, job_id: str, update_interval: int
) -> bool:
    logger = get_run_logger()
    logger.info(f"Monitoring streaming job {job_id}...")
    perlmutter = client.compute(Machine.perlmutter)
    retries = 0
    max_retries = 5
    slurm_job_block = SlurmJobBlock(job_id=job_id)

    while True:
        try:
            job: JobSacct = cast(JobSacct, perlmutter.job(jobid=job_id))
            job.update()
            status = job.state
            slurm_job_block.job_state = status
            slurm_job_block.elapsed = job.elapsed
            slurm_job_block.timelimit = job.timelimit
            save_block(slurm_job_block)

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
    --gpu \
    --network=host \
    --volume {DOTENV_FILE}:/app/.env \
    --volume {DATA_DIR}:/mnt/outputs \
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
            logger.exception(f"Failed to submit streaming job: {e}")
            raise


def cancellation_hook(flow: Flow, flow_run: FlowRun, state: State):
    logger = flow_run_logger(flow_run, flow)
    block_name = f"{flow_run.id}-metadata"
    try:
        meta = JSON.load(name=block_name)
    except ValueError:
        logger.exception("No metadata found for this flow run")
        return
    job_id = meta.value.get("job_id")
    if not job_id:
        logger.info("No job ID found in metadata")
        return

    logger.info(f"Attempting to cancel NERSC job {job_id}")
    try:
        cfg.create_sfapi_client().compute(Machine.perlmutter).job(jobid=job_id).cancel()
    except (Exception, SfApiError) as e:
        logger.error(f"Failed to cancel job: {e}")

    logger.info(f"Successfully requested cancellation for job {job_id}")

    # cleanup block
    meta.delete(name=block_name)


def save_block(slurm_block: SlurmJobBlock) -> JSON:
    block = JSON(value=slurm_block.model_dump())
    block.save(name=f"{flow_run.get_id()}-metadata", overwrite=True)
    return block


FLOW_NAME = "nersc_streaming_flow"


@flow(name=FLOW_NAME, on_cancellation=[cancellation_hook], log_prints=True)
def nersc_streaming_flow(
    walltime: datetime.timedelta = datetime.timedelta(minutes=5),
    monitor_interval: int = 10,
) -> bool:
    logger = get_run_logger()
    logger.info(f"Starting NERSC streaming flow with {walltime} walltime")

    try:
        client = cfg.create_sfapi_client()
    except RuntimeError as e:
        logger.error(f"Failed to create NERSC client: {e}")

    job_id = NerscStreamingMixin().start_streaming_service(
        client=client, walltime=walltime
    )

    logger.info("Saving job ID to metadata block...")

    save_block(SlurmJobBlock(job_id=job_id))

    success = monitor_streaming_job(
        client=client,
        job_id=job_id,
        update_interval=monitor_interval,
    )

    return success


if __name__ == "__main__":
    nersc_streaming_flow()
