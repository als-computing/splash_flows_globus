import datetime
import os
from pathlib import Path
import uuid

from globus_sdk import TransferClient
from prefect import flow, task, get_run_logger
from prefect.blocks.system import JSON
from prefect.blocks.system import Secret

from orchestration.flows.bl832.move import transfer_spot_to_data, transfer_data_to_nersc
from orchestration.nersc import NerscClient
import time

@task(name="create_nersc_client")
def create_nersc_client():
    client_id_path = os.getenv("PATH_NERSC_CLIENT_ID") #.txt file
    sfapi_key_path = os.getenv("PATH_NERSC_PRI_KEY") #.jwk file

    client = NerscClient(client_id_path, sfapi_key_path)
    user = client.user()

    #error handling

    return client

@task(name="submit_job_script")
def submit_job_script(client, user, logger):
    home_path = f"/global/homes/{user.name[0]}/{user.name}"
    scratch_path = f"/pscratch/sd/{user.name[0]}/{user.name}"

    client.perlmutter.run(f"mkdir -p {scratch_path}/prefect-recon-test")
    job_script = f"""#!/bin/bash
#SBATCH -q debug
#SBATCH -A als
#SBATCH -C cpu
#SBATCH --job-name=tomorecon_nersc_mpi_hdf5_1-0     
#SBATCH --output={scratch_path}/nerscClient-test/%x_%j.out          
#SBATCH --error={scratch_path}/nerscClient-test/%x_%j.err 
#SBATCH -N 1
#SBATCH --ntasks-per-node 1
#SBATCH --cpus-per-task 1
#SBATCH --time=00:15:00
#SBATCH --exclusive

date
srun podman-hpc run --volume {home_path}/tomo_recon_repo/microct/legacy/reconstruction.py:/alsuser/reconstruction.py --volume {home_path}/tomo_recon_repo/microct/legacy/input.txt:/alsuser/input.txt --volume {scratch_path}/microctdata:/alsdata localhost/tomorecon_nersc_mpi_hdf5:1.0 python reconstruction.py input.txt
date                        
"""
    job = client.perlmutter.submit_job(job_script)
    job.complete() #waits for job completion
    logger.info("Job completed")
    #logger.info(f"Job {job.id} completed")

    return

@flow(name="launch-nersc-jobs-tomo_recon")
def launch_nersc_jobs_tomo_recon(
):
    logger = get_run_logger()

    # # Data transfer step
    # config = Config832()
    # # test_scicat(config)
    # logger.info(f"{str(uuid.uuid4())}{file_path}")
    # # copy file to a uniquely-named file in the same folder
    # file = Path(file_path)
    # new_file = str(file.with_name(f"test_{str(uuid.uuid4())}.txt"))
    # logger.info(new_file)
    # success = start_transfer(
    #     config.tc, config.spot832, file_path, config.spot832, new_file, logger=logger
    # )
    # logger.info(success)
    # spot832_path = transfer_spot_to_data(
    #     new_file, config.tc, config.spot832, config.data832
    # )
    # logger.info(f"Transferred {spot832_path} to spot to data")

    # task = transfer_data_to_nersc(new_file, config.tc, config.data832, config.nersc832)
    # logger.info(
    #     f"File successfully transferred from data832 to NERSC {spot832_path}. Task {task}"
    # )
    

    # Creating a sfapi client object
    client = create_nersc_client()
    user = client.user()

    logger.info("Client created")

    #Job submission step
    submit_job_script(client, user, logger)

    

    return
