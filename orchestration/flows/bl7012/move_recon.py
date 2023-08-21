# import datetime
import os
from pathlib import Path
import uuid

from globus_sdk import TransferClient
from prefect import flow, task, get_run_logger
from orchestration.flows.bl7012.config import Config7012
from orchestration.globus import GlobusEndpoint, start_transfer

from .ptycho_jobscript import nserc_cdtools, nersc_ptychocam


API_KEY = os.getenv("API_KEY")
PATH_CLIENT_ID = os.getenv("PATH_NERSC_ID")
PATH_PRIV_KEY = os.getenv("PATH_NERSC_PRI_KEY")
PATH_JOB_SCRIPT = os.getenv("PATH_JOB_SCRIPT")
PATH_PTYCHOCAM_NERSC = os.getenv("PATH_PTYCHOCAM_NERSC")
PATH_CDTOOLS_NERSC = os.getenv("PATH_CDTOOLS_NERSC")


@task(name="transfer_to_nersc")
def transfer_data_to_nersc(
    file_path: str,
    transfer_client: TransferClient,
    source_endpoint: GlobusEndpoint,
    destination_endpoint: GlobusEndpoint,
):
    logger = get_run_logger()

    # Logging the original source paths
    logger.info(f"Requested relative source path: {file_path}")

    # if source_file begins with "/", it will mess up os.path.join
    file_path = file_path[1:] if file_path[0] == "/" else file_path
    source_path = os.path.join(source_endpoint.root_path, file_path)
    dest_path = os.path.join(destination_endpoint.root_path, file_path)

    # Logging the full source and destination paths
    logger.info(f"Full source path at {source_endpoint.name}: {source_path}")
    logger.info(f"Full destination path at {destination_endpoint.name}: {dest_path}")

    # Start globus transfer
    logger.info(
        f"Transferring {source_path} {source_endpoint.name} to {destination_endpoint.name}"
    )

    success = start_transfer(
        transfer_client,
        source_endpoint,
        source_path,
        destination_endpoint,
        dest_path,
        max_wait_seconds=600,
        logger=logger,
    )

    return success


@task(name="transfer_data_within_single_endpoint")
def transfer_data_within_single_endpoint(
    source_path: str,
    dest_path: str,
    transfer_client: TransferClient,
    globus_endpoint: GlobusEndpoint,
):
    logger = get_run_logger()

    # Logging the original source and destination paths
    logger.info(f"Requested relative source path: {source_path}")
    logger.info(f"Requested relative destination path: {dest_path}")

    # Remove the leading "/" in paths in present
    source_path = source_path[1:] if source_path[0] == "/" else source_path
    dest_path = dest_path[1:] if dest_path[0] == "/" else dest_path
    source_path = os.path.join(globus_endpoint.root_path, source_path)
    dest_path = os.path.join(globus_endpoint.root_path, dest_path)

    # Logging the full source and destination paths
    logger.info(f"Full source path at: {source_path}")
    logger.info(f"Full destination path at: {dest_path}")

    # Start globus transfer
    logger.info(f"Transferring {source_path} to {dest_path} at {globus_endpoint.name}")
    success = start_transfer(
        transfer_client,
        globus_endpoint,
        source_path,
        globus_endpoint,
        dest_path,
        max_wait_seconds=600,
        logger=logger,
    )

    return success


@task(name="cdtools_recon_nersc")
def cdtools_recon_nersc(
    file_path,
    config,
    path_job_script,
    path_cdtools_nersc,
    n_gpu,
    **kwargs,
):
    logger = get_run_logger()
    logger.info("Performing cdtools ptycho reconstruction at nersc")
    config.nersc.logger = logger
    success = nserc_cdtools(
        config.nersc,
        os.path.basename(file_path),
        path_job_script,
        path_cdtools_nersc,
        n_gpu,
        **kwargs,
    )
    return success


@task(name="ptychocam_recon_nersc")
def ptychocam_recon_nersc(
    file_path,
    config,
    path_job_script,
    path_ptychocam_nersc,
    n_gpu,
    **kwargs,
):
    logger = get_run_logger()
    logger.info("Performing ptychocam ptycho reconstruction at nersc")
    config.nersc.logger = logger
    success = nersc_ptychocam(
        os.path.basename(file_path),
        path_job_script,
        path_ptychocam_nersc,
        n_gpu,
        **kwargs,
    )
    # success = nersc.ptychocam(
    #     os.path.basename(file_path), n_gpu, logger=logger, **kwargs
    # )
    return success


@flow(name="test_cosmicDTN_transfers")
def test_transfers_7012(file_path: str = "datamovement_test/test.txt"):
    logger = get_run_logger()
    config = Config7012()
    logger.info(f"{str(uuid.uuid4())}{file_path}")

    # copy file to a uniquely-named file in the same folder
    file = Path(file_path)
    new_file = str(file.with_name(f"test_{str(uuid.uuid4())}.txt"))
    logger.info(new_file)
    print(new_file)

    task = transfer_data_within_single_endpoint(
        file_path, new_file, config.tc, config.nersc7012
    )
    logger.info(
        f"File successfully transferred from {file_path} to {new_file}. Task {task}"
    )
    return


@flow(name="process_newfile_7012_ptycho4")
def process_new_file_ptycho4(file_path: str):
    logger = get_run_logger()
    logger.info("Starting flow")
    config = Config7012()

    # Transferring data from cosmicDTN to NERSC
    task = transfer_data_to_nersc(
        file_path, config.tc, config.data7012, config.nersc7012
    )
    logger.info(f"File successfully transferred from cosmicDTN to NERSC. Task {task}")

    # Next task is: start ptycho reconstruction ...

    return task


@flow(name="transfer_auto_recon")
def transfer_auto_recon(
    file_path: str,
    do_cdtools=False,
    run_split_reconstructions=False,
    n_modes=1,
    oversampling_factor=2,
    propagation_distance=50 * 1e-6,
    simulate_probe_translation=True,
    n_init_rounds=1,
    n_init_iter=50,
    n_final_iter=50,
    translation_randomization=0,
    probe_initialization=None,
    init_background=False,
    probe_support_radius=None,
    do_ptychocam=False,
    n_iter=500,
    period_illu_refine=0,
    period_bg_refine=0,
    use_illu_mask=False,
    n_gpu=1,
):
    logger = get_run_logger()
    logger.info("Starting flow")
    config = Config7012(
        PATH_CLIENT_ID,
        PATH_PRIV_KEY,
    )

    # Transferring data from cosmicDTN to NERSC
    task = transfer_data_to_nersc(
        file_path, config.tc, config.data7012, config.nersc7012
    )
    logger.info(f"File successfully transferred from cosmicDTN to NERSC. Task {task}")

    # Next task is: start ptycho reconstruction ...
    if do_cdtools:
        task = cdtools_recon_nersc(
            file_path,
            config,
            PATH_JOB_SCRIPT,
            PATH_CDTOOLS_NERSC,
            n_gpu,
            run_split_reconstructions=run_split_reconstructions,
            n_modes=n_modes,
            oversampling_factor=oversampling_factor,
            propagation_distance=propagation_distance,
            simulate_probe_translation=simulate_probe_translation,
            n_init_rounds=n_init_rounds,
            n_init_iter=n_init_iter,
            n_final_iter=n_final_iter,
            translation_randomization=translation_randomization,
            probe_initialization=probe_initialization,
            init_background=init_background,
            probe_support_radius=probe_support_radius,
        )
        logger.info(f"Reconstruction returns with status {task}")

    if do_ptychocam:
        task = ptychocam_recon_nersc(
            file_path,
            config,
            PATH_JOB_SCRIPT,
            PATH_PTYCHOCAM_NERSC,
            n_gpu,
            n_iter=n_iter,
            period_illu_refine=period_illu_refine,
            period_bg_refine=period_bg_refine,
            use_illu_mask=use_illu_mask,
        )
        logger.info(f"Reconstruction returns with status {task}")

    # logger.info(f"Submitted task id: {task[0]}")
    # logger.info(f"Task status: {task[1]}")

    return task


if __name__ == "__main__":
    import os
    import dotenv

    # print(os.getenv("GLOBUS_CLIENT_ID"))
    dotenv.load_dotenv()

    # testing nersc-api all for ptychocam
    cxi_filename = "2023/02/230216/NS_230216033_ccdframes_0_0.cxi"
    transfer_auto_recon(
        cxi_filename,
        do_cdtools=True,
        do_ptychocam=False,
        n_gpu=1,
    )

    # test_transfers_7012()
    # process_new_file2(sys.argv[1])
