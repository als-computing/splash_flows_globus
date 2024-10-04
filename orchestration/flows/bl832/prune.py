import logging
from prefect import flow, get_run_logger
from prefect.blocks.system import JSON

from orchestration.flows.bl832.config import Config832
from orchestration.globus.transfer import GlobusEndpoint, prune_one_safe


logger = logging.getLogger(__name__)


def prune_files(
    relative_path: str,
    source_endpoint: GlobusEndpoint,
    check_endpoint: GlobusEndpoint = None,
):
    """
    Prune files from a source endpoint.

    Args:
        relative_path (str): The path of the file or directory to prune.
        source_endpoint (GlobusEndpoint): The Globus source endpoint to prune from.
        check_endpoint (GlobusEndpoint, optional): The Globus target endpoint to check. Defaults to None.
    """
    p_logger = get_run_logger()
    config = Config832()
    globus_settings = JSON.load("globus-settings").value
    max_wait_seconds = globus_settings["max_wait_seconds"]
    flow_name = f"prune_from_{source_endpoint.name}"
    p_logger.info(f"Running flow: {flow_name}")
    p_logger.info(f"Pruning {relative_path} from source endpoint: {source_endpoint.name}")
    prune_one_safe(
        file=relative_path,
        if_older_than_days=0,
        tranfer_client=config.tc,
        source_endpoint=source_endpoint,
        check_endpoint=check_endpoint,
        logger=p_logger,
        max_wait_seconds=max_wait_seconds
    )


@flow(name="prune_spot832")
def prune_spot832(relative_path: str):
    prune_files(relative_path=relative_path,
                source_endpoint=Config832().spot832,
                check_endpoint=Config832().data832)


@flow(name="prune_data832")
def prune_data832(relative_path: str):
    prune_files(relative_path=relative_path,
                source_endpoint=Config832().data832,
                check_endpoint=Config832().nersc832)


@flow(name="prune_data832_raw")
def prune_data832_raw(relative_path: str):
    prune_files(relative_path=relative_path,
                source_endpoint=Config832().data832_raw,
                check_endpoint=None)


@flow(name="prune_data832_scratch")
def prune_data832_scratch(relative_path: str):
    prune_files(relative_path=relative_path,
                source_endpoint=Config832().data832_scratch,
                check_endpoint=None)


@flow(name="prune_alcf832_raw")
def prune_alcf832_raw(relative_path: str):
    prune_files(relative_path=relative_path,
                source_endpoint=Config832().alcf832_raw,
                check_endpoint=Config832().data832_raw)


@flow(name="prune_alcf832_scratch")
def prune_alcf832_scratch(relative_path: str):
    prune_files(relative_path=relative_path,
                source_endpoint=Config832().alcf832_scratch,
                check_endpoint=Config832().data832_scratch)


@flow(name="prune_nersc832_alsdev_scratch")
def prune_nersc832_alsdev_scratch(relative_path: str):
    prune_files(relative_path=relative_path,
                source_endpoint=Config832().nersc832_alsdev_scratch,
                check_endpoint=None)


if __name__ == "__main__":
    prune_nersc832_alsdev_scratch("BLS-00564_dyparkinson/")
