from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from dateutil import parser
import dbm
import logging
import os
from pathlib import Path
from time import time
from typing import Dict, List


from dotenv import load_dotenv
from globus_sdk import (
    ClientCredentialsAuthorizer,
    ConfidentialAppAuthClient,
    DeleteData,
    TransferClient,
    TransferData,
)

from prefect import task, get_run_logger

from .config import get_config

load_dotenv()

GLOBUS_CLIENT_ID = os.getenv("GLOBUS_CLIENT_ID")
GLOBUS_CLIENT_SECRET = os.getenv("GLOBUS_CLIENT_SECRET")

logger = logging.getLogger("data_mover.globus")

globus_endpoints = {}

class TransferError(Exception):
    pass

@dataclass
class GlobusEndpoint:
    uuid: str
    uri: str
    root_path: str

    def full_path(self, path_suffix: str):
        # if path_suffix begins with "/", it will mess up the Path join
        if path_suffix[0] == "/":
            path_suffix = path_suffix[1:]
        path = Path(self.root_path) / path_suffix
        return str(path)


@dataclass
class GlobusApp:
    client_id: str
    client_secret: str


def activate_transfer_endpoint(tc: TransferClient, enddpoint_name: str):
    endpoint_config = get_config["globus"]["globus_endpoints"][enddpoint_name]
    tc.endpoint_autoactivate(endpoint_config["uuid"])


def build_endpoints(config: Dict) -> Dict[str, GlobusEndpoint]:
    all_endpoints = {}
    for endpoint_name, endpoint_config in config["globus"]["globus_endpoints"].items():
        all_endpoints[endpoint_name] = GlobusEndpoint(
            endpoint_config.get("uuid"),
            endpoint_config.get("uri"),
            endpoint_config.get("root_path"),
        )
    return all_endpoints


def build_apps(config: Dict) -> Dict[str, GlobusEndpoint]:
    apps = {}
    for app_name, app_config in config["globus"]["globus_apps"].items():
        apps[app_name] = GlobusApp(app_config["client_id"], app_config["client_secret"])
    return apps


def init_transfer_client(app: GlobusApp) -> TransferClient:
    confidential_client = ConfidentialAppAuthClient(
        client_id=app.client_id, client_secret=app.client_secret
    )

    scopes = "urn:globus:auth:scope:transfer.api.globus.org:all"
    cc_authorizer = ClientCredentialsAuthorizer(confidential_client, scopes)
    # create a new client
    return TransferClient(authorizer=cc_authorizer)


def start_transfer(
    transfer_client: TransferClient,
    source_endpoint: GlobusEndpoint,
    source_file: str,
    dest_endpoint: GlobusEndpoint,
    dest_path: str,
    wait_seconds=120,
    logger=logger
):
    source_path = Path(source_file)
    label = source_path.stem
    tdata = TransferData(
        transfer_client,
        source_endpoint.uuid,
        dest_endpoint.uuid,
        label=label,
        sync_level="checksum",
    )
    tdata.add_item(source_file, dest_path)
    logger.info(f"starting transfer {source_endpoint.uri}:{source_file} to {dest_endpoint.uri}:{dest_path}")

    task = transfer_client.submit_transfer(tdata)

    # if a transfer failed, like for a file not found globus keeps trying for a long time
    # and won't let another be attempted
    task_id = task["task_id"]
    return task_wait(transfer_client, task_id, wait_seconds=wait_seconds, logger=logger)


def is_globus_file_older(file_obj, older_than_days):
    last_modified = parser.parse(file_obj['last_modified'])
    comparison_time = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    return comparison_time > last_modified 


def get_files_recursive(tc: TransferClient, endpoint: GlobusEndpoint, path: str, files: List, older_than_days = 14):
    print(f"{endpoint.uri} {path}")
    contents = tc.operation_ls(endpoint.uuid, endpoint.full_path(path))
    for obj in contents:
        if obj['type'] == "file":
            obj['path'] = path
            if is_globus_file_older(obj, older_than_days):
                files.append(path + "/" + obj['name'])
        if obj['type'] == "dir":
            files = get_files_recursive(tc, endpoint, path + "/" + obj['name'], files)
    return files


@task
def get_files(tc: TransferClient, endpoint: GlobusEndpoint, path: str, files: List, older_than_days = 14):
    return get_files_recursive(tc, endpoint, path, files, older_than_days)


def get_globus_file_object(tc: TransferClient, endpoint: GlobusEndpoint, file: str):
    p_logger = get_run_logger()
    # get containing directory, we have to do an ls to find a file
    file_path = Path(file)
    p_logger.info(f"root path {endpoint.root_path}")
    globus_server_path = endpoint.full_path(str(file_path.parent))
    p_logger.info(f"globus_server_path  {globus_server_path}")

    files = tc.operation_ls(endpoint.uuid, globus_server_path)
    # logger.info(f"files {files}")
        # endpoint_obj = next(obj for obj in files if obj['name'] == file_path.name)
    for file_obj in files:
        if file_obj['name'] == file_path.name:
            return file_obj
    return None


@task
def prune_files(transfer_client: TransferClient, endpoint: GlobusEndpoint, files: List):
    p_logger = get_run_logger()
    ddata = DeleteData(transfer_client, endpoint.uuid)
    p_logger.info(f"deleting {len(files)} from endpoint: {endpoint.uri}")
    for file in files:
        file_path = endpoint.full_path(file)
        ddata.add_item(file_path)
    delete_result = transfer_client.submit_delete(ddata)
    task_id = delete_result['task_id']
    task_wait(transfer_client, task_id, p_logger)
    p_logger.info(f'delete_result {delete_result}')


def rename(transfer_client: TransferClient, endpoint: GlobusEndpoint, old_file: str, new_file: str):
    rename_result = transfer_client.operation_rename(endpoint.uuid, old_file, new_file)
    return task_wait(transfer_client, rename_result['task_id'])


def task_wait(transfer_client: TransferClient, task_id: str, wait_seconds=120, logger=logger):
    start = time()
    while not transfer_client.task_wait(task_id, polling_interval=5, timeout=5):
        elapsed = time() - start
        if elapsed > wait_seconds:
            logger.info(f"done waiting for completion of task ")
            raise TransferError(f"Configred to wait {wait_seconds}, elapsed is {elapsed}. Last globus transfer nice_status {task['nice_status']}")
        task = transfer_client.get_task(task_id)
        logger.info(
            f"waiting for task with task_id {task_id} to complete {task['nice_status']}"
        )
        
        if task["status"] == "SUCCEEDED":
            logger.info("COMPLETE")
        elif task["status"] == "FAILED":
            logger.info(f"globus task failed {task_id}")
        
        if task['nice_status'] in ['FILE_NOT_FOUND']:
            transfer_client.cancel_task(task_id)
            raise TransferError(f"Received FILE_NOT_FOUND, cancelling task")
    return True

@task
def prune_one_safe(
        file: str,
        if_older_than_days: int,
        tranfer_client: TransferClient,
        source_endpoint: GlobusEndpoint,
        check_endpoint: GlobusEndpoint,
        logger=logger):
    """
        Prunes a single file or directory. Safety means
        this performs a check to make sure that the asset on the `source_endpoint`
        is also located at the check_endpoint. If not, raises
    """
    # does the file exist at the source endpoint?
    g_file_obj = get_globus_file_object(tranfer_client, source_endpoint, file)
    assert g_file_obj is not None, f"file not found {source_endpoint.uri}"
    logger.info(f"file: {file} found on {source_endpoint.uri}")

    # does the file exist at the check endpoint?
    g_file_obj = get_globus_file_object(tranfer_client, check_endpoint, file)
    assert g_file_obj is not None, f"file not found {check_endpoint.uri}"
    logger.info(f"file: {file} found on {check_endpoint.uri}")

    # is the file older than the days asked for?
    assert is_globus_file_older(g_file_obj, if_older_than_days), (
        f"Will not prune, file date {g_file_obj['last_modified']} is "
        f"newer than {if_older_than_days} days"
    )
    logger.info(f"{file}")
    prune_files(tranfer_client, source_endpoint, [file])


if __name__ == "__main__":
    config = get_config()
    endpoints = build_endpoints(config)
    apps = build_apps(config)
    tc = init_transfer_client(apps["als_transfer"])
    source_ep = endpoints["spot832"]
    dest_ep = endpoints["data832"]
    task_id = start_transfer(
        tc,
        source_ep,
        "/raw/dmcreynolds/test2.txt",
        dest_ep,
        "/data/raw/dmcreynolds/test2.txt",
    )
    while not tc.task_wait(task_id, timeout=5):
        print("Another second went by without {0} terminating".format(task_id))
