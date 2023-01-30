from dataclasses import dataclass
import logging
import os
from pathlib import Path
from typing import Dict

from dotenv import load_dotenv
from globus_sdk import (
    ClientCredentialsAuthorizer,
    ConfidentialAppAuthClient,
    TransferClient,
    TransferData,
)

from .config import read_config

load_dotenv()

GLOBUS_CLIENT_ID = os.getenv("GLOBUS_CLIENT_ID")
GLOBUS_CLIENT_SECRET = os.getenv("GLOBUS_CLIENT_SECRET")

logger = logging.getLogger("data_mover.globus")

globus_endpoints = {}


@dataclass
class GlobusEndpoint:
    uuid: str
    uri: str
    root_path: str
    username: str
    password: str


@dataclass
class GlobusApp:
    client_id: str
    client_secret: str


def activate_transfer_endpoint(tc: TransferClient, enddpoint_name: str):
    endpoint_config = get_config["globus_endpoints"][enddpoint_name]
    tc.endpoint_autoactivate(endpoint_config["uuid"])


def build_endpoints(config: Dict) -> Dict[str, GlobusEndpoint]:
    all_endpoints = {}
    for endpoint_name, endpoint_config in config["globus_endpoints"].items():
        all_endpoints[endpoint_name] = GlobusEndpoint(
            endpoint_config.get("uuid"),
            endpoint_config.get("uri"),
            endpoint_config.get("root_path"),
            endpoint_config.get("username"),
            endpoint_config.get("password"),
        )
    return all_endpoints


def build_apps(config: Dict) -> Dict[str, GlobusEndpoint]:
    apps = {}
    for app_name, app_config in config["globus_apps"].items():
        apps[app_name] = GlobusApp(app_config["client_id"], app_config["client_secret"])
    return apps


def get_config():
    return read_config(config_file=Path(__file__).parent / "config.yml")


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
    job_observer=None,
    label="",
    wait_for_task=True,
):

    tdata = TransferData(
        transfer_client,
        source_endpoint.uuid,
        dest_endpoint.uuid,
        label=label,
        sync_level="checksum",
    )
    tdata.add_item(source_file, dest_path)
    task = transfer_client.submit_transfer(tdata)
    task_id = task["task_id"]
    if wait_for_task:
        while not transfer_client.task_wait(task_id, polling_interval=2, timeout=600):
            logger.debug(
                f"waiting for {source_file} with task_id {task_id} to complete data832 to nersc"
            )
        task = transfer_client.get_task(task_id)
        if task["status"] == "SUCCEEDED" and job_observer:
            job_observer.complete()
        elif task["status"] == "FAILED" and job_observer:
            job_observer.error(f"globus transfer failed {task_id}")
        # @TODO fiture out how to handle ACTIVE and INACTIVE
    return task


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
