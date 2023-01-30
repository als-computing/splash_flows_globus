import os
from pathlib import Path
import time

from pytest import MonkeyPatch


from globus_sdk import TransferData


from ..data_mover_ng import JobObserver, JobComm
from ..config import read_config
from ..globus import build_endpoints, GlobusEndpoint, start_transfer


class MockTransferClient:
    transfer_data: TransferData

    def submit_transfer(self, transfer_data: TransferData):
        self.transfer_data = transfer_data
        return {"task_id": "12345"}

    def get_submission_id(self):
        return {"value": "42"}

    def task_wait(self, task_id, polling_interval=1, timeout=1):
        time.sleep(polling_interval)
        return True

    def get_task(self, task_id):
        return {"task_id": task_id, "status": "SUCCEEDED"}


class FailedTransferClient(MockTransferClient):
    def get_task(self, task_id):
        return {"task_id": task_id, "status": "FAILED"}


class MockJobComm(JobComm):

    last_state = {}

    def get_new_jobs(self):
        pass

    def update_state(self, job_id, state):
        self.last_state = {"job_id": job_id, "state": state}


def test_globus_config():
    config_file = Path(__file__).parent / "test_config.yml"
    with MonkeyPatch.context() as mp:
        mp.setenv("TEST_USERNAME", "TEST_USERNAME")
        mp.setenv("TEST_PASSWORD", "TEST_PASSWORD")
        globus_config = read_config(config_file=config_file)
        endpoints = build_endpoints(globus_config)
        test_endpoint = endpoints["test_endpoint"]

        assert test_endpoint.root_path == "/data"
        assert test_endpoint.username == "TEST_USERNAME"
        assert test_endpoint.password == "TEST_PASSWORD"
        assert test_endpoint.uri == "test.example"
        assert test_endpoint.uuid == "12345"


def test_succeeded_transfer():
    transfer_client = MockTransferClient()
    source_endpoint = GlobusEndpoint(
        "123", "source.magrathea.com", "/root", "slartibartfast", "deepthought"
    )
    dest_endpoint = GlobusEndpoint(
        "456", "dest.magrathea.com", "/root", "slartibartfast", "deepthought"
    )
    job_comm = MockJobComm("url", "api_key")
    job_observer = JobObserver({"id": "job_id"}, job_comm)
    task = start_transfer(
        transfer_client,
        source_endpoint,
        "/42/mice.jpg",
        dest_endpoint,
        "/42/mice.jpg",
        job_observer=job_observer,
        label="",
        wait_for_task=True,
    )

    assert task["task_id"] == "12345"

    # did the jobcomm/jobobserver chain work?
    assert job_comm.last_state["state"] == "complete"


def test_failed_transfer():
    transfer_client = FailedTransferClient()
    source_endpoint = GlobusEndpoint(
        "123", "source.magrathea.com", "/root", "slartibartfast", "deepthought"
    )
    dest_endpoint = GlobusEndpoint(
        "456", "dest.magrathea.com", "/root", "slartibartfast", "deepthought"
    )
    job_comm = MockJobComm("url", "api_key")
    job_observer = JobObserver({"id": "job_id"}, job_comm)
    task = start_transfer(
        transfer_client,
        source_endpoint,
        "/42/mice.jpg",
        dest_endpoint,
        "/42/mice.jpg",
        job_observer=job_observer,
        label="",
        wait_for_task=True,
    )

    assert task["task_id"] == "12345"

    # did the jobcomm/jobobserver chain work?
    assert job_comm.last_state["state"] == "error"
