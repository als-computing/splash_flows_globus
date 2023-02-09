import os
from pathlib import Path
import time


from freezegun import freeze_time
from pytest import MonkeyPatch


from globus_sdk import TransferData

from ..config import read_config
from ..globus import build_endpoints, GlobusEndpoint, is_globus_file_older, start_transfer


@freeze_time("2022-11-16 23:17:43+00:00")
def test_file_compare():
        old_file = {"last_modified": "2022-11-01 23:17:43+00:00"}
        new_file = {"last_modified": "2022-11-03 23:17:43+00:00"}

        assert is_globus_file_older(old_file, 14)
        assert not is_globus_file_older(new_file, 14)


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



def test_globus_config():
    config_file = Path(__file__).parent / "test_config.yml"
    with MonkeyPatch.context() as mp:
        mp.setenv("TEST_USERNAME", "TEST_USERNAME")
        mp.setenv("TEST_PASSWORD", "TEST_PASSWORD")
        globus_config = read_config(config_file=config_file)
        endpoints = build_endpoints(globus_config)
        test_endpoint = endpoints["test_endpoint"]

        assert test_endpoint.root_path == "/data"
        assert test_endpoint.uri == "test.example"
        assert test_endpoint.uuid == "12345"


def test_succeeded_transfer():
    transfer_client = MockTransferClient()
    source_endpoint = GlobusEndpoint(
        "123", "source.magrathea.com", "/root"
    )
    dest_endpoint = GlobusEndpoint(
        "456", "dest.magrathea.com", "/root"
    )

    result = start_transfer(
        transfer_client,
        source_endpoint,
        "/42/mice.jpg",
        dest_endpoint,
        "/42/mice.jpg"
        )

    assert result


def test_failed_transfer():
    transfer_client = FailedTransferClient()
    source_endpoint = GlobusEndpoint(
        "123", "source.magrathea.com", "/root"
    )
    dest_endpoint = GlobusEndpoint(
        "456", "dest.magrathea.com", "/root"
    )

    result = start_transfer(
        transfer_client,
        source_endpoint,
        "/42/mice.jpg",
        dest_endpoint,
        "/42/mice.jpg"
    )

    assert result
