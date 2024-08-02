import datetime
from freezegun import freeze_time
from globus_sdk import TransferData
from orchestration.config import read_config
from orchestration.flows.bl832.alcf import (
    transfer_data_to_alcf,
    transfer_data_to_nersc,
    transfer_data_to_data832,
    schedule_prune_task,
    schedule_pruning,
    alcf_tomopy_reconstruction_flow,
    process_new_832_ALCF_flow
)
from orchestration.globus.transfer import GlobusEndpoint, start_transfer, build_endpoints
from pathlib import Path
from pytest import MonkeyPatch
import time


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
    source_endpoint = GlobusEndpoint("123", "source.magrathea.com", "/root")
    dest_endpoint = GlobusEndpoint("456", "dest.magrathea.com", "/root")

    result = start_transfer(
        transfer_client, source_endpoint, "/42/mice.jpg", dest_endpoint, "/42/mice.jpg"
    )

    assert result


def test_failed_transfer():
    transfer_client = FailedTransferClient()
    source_endpoint = GlobusEndpoint("123", "source.magrathea.com", "/root")
    dest_endpoint = GlobusEndpoint("456", "dest.magrathea.com", "/root")

    result = start_transfer(
        transfer_client, source_endpoint, "/42/mice.jpg", dest_endpoint, "/42/mice.jpg"
    )

    assert result


def test_transfer_data_to_alcf():
    transfer_client = MockTransferClient()
    source_endpoint = GlobusEndpoint("123", "source.magrathea.com", "/root")
    dest_endpoint = GlobusEndpoint("456", "dest.magrathea.com", "/root")
    file_path = "/path/to/file.txt"
    result = transfer_data_to_alcf(file_path, transfer_client, source_endpoint, dest_endpoint)
    assert result


def test_transfer_data_to_nersc():
    transfer_client = MockTransferClient()
    source_endpoint = GlobusEndpoint("123", "source.magrathea.com", "/root")
    dest_endpoint = GlobusEndpoint("456", "dest.magrathea.com", "/root")
    file_path = "/path/to/file.txt"
    result = transfer_data_to_nersc(file_path, transfer_client, source_endpoint, dest_endpoint)
    assert result


def test_transfer_data_to_data832():
    transfer_client = MockTransferClient()
    source_endpoint = GlobusEndpoint("123", "source.magrathea.com", "/root")
    dest_endpoint = GlobusEndpoint("456", "dest.magrathea.com", "/root")
    file_path = "/path/to/file.txt"
    result = transfer_data_to_data832(file_path, transfer_client, source_endpoint, dest_endpoint)
    assert result


def test_schedule_prune_task():
    path = "/path/to/prune"
    location = "test_location"
    schedule_days = datetime.timedelta(days=0.01)
    result = schedule_prune_task(path, location, schedule_days)
    assert not result


def test_schedule_pruning():
    with freeze_time("2024-01-01"):
        schedule_days = datetime.timedelta(days=0.01)
        alcf_raw_path = "/path/to/alcf/raw"
        alcf_scratch_path_tiff = "/path/to/alcf/scratch/tiff"
        alcf_scratch_path_zarr = "/path/to/alcf/scratch/zarr"
        nersc_scratch_path_tiff = "/path/to/nersc/scratch/tiff"
        nersc_scratch_path_zarr = "/path/to/nersc/scratch/zarr"
        data832_scratch_path = "/path/to/data832/scratch"
        one_minute = True

        result = schedule_pruning(
            alcf_raw_path=alcf_raw_path,
            alcf_scratch_path_tiff=alcf_scratch_path_tiff,
            alcf_scratch_path_zarr=alcf_scratch_path_zarr,
            nersc_scratch_path_tiff=nersc_scratch_path_tiff,
            nersc_scratch_path_zarr=nersc_scratch_path_zarr,
            data832_scratch_path=data832_scratch_path,
            one_minute=one_minute
        )
        assert result


def test_alcf_tomopy_reconstruction_flow():
    raw_path = "/path/to/raw"
    scratch_path = "/path/to/scratch"
    folder_name = "test_folder"
    file_name = "test_file"
    result = alcf_tomopy_reconstruction_flow(raw_path, scratch_path, folder_name, file_name)
    assert not result


def test_process_new_832_ALCF_flow():
    folder_name = "test_folder"
    file_name = "test_file"
    is_export_control = False
    send_to_alcf = True
    result = process_new_832_ALCF_flow(folder_name, file_name, is_export_control, send_to_alcf)
    assert isinstance(result, list)        