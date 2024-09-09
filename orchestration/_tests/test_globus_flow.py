from typing import List, Optional, Dict, Any
from uuid import UUID, uuid4
import warnings

from globus_compute_sdk.sdk.client import Client

from prefect.testing.utilities import prefect_test_harness
from prefect.blocks.system import JSON, Secret
from pydantic import BaseModel, PydanticDeprecatedSince20
import pytest
from pytest_mock import MockFixture

from .test_globus import MockTransferClient

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=PydanticDeprecatedSince20)


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    """
    A pytest fixture that automatically sets up and tears down the Prefect test harness
    for the entire test session. It creates and saves test secrets and configurations
    required for Globus integration.

    Yields:
        None
    """
    with prefect_test_harness():
        globus_client_id = Secret(value=str(uuid4()))
        globus_client_id.save(name="globus-client-id")

        globus_client_secret = Secret(value=str(uuid4()))
        globus_client_secret.save(name="globus-client-secret")

        globus_compute_endpoint = Secret(value=str(uuid4()))
        globus_compute_endpoint.save(name="globus-compute-endpoint")

        globus_reconstruction_function = Secret(value=str(uuid4()))
        globus_reconstruction_function.save(name="globus-reconstruction-function")

        globus_iribeta_cgs_endpoint = Secret(value=str(uuid4()))
        globus_iribeta_cgs_endpoint.save(name="globus-iribeta-cgs-endpoint")

        globus_flow_id = Secret(value=str(uuid4()))
        globus_flow_id.save(name="globus-flow-id")

        pruning_config = JSON(value={"max_wait_seconds": 600})
        pruning_config.save(name="pruning-config")
        yield


class FlowDefinition(BaseModel):
    """Model for flow definition"""
    StartAt: str
    States: Dict[str, Dict[str, Any]]


class FlowInputSchema(BaseModel):
    """Model for flow input schema"""
    type: str
    properties: Dict[str, Dict[str, Any]]


class MockEndpoint:
    def __init__(self, root_path, uuid_value=None):
        self.root_path = root_path
        self.uuid = uuid_value or str(uuid4())
        self.uri = f"mock_endpoint_uri_{self.uuid}"


class MockConfig832():
    def __init__(self) -> None:
        # Mock configuration
        config = {
            "scicat": "mock_scicat_value"
        }

        # Mock endpoints with UUIDs
        self.endpoints = {
            "spot832": MockEndpoint(root_path="mock_spot832_path", uuid_value=str(uuid4())),
            "data832": MockEndpoint(root_path="mock_data832_path", uuid_value=str(uuid4())),
            "data832_raw": MockEndpoint(root_path="mock_data832_raw_path", uuid_value=str(uuid4())),
            "data832_scratch": MockEndpoint(root_path="mock_data832_scratch_path", uuid_value=str(uuid4())),
            "nersc832": MockEndpoint(root_path="mock_nersc832_path", uuid_value=str(uuid4())),
            "nersc_test": MockEndpoint(root_path="mock_nersc_test_path", uuid_value=str(uuid4())),
            "nersc_alsdev": MockEndpoint(root_path="mock_nersc_alsdev_path", uuid_value=str(uuid4())),
            "nersc832_alsdev_raw": MockEndpoint(root_path="mock_nersc832_alsdev_raw_path",
                                                uuid_value=str(uuid4())),
            "nersc832_alsdev_scratch": MockEndpoint(root_path="mock_nersc832_alsdev_scratch_path",
                                                    uuid_value=str(uuid4())),
            "alcf832_raw": MockEndpoint(root_path="mock_alcf832_raw_path", uuid_value=str(uuid4())),
            "alcf832_scratch": MockEndpoint(root_path="mock_alcf832_scratch_path", uuid_value=str(uuid4())),
        }

        # Mock apps
        self.apps = {
            "als_transfer": "mock_als_transfer_app"
        }

        # Use the MockTransferClient instead of the real TransferClient
        self.tc = MockTransferClient()

        # Use the MockSpecificFlowClient instead of the real FlowsClient
        self.flow_client = MockSpecificFlowClient(UUID("123e4567-e89b-12d3-a456-426614174000"))

        # Set attributes directly on the object
        self.nersc_test = self.endpoints["nersc_test"]
        self.alcf832_raw = self.endpoints["alcf832_raw"]
        self.alcf832_scratch = self.endpoints["alcf832_scratch"]
        self.nersc832_alsdev_scratch = self.endpoints["nersc832_alsdev_scratch"]
        self.data832 = self.endpoints["data832"]
        self.data832_raw = self.endpoints["data832_raw"]
        self.data832_scratch = self.endpoints["data832_scratch"]
        self.scicat = config["scicat"]


# Mock the Client class to avoid real network calls
class MockGlobusComputeClient(Client):
    def __init__(self, *args, **kwargs):
        # Skip initializing the real client
        pass

    def version_check(self):
        # Mock version check to do nothing
        pass

    def run(self, *args, **kwargs):
        # Mock run to return a fake task ID
        return "mock_task_id"

    def get_task(self, task_id):
        # Mock getting task to return a successful result
        return {
            "pending": False,
            "status": "success",
            "result": "mock_result"
        }

    def get_result(self, task_id):
        # Mock getting the result of a task
        return "mock_result"


# Define mock classes for FlowsClient and SpecificFlowClient
class MockFlowsClient:
    """Mock class for FlowsClient"""
    def __init__(self):
        self.flows = {}

    def create_flow(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Mock method for initializing a new flow with Globus Flows"""
        flow_id = UUID("123e4567-e89b-12d3-a456-426614174000")
        request = {
            "title": str,
            "definition": FlowDefinition,
            "input_schema": FlowInputSchema,
            "subtitle": Optional[str],
            "description": Optional[str],
            "flow_viewers": Optional[List[str]],
            "flow_starters": Optional[List[str]],
            "flow_administrators": Optional[List[str]],
            "keywords": Optional[List[str]],
            "subscription_id": Optional[UUID],
            "additional_fields": Optional[Dict[str, Any]],
            "model_config": {"arbitrary_types_allowed": True}
        }
        self.flows[flow_id] = request
        return {"flow_id": str(flow_id)}

    def get_flow(self, flow_id: UUID) -> Dict[str, Any]:
        """Mock method for getting a flow"""
        return self.flows.get(flow_id, {})


class MockSpecificFlowClient:
    """Mock class for SpecificFlowClient"""
    def __init__(self, flow_id: UUID):
        self.flow_id = flow_id
        self.runs = {}

    def run_flow(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Mock method for running a registered flow function"""
        run_id = UUID("123e4567-e89b-12d3-a456-426614174001")
        request = {
            "body": Dict[str, Any],
            "label": Optional[str],
            "tags": Optional[List[str]],
            "run_monitors": Optional[List[str]],
            "run_managers": Optional[List[str]],
            "additional_fields": Optional[Dict[str, Any]],
            "model_config": {"arbitrary_types_allowed": True}
        }
        self.runs[run_id] = request.model_dump()
        return {"run_id": str(run_id), "status": "SUCCEEDED"}

    def get_run(self, run_id: UUID) -> Dict[str, Any]:
        """Mock method for getting a run"""
        return self.runs.get(run_id, {})


def test_process_new_832_ALCF_flow(mocker: MockFixture):
    mock_secret = mocker.MagicMock()
    mock_secret.value = str(uuid4())
    # https://pytest-mock.readthedocs.io/en/latest/usage.html#usage-as-context-manager
    with mocker.patch('prefect.blocks.system.Secret.load', return_value=mock_secret):
        from orchestration.flows.bl832.alcf import process_new_832_ALCF_flow

    """Test for the process of a new 832 ALCF flow"""
    folder_name = "test_folder"
    file_name = "test_file"

    # Mock the Config832 class inserting into the module being tested
    mock_config = MockConfig832()

    mock_transfer_to_alcf = mocker.patch('orchestration.flows.bl832.alcf.transfer_data_to_alcf',
                                         return_value=True)
    mock_reconstruction_flow = mocker.patch('orchestration.flows.bl832.alcf.alcf_tomopy_reconstruction_flow',
                                            return_value=True)
    mock_alcf_tiff_to_zarr_flow = mocker.patch('orchestration.flows.bl832.alcf.alcf_tiff_to_zarr_flow',
                                               return_value=True)
    mock_transfer_to_data832 = mocker.patch('orchestration.flows.bl832.alcf.transfer_data_to_data832',
                                            return_value=True)
    mock_transfer_to_nersc = mocker.patch('orchestration.flows.bl832.alcf.transfer_data_to_nersc',
                                          return_value=True)
    mock_schedule_pruning = mocker.patch('orchestration.flows.bl832.alcf.schedule_pruning',
                                         return_value=True)

    alcf_raw_path = f"{folder_name}/{file_name}.h5"
    scratch_path_tiff = f"{folder_name}/rec{file_name}/"
    scratch_path_zarr = f"{folder_name}/rec{file_name}.zarr/"

    # Assert the expected results
    # Case 1: Send to ALCF is True and is_export_control is False
    # Expect all functions to be called
    send_to_alcf = True
    is_export_control = False
    result = process_new_832_ALCF_flow(folder_name, file_name, is_export_control, send_to_alcf, config=mock_config)

    mock_transfer_to_alcf.assert_called_once_with(f"{folder_name}/{file_name}.h5",
                                                  mock_config.tc,
                                                  mock_config.data832_raw,
                                                  mock_config.alcf832_raw)

    mock_reconstruction_flow.assert_called_once_with(raw_path=f"bl832/raw/{folder_name}",
                                                     scratch_path=f"bl832/scratch/{folder_name}",
                                                     folder_name=folder_name, file_name=f"{file_name}.h5")

    mock_alcf_tiff_to_zarr_flow.assert_called_once_with(raw_path=f"bl832/raw/{folder_name}",
                                                        scratch_path=f"bl832/scratch/{folder_name}",
                                                        folder_name=folder_name, file_name=f"{file_name}.h5")
    mock_transfer_to_data832.assert_has_calls([
        mocker.call(scratch_path_tiff,
                    mock_config.tc, mock_config.alcf832_scratch, mock_config.data832_scratch),
        mocker.call(scratch_path_zarr,
                    mock_config.tc, mock_config.alcf832_scratch, mock_config.data832_scratch)
    ])

    mock_schedule_pruning.assert_called_once_with(
        alcf_raw_path=alcf_raw_path,
        alcf_scratch_path_tiff=scratch_path_tiff,
        alcf_scratch_path_zarr=scratch_path_zarr,
        nersc_scratch_path_tiff=None,
        nersc_scratch_path_zarr=None,
        data832_raw_path=alcf_raw_path,
        data832_scratch_path_tiff=f"{scratch_path_tiff}",
        data832_scratch_path_zarr=f"{scratch_path_zarr}",
        one_minute=True
    )
    assert isinstance(result, list), "Result should be a list"
    assert result == [True, True, True, True, True], "Result does not match expected values"
    mock_transfer_to_alcf.reset_mock()
    mock_reconstruction_flow.reset_mock()
    mock_alcf_tiff_to_zarr_flow.reset_mock()
    mock_transfer_to_data832.reset_mock()
    mock_transfer_to_nersc.reset_mock()
    mock_schedule_pruning.reset_mock()

    # Case 2: Send to ALCF is False and is_export_control is False
    # Expect no functions to be called
    send_to_alcf = False
    is_export_control = False
    result = process_new_832_ALCF_flow(folder_name, file_name, is_export_control, send_to_alcf, config=mock_config)
    mock_transfer_to_alcf.assert_not_called()
    mock_reconstruction_flow.assert_not_called()
    mock_alcf_tiff_to_zarr_flow.assert_not_called()
    mock_transfer_to_data832.assert_not_called()
    mock_transfer_to_nersc.assert_not_called()
    mock_schedule_pruning.assert_not_called()
    assert isinstance(result, list), "Result should be a list"
    assert result == [False, False, False, False, False], "Result does not match expected values"

    mock_transfer_to_alcf.reset_mock()
    mock_reconstruction_flow.reset_mock()
    mock_alcf_tiff_to_zarr_flow.reset_mock()
    mock_transfer_to_data832.reset_mock()
    mock_transfer_to_nersc.reset_mock()
    mock_schedule_pruning.reset_mock()

    # Case 3: Send to ALCF is False and is_export_control is True
    # Expect no functions to be called
    send_to_alcf = False
    is_export_control = True
    result = process_new_832_ALCF_flow(folder_name, file_name, is_export_control, send_to_alcf, config=mock_config)
    mock_transfer_to_alcf.assert_not_called()
    mock_reconstruction_flow.assert_not_called()
    mock_alcf_tiff_to_zarr_flow.assert_not_called()
    mock_transfer_to_data832.assert_not_called()
    mock_transfer_to_nersc.assert_not_called()
    mock_schedule_pruning.assert_not_called()
    assert isinstance(result, list), "Result should be a list"
    assert result == [False, False, False, False, False], "Result does not match expected values"

    mock_transfer_to_alcf.reset_mock()
    mock_reconstruction_flow.reset_mock()
    mock_alcf_tiff_to_zarr_flow.reset_mock()
    mock_transfer_to_data832.reset_mock()
    mock_transfer_to_nersc.reset_mock()
    mock_schedule_pruning.reset_mock()

    # Case 4: Send to ALCF is True and is_export_control is True
    # Expect no functions to be called
    send_to_alcf = True
    is_export_control = True
    result = process_new_832_ALCF_flow(folder_name, file_name, is_export_control, send_to_alcf, config=mock_config)
    mock_transfer_to_alcf.assert_not_called()
    mock_reconstruction_flow.assert_not_called()
    mock_alcf_tiff_to_zarr_flow.assert_not_called()
    mock_transfer_to_data832.assert_not_called()
    mock_transfer_to_nersc.assert_not_called()
    mock_schedule_pruning.assert_not_called()
    assert isinstance(result, list), "Result should be a list"
    assert result == [False, False, False, False, False], "Result does not match expected values"
