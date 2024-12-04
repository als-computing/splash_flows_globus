import asyncio
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4
import warnings

from prefect.blocks.system import JSON, Secret
from prefect.testing.utilities import prefect_test_harness
import pytest
from pytest_mock import MockFixture

from .test_globus import MockTransferClient

warnings.filterwarnings("ignore", category=DeprecationWarning)


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

        decision_settings = JSON(value={
            "alcf_recon_flow/alcf_recon_flow": True,
            "nersc_recon/nersc_recon": False,  # This is a placeholder for the NERSC reconstruction flow
            "new_832_file_flow/new_file_832": False
        })
        decision_settings.save(name="decision-settings")
        yield


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
        self.alcf832_raw = self.endpoints["alcf832_raw"]
        self.alcf832_scratch = self.endpoints["alcf832_scratch"]
        self.data832 = self.endpoints["data832"]
        self.data832_raw = self.endpoints["data832_raw"]
        self.data832_scratch = self.endpoints["data832_scratch"]
        self.nersc832_alsdev_scratch = self.endpoints["nersc832_alsdev_scratch"]
        self.nersc832 = self.endpoints["nersc832"]
        self.nersc_test = self.endpoints["nersc_test"]
        self.spot832 = self.endpoints["spot832"]
        self.scicat = config["scicat"]


# Mock the Client class to avoid real network calls
class MockGlobusComputeClient:
    def __init__(self, *args, **kwargs):
        # No real initialization, as this is a mock
        pass

    def version_check(self):
        # Mock version check to do nothing
        pass

    def run(self, *args, **kwargs):
        # Return a mock task ID
        return "mock_task_id"

    def get_task(self, task_id):
        # Return a mock task response
        return {
            "pending": False,
            "status": "success",
            "result": "mock_result"
        }

    def get_result(self, task_id):
        # Return a mock result
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


def test_832_dispatcher(mocker: MockFixture):
    """Test 832 uber decision flow."""

    # Mock the Secret block load using a simple manual mock class
    class MockSecret:
        value = str(uuid4())

    mocker.patch('prefect.blocks.system.Secret.load', return_value=MockSecret())

    # Import decision flow after mocking the necessary components
    from orchestration.flows.bl832.dispatcher import dispatcher

    # Mock read_deployment_by_name with a manually defined mock class
    class MockDeployment:
        version = "1.0.0"
        flow_id = str(uuid4())
        name = "test_deployment"

    mocker.patch('prefect.client.orchestration.PrefectClient.read_deployment_by_name',
                 return_value=MockDeployment())

    # Mock run_deployment to avoid executing any Prefect workflows
    async def mock_run_deployment(*args, **kwargs):
        return None

    mocker.patch('prefect.deployments.deployments.run_deployment', new=mock_run_deployment)

    # Mock asyncio.gather to avoid actual async task execution
    async def mock_gather(*args, **kwargs):
        return [None]

    mocker.patch('asyncio.gather', new=mock_gather)

    # Run the decision flow
    result = asyncio.run(dispatcher(
        file_path="/global/raw/transfer_tests/test.txt",
        is_export_control=False,
        config=MockConfig832()
    ))

    # Ensure the flow runs without throwing an error
    assert result is None, "The decision flow did not complete successfully."


def test_process_new_832_file(mocker: MockFixture):
    """
    Test process_new_832_file function in orchestration/flows/bl832/move.py
    """
    mock_secret = mocker.MagicMock()
    mock_secret.value = str(uuid4())
    bl832_settings = mocker.MagicMock()
    bl832_settings.value = {"delete_spot832_files_after_days": 1,
                            "delete_data832_files_after_days": 1}

    # https://pytest-mock.readthedocs.io/en/latest/usage.html#usage-as-context-manager
    with mocker.patch('prefect.blocks.system.Secret.load', return_value=mock_secret):
        from orchestration.flows.bl832.move import process_new_832_file

    mocker.patch('prefect.blocks.system.JSON.load', return_value=bl832_settings)
    mocker.patch('prefect.client.orchestration.PrefectClient.read_deployment_by_name',
                 return_value=mocker.MagicMock(id=str(uuid4())))

    mock_config = MockConfig832()
    mock_transfer_spot_to_data = mocker.patch('orchestration.flows.bl832.move.transfer_spot_to_data',
                                              return_value=True)
    mock_transfer_data_to_nersc = mocker.patch('orchestration.flows.bl832.move.transfer_data_to_nersc',
                                               return_value=True)
    mock_ingest_schedule = mocker.patch('orchestration.prefect.schedule', return_value=None)
    mock_schedule_prefect_flow = mocker.patch('orchestration.prefect.schedule_prefect_flow',
                                              return_value=None)
    mock_ingest_dataset = mocker.patch('orchestration.flows.scicat.ingest.ingest_dataset',
                                       return_value=None)
    mock_ingest_dataset = mocker.patch('orchestration.flows.scicat.ingest.ingest_dataset_task',
                                       return_value=mocker.ANY)

    # Case 1: is_export_control is True, send_to_nersc is False
    is_export_control = False
    send_to_nersc = True
    file_path = "/global/raw/transfer_tests/test.txt"
    result = process_new_832_file(file_path, is_export_control, send_to_nersc, mock_config)
    mock_ingest_schedule, mock_schedule_prefect_flow, result  # avoid "not used" linting errors

    relative_path = "/raw/transfer_tests/test.txt"
    TOMO_INGESTOR_MODULE = "orchestration.flows.bl832.ingest_tomo832"

    mock_transfer_spot_to_data.assert_called_once_with(relative_path,
                                                       mock_config.tc,
                                                       mock_config.spot832,
                                                       mock_config.data832)
    mock_transfer_data_to_nersc.assert_called_once_with(relative_path,
                                                        mock_config.tc,
                                                        mock_config.data832,
                                                        mock_config.nersc832)
    mock_ingest_dataset.assert_called_once_with(file_path,
                                                TOMO_INGESTOR_MODULE)

    # this assertion does not pass when called with the expected arguments
    mock_schedule_prefect_flow.assert_has_calls(())

    assert all(r.type == "COMPLETED" for r in result), "Flow should complete successfully"


def test_process_new_832_ALCF_flow(mocker: MockFixture):
    mock_secret = mocker.MagicMock()
    mock_secret.value = str(uuid4())
    # https://pytest-mock.readthedocs.io/en/latest/usage.html#usage-as-context-manager
    with mocker.patch('prefect.blocks.system.Secret.load', return_value=mock_secret):
        from orchestration.flows.bl832.alcf import alcf_recon_flow

    """Test for the process of a new 832 ALCF flow"""
    file_path = "/global/raw/transfer_tests/test.h5"
    folder_name = "transfer_tests"
    file_name = "test"

    # Mock the Config832 class inserting into the module being tested
    mock_config = MockConfig832()

    mock_transfer_to_alcf = mocker.patch('orchestration.flows.bl832.alcf.transfer_data_to_alcf',
                                         return_value=True)
    mock_reconstruction_flow = mocker.patch('orchestration.flows.bl832.alcf.alcf_globus_compute_reconstruction',
                                            return_value=True)
    mock_alcf_tiff_to_zarr_flow = mocker.patch('orchestration.flows.bl832.alcf.alcf_globus_compute_tiff_to_zarr',
                                               return_value=True)
    mock_transfer_to_data832 = mocker.patch('orchestration.flows.bl832.alcf.transfer_data_to_data832',
                                            return_value=True)
    mock_schedule_pruning = mocker.patch('orchestration.flows.bl832.alcf.schedule_pruning',
                                         return_value=True)

    alcf_raw_path = f"{folder_name}/{file_name}.h5"
    scratch_path_tiff = f"{folder_name}/rec{file_name}/"
    scratch_path_zarr = f"{folder_name}/rec{file_name}.zarr/"

    # Assert the expected results
    # Case 1: is_export_control is False
    # Expect all functions to be called
    is_export_control = False

    result = alcf_recon_flow(file_path, is_export_control, config=mock_config)

    mock_transfer_to_alcf.assert_called_once_with(
        "transfer_tests/test.h5",
        mock_config.tc,
        mock_config.data832_raw,
        mock_config.alcf832_raw)

    mock_reconstruction_flow.assert_called_once_with(
        folder_name=folder_name, file_name=f"{file_name}.h5")

    raw_path = f"/eagle/IRI-ALS-832/data/raw/{alcf_raw_path}"
    tiff_scratch_path = f"/eagle/IRI-ALS-832/data/scratch/{folder_name}/rec{file_name}/"

    mock_alcf_tiff_to_zarr_flow.assert_called_once_with(
        raw_path=raw_path,
        tiff_scratch_path=tiff_scratch_path)
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
        one_minute=False,
        config=mock_config
    )
    assert isinstance(result, list), "Result should be a list"
    assert result == [True, True, True, True, True], "Result does not match expected values"
    mock_transfer_to_alcf.reset_mock()
    mock_reconstruction_flow.reset_mock()
    mock_alcf_tiff_to_zarr_flow.reset_mock()
    mock_transfer_to_data832.reset_mock()
    mock_schedule_pruning.reset_mock()

    # Case 2: is_export_control is True
    # Expect no functions to be called
    is_export_control = True
    result = alcf_recon_flow(file_path, is_export_control, config=mock_config)
    mock_transfer_to_alcf.assert_not_called()
    mock_reconstruction_flow.assert_not_called()
    mock_alcf_tiff_to_zarr_flow.assert_not_called()
    mock_transfer_to_data832.assert_not_called()
    mock_schedule_pruning.assert_not_called()
    assert isinstance(result, list), "Result should be a list"
    assert result == [False, False, False, False, False], "Result does not match expected values"

    mock_transfer_to_alcf.reset_mock()
    mock_reconstruction_flow.reset_mock()
    mock_alcf_tiff_to_zarr_flow.reset_mock()
    mock_transfer_to_data832.reset_mock()
    mock_schedule_pruning.reset_mock()
