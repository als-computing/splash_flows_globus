from pydantic import BaseModel, ConfigDict, PydanticDeprecatedSince20
import warnings
import pytest
from unittest.mock import MagicMock, patch, ANY
from typing import List, Optional, Dict, Any
from uuid import UUID
from orchestration.globus.flows import get_flows_client, get_specific_flow_client
from orchestration.flows.bl832.alcf import (
    alcf_tomopy_reconstruction_flow,
    process_new_832_ALCF_flow
)

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=PydanticDeprecatedSince20)


# Define models using Pydantic for better validation and type checking
class FlowDefinition(BaseModel):
    """Model for flow definition"""
    StartAt: str
    States: Dict[str, Dict[str, Any]]


class FlowInputSchema(BaseModel):
    """Model for flow input schema"""
    type: str
    properties: Dict[str, Dict[str, Any]]


class CreateFlowRequest(BaseModel):
    """Model for creating a flow request"""
    title: str
    definition: FlowDefinition
    input_schema: FlowInputSchema
    subtitle: Optional[str] = None
    description: Optional[str] = None
    flow_viewers: Optional[List[str]] = None
    flow_starters: Optional[List[str]] = None
    flow_administrators: Optional[List[str]] = None
    keywords: Optional[List[str]] = None
    subscription_id: Optional[UUID] = None
    additional_fields: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)


class RunFlowRequest(BaseModel):
    """Model for running a flow request"""
    body: Dict[str, Any]
    label: Optional[str] = None
    tags: Optional[List[str]] = None
    run_monitors: Optional[List[str]] = None
    run_managers: Optional[List[str]] = None
    additional_fields: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

# Define mock classes for FlowsClient and SpecificFlowClient
class MockFlowsClient:
    """Mock class for FlowsClient"""
    def __init__(self):
        self.flows = {}


    def create_flow(self, request: CreateFlowRequest) -> Dict[str, Any]:
        """Mock method for initializing a new flow with Globus Flows"""
        flow_id = UUID("123e4567-e89b-12d3-a456-426614174000")
        self.flows[flow_id] = request.model_dump()
        return {"flow_id": str(flow_id)}


    def get_flow(self, flow_id: UUID) -> Dict[str, Any]:
        """Mock method for getting a flow"""
        return self.flows.get(flow_id, {})


class MockSpecificFlowClient:
    """Mock class for SpecificFlowClient"""
    def __init__(self, flow_id: UUID):
        self.flow_id = flow_id
        self.runs = {}


    def run_flow(self, request: RunFlowRequest) -> Dict[str, Any]:
        """Mock method for running a registered flow function"""
        run_id = UUID("123e4567-e89b-12d3-a456-426614174001")
        self.runs[run_id] = request.model_dump()
        return {"run_id": str(run_id), "status": "SUCCEEDED"}


    def get_run(self, run_id: UUID) -> Dict[str, Any]:
        """Mock method for getting a run"""
        return self.runs.get(run_id, {})


# Create fixtures for mocking the confidential client, specific flow client, and client credentials authorizer
@pytest.fixture
def mock_confidential_client():
    """Fixture for mocking the confidential client"""
    with patch('orchestration.globus.flows.ConfidentialAppAuthClient') as MockClient:
        instance = MockClient.return_value
        instance.oauth2_client_credentials_tokens.return_value = {
            "access_token": "fake_access_token",
            "expires_in": 3600
        }
        yield instance


@pytest.fixture
def mock_specific_flow_client():
    """Fixture for mocking the specific flow client"""
    with patch('orchestration.globus.flows.SpecificFlowClient') as MockClient:
        instance = MockClient.return_value
        instance.scopes = MagicMock()
        instance.scopes.make_mutable.return_value = "fake_scope"
        yield instance


@pytest.fixture
def mock_client_credentials_authorizer():
    """Fixture for mocking the client credentials authorizer"""
    with patch('orchestration.globus.flows.ClientCredentialsAuthorizer') as MockAuthorizer:
        instance = MockAuthorizer.return_value
        yield instance

# Create fixtures for mocking the tasks in the 832 ALCF flow
@pytest.fixture
def mock_transfer_data_to_alcf():
    """Fixture for mocking the transfer_data_to_alcf task"""
    with patch('orchestration.flows.bl832.alcf.transfer_data_to_alcf') as mock_task:
        yield mock_task


@pytest.fixture
def mock_transfer_data_to_nersc():
    """Fixture for mocking the transfer_data_to_nersc task"""
    with patch('orchestration.flows.bl832.alcf.transfer_data_to_nersc') as mock_task:
        yield mock_task


@pytest.fixture
def mock_schedule_pruning():
    """Fixture for mocking the schedule_pruning task"""
    with patch('orchestration.flows.bl832.alcf.schedule_pruning') as mock_task:
        yield mock_task


@pytest.fixture
def mock_alcf_tomopy_reconstruction_flow():
    """Fixture for mocking the alcf_tomopy_reconstruction_flow"""
    with patch('orchestration.flows.bl832.alcf.alcf_tomopy_reconstruction_flow') as mock_task:
        yield mock_task


# Create tests for the functions in the 832 ALCF flow
def test_get_flows_client(mock_confidential_client):
    """Test for getting the flows client"""
    # Run the function
    client = get_flows_client()
    
    # Ensure the returned client is not None
    assert client is not None, "Client should not be None"


def test_get_specific_flow_client(mock_confidential_client, mock_specific_flow_client, mock_client_credentials_authorizer):
    """Test for getting a specific flow client"""
    flow_id = "your_flow_id"
    collection_ids = ["your_collection_id"]

    # Run the function
    client = get_specific_flow_client(flow_id, collection_ids)
    
    # Ensure the returned client is not None
    assert client is not None, "Client should not be None"


def test_process_new_832_ALCF_flow(mock_transfer_data_to_alcf, mock_transfer_data_to_nersc, mock_schedule_pruning, mock_alcf_tomopy_reconstruction_flow):
    """Test for the process of a new 832 ALCF flow"""
    folder_name = "test_folder"
    file_name = "test_file"
    is_export_control = False
    send_to_alcf = True

    # Set up the mock return values
    mock_transfer_data_to_alcf.return_value = True
    mock_transfer_data_to_nersc.return_value = True
    mock_alcf_tomopy_reconstruction_flow.return_value = True

    # Run the function
    result = process_new_832_ALCF_flow(folder_name, file_name, is_export_control, send_to_alcf)

    # Assert the expected results
    assert isinstance(result, list), "Result should be a list"
    assert result == [True, True, True], "Result does not match expected values"

    # Verify that the transfer_data_to_alcf task was called with the expected arguments
    mock_transfer_data_to_alcf.assert_called_once_with(f"{folder_name}/{file_name}.h5", ANY, ANY, ANY)
    
    # Verify that the alcf_tomopy_reconstruction_flow task was called with the expected arguments
    mock_alcf_tomopy_reconstruction_flow.assert_called_once_with(
        raw_path=f"bl832_test/raw/{folder_name}",
        scratch_path=f"bl832_test/scratch/{folder_name}",
        folder_name=folder_name,
        file_name=f"{file_name}.h5"
    )
    
    # Verify that the transfer_data_to_nersc task was called with the expected arguments
    mock_transfer_data_to_nersc.assert_any_call(
        f"{folder_name}/rec{file_name}/",
        ANY, ANY, ANY
    )
    mock_transfer_data_to_nersc.assert_any_call(
        f"{folder_name}/rec{file_name}.zarr/",
        ANY, ANY, ANY
    )
    
    # Verify that the schedule_pruning task was called with the expected arguments
    mock_schedule_pruning.assert_called_once_with(
        alcf_raw_path=f"{folder_name}/{file_name}.h5",
        alcf_scratch_path_tiff=f"{folder_name}/rec{file_name}/",
        alcf_scratch_path_zarr=f"{folder_name}/rec{file_name}.zarr/",
        nersc_scratch_path_tiff=f"{folder_name}/rec{file_name}/",
        nersc_scratch_path_zarr=f"{folder_name}/rec{file_name}.zarr/",
        one_minute=True
    )