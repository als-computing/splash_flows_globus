from pydantic import BaseModel, ConfigDict, PydanticDeprecatedSince20
import warnings
import pytest
from unittest.mock import MagicMock, patch
from typing import List, Optional, Dict, Any
from uuid import UUID
from orchestration.flows.bl832.alcf import (
    process_new_832_ALCF_flow
)
from orchestration.flows.bl832.config import Config832

from prefect.testing.utilities import prefect_test_harness
from prefect.blocks.system import JSON, Secret

from orchestration._tests.test_globus import MockTransferClient

from globus_sdk import ConfidentialAppAuthClient
from globus_sdk.authorizers.client_credentials import ClientCredentialsAuthorizer
from globus_compute_sdk.sdk.client import Client


warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=PydanticDeprecatedSince20)

@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        globus_client_id = Secret(value =  "test-globus-client-id")
        globus_client_id.save(name = "globus-client-id")
        globus_client_secret = Secret(value = "your_globus_client_secret")
        globus_client_secret.save(name = "globus-client-secret")
        pruning_config = JSON(value={"max_wait_seconds": 600})
        pruning_config.save(name="pruning-config")
        yield


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


def test_process_new_832_ALCF_flow(monkeypatch):
    """Test for the process of a new 832 ALCF flow"""
    folder_name = "test_folder"
    file_name = "test_file"
    is_export_control = False
    send_to_alcf = True

    mock_flow_client = MockSpecificFlowClient(UUID("123e4567-e89b-12d3-a456-426614174000"))
    mock_transfer_client = MockTransferClient()

    # Monkeypatch the Config832 __init__ method to inject mock clients
    original_init = Config832.__init__

    def mock_init(self):
        original_init(self)
        self.flow_client = mock_flow_client
        self.tc = mock_transfer_client

    monkeypatch.setattr(Config832, "__init__", mock_init)

    # Monkeypatch the ConfidentialAppAuthClient and ClientCredentialsAuthorizer
    def mock_oauth2_client_credentials_tokens(self):
        return {
            "access_token": "fake_access_token",
            "expires_in": 3600,
        }

    monkeypatch.setattr(ConfidentialAppAuthClient, "oauth2_client_credentials_tokens", mock_oauth2_client_credentials_tokens)

    def mock_get_new_access_token(self):
        pass

    monkeypatch.setattr(ClientCredentialsAuthorizer, "_get_new_access_token", mock_get_new_access_token)

    # Mock the Client to avoid real network calls and login flow
    with patch.object(Client, 'version_check', return_value=None):
        with patch.object(Client, '__init__', return_value=None):
            result = process_new_832_ALCF_flow(folder_name, file_name, is_export_control, send_to_alcf)

            # Assert the expected results
            assert isinstance(result, list), "Result should be a list"
            assert result == [True, True, True], "Result does not match expected values"
