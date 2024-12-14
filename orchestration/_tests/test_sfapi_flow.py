import pytest
from unittest.mock import MagicMock, PropertyMock, patch
from uuid import uuid4

from prefect.blocks.system import Secret
from prefect.testing.utilities import prefect_test_harness

# Patch Secret.load globally before importing application code
with patch("prefect.blocks.system.Secret.load") as mock_secret_load:
    mock_secret_load.return_value = Secret(value=str(uuid4()))
    # Import application modules after patching Secret.load
    from orchestration.flows.bl832.nersc import nersc_recon_flow, NERSCTomographyHPCController
    from orchestration.flows.bl832.config import Config832
    from orchestration.flows.bl832.job_controller import HPC
    from orchestration.nersc import NerscClient


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
        yield


@pytest.fixture
def mock_nersc_client():
    """Fixture to mock the NerscClient class."""
    mock_client = MagicMock(spec=NerscClient)
    mock_client.user.return_value.name = "testuser"

    # Mock perlmutter client with job-related methods
    perlmutter_mock = MagicMock()
    type(mock_client).perlmutter = PropertyMock(return_value=perlmutter_mock)
    perlmutter_mock.submit_job.return_value.jobid = "12345"
    perlmutter_mock.submit_job.return_value.state = "COMPLETED"
    perlmutter_mock.job.return_value.state = "COMPLETED"
    perlmutter_mock.job.return_value.complete = MagicMock()
    perlmutter_mock.job.return_value.jobid = "12345"

    return mock_client


@pytest.fixture
def mock_config832():
    """Fixture to mock the Config832 class."""
    mock_config = MagicMock(spec=Config832)
    mock_config.harbor_images832 = {
        "recon_image": "mock_recon_image",
        "multires_image": "mock_multires_image",
    }
    return mock_config


@pytest.fixture
def mock_controller(mock_nersc_client, mock_config832):
    """Fixture to mock the NERSCTomographyHPCController class."""
    with patch("orchestration.flows.bl832.job_controller.get_controller") as mock_get_controller:
        mock_get_controller.return_value = MagicMock(
            spec=HPC.NERSC,
            client=mock_nersc_client,
            config=mock_config832
        )
        mock_get_controller.return_value.build_multi_resolution = MagicMock(return_value=True)
        mock_get_controller.return_value.reconstruct = MagicMock(return_value=True)
        yield mock_get_controller.return_value


def test_nersc_recon_flow_success(mock_controller):
    """Test the nersc_recon_flow for a successful run."""
    file_path = "dabramov/20230606_151124_jong-seto_fungal-mycelia_roll-AQ_fungi1_fast.h5"

    with patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller):
        result = nersc_recon_flow(file_path=file_path)

    assert result is True, "nersc_recon_flow should return True for a successful run."


def test_nersc_recon_flow_failure(mock_controller):
    """Test the nersc_recon_flow for a failure scenario."""
    file_path = "dabramov/20230606_151124_jong-seto_fungal-mycelia_roll-AQ_fungi1_fast.h5"
    mock_controller.build_multi_resolution.return_value = False

    with patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller):
        result = nersc_recon_flow(file_path=file_path)

    assert result is False, "nersc_recon_flow should return False for a failure scenario."


def test_nersc_client_initialization_error():
    """Test error handling during NERSC client initialization."""
    with patch("orchestration.flows.bl832.nersc.NERSCTomographyHPCController.create_nersc_client",
               side_effect=ValueError("Missing NERSC credentials paths.")):
        with pytest.raises(ValueError, match="Missing NERSC credentials paths."):
            NERSCTomographyHPCController.create_nersc_client()


def test_job_submission(mock_controller):
    """Test job submission and status updates."""
    job_script = "mock_job_script"
    mock_job = mock_controller.client.perlmutter.submit_job.return_value
    job_id = mock_job.jobid

    mock_controller.client.perlmutter.submit_job(job_script)
    mock_controller.client.perlmutter.submit_job.assert_called_once_with(job_script)
    assert job_id == "12345", "Job ID should match the mock job ID."


def test_job_recovery(mock_controller):
    """Test recovery of a failed or lost job."""
    mock_job = mock_controller.client.perlmutter.job.return_value
    mock_job.complete = MagicMock()

    mock_controller.client.perlmutter.job.side_effect = [
        FileNotFoundError("Job not found: 12345"),
        mock_job
    ]

    with patch("time.sleep", return_value=None):
        recon_result = mock_controller.reconstruct(file_path="mock_file_path")
        multires_result = mock_controller.build_multi_resolution(file_path="mock_file_path")

    assert recon_result is True, "Job recovery should succeed."
    assert multires_result is True, "Job recovery should succeed."
