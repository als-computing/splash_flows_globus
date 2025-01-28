# test_transfer_controller.py

import pytest
import globus_sdk
from unittest.mock import MagicMock, patch

from orchestration.flows.bl832.config import Config832
from orchestration.transfer_controller import (
    FileSystemEndpoint,
    GlobusTransferController,
    SimpleTransferController,
    get_transfer_controller,
    CopyMethod,
)
from orchestration.globus.transfer import GlobusEndpoint


@pytest.fixture
def mock_config832():
    """
    A pytest fixture that provides a mocked Config832 object
    with a mocked TransferClient (tc).
    """
    mock_config = MagicMock(spec=Config832)
    # Mock the Globus transfer client
    mock_config.tc = MagicMock(name="MockTransferClient")
    return mock_config


@pytest.fixture
def mock_globus_endpoint():
    """
    A pytest fixture that returns a mocked GlobusEndpoint.
    """
    endpoint = GlobusEndpoint(
        name="mock_globus_endpoint",
        root_path="/mock_globus_root/",
        uuid="mock_endpoint_id",
        uri="mock_endpoint_uri"
    )
    return endpoint


@pytest.fixture
def mock_file_system_endpoint():
    """
    A pytest fixture that returns a FileSystemEndpoint instance.
    """
    endpoint = FileSystemEndpoint(
        name="mock_filesystem_endpoint",
        root_path="/mock_fs_root"
    )
    return endpoint


# --------------------------------------------------------------------------
# Tests for get_transfer_controller
# --------------------------------------------------------------------------
def test_get_transfer_controller_globus(mock_config832):
    """
    Test that get_transfer_controller returns a GlobusTransferController when
    the transfer type is CopyMethod.GLOBUS.
    """
    controller = get_transfer_controller(CopyMethod.GLOBUS, mock_config832)
    assert isinstance(controller, GlobusTransferController), (
        "get_transfer_controller should return a GlobusTransferController "
        "instance for GLOBUS transfer."
    )


def test_get_transfer_controller_simple(mock_config832):
    """
    Test that get_transfer_controller returns a SimpleTransferController when
    the transfer type is CopyMethod.SIMPLE.
    """
    controller = get_transfer_controller(CopyMethod.SIMPLE, mock_config832)
    assert isinstance(controller, SimpleTransferController), (
        "get_transfer_controller should return a SimpleTransferController "
        "instance for SIMPLE transfer."
    )


def test_get_transfer_controller_invalid(mock_config832):
    """
    Test that get_transfer_controller raises ValueError for invalid transfer method.
    """
    with pytest.raises(ValueError, match="Invalid transfer type"):
        get_transfer_controller("invalid_type", mock_config832)


# --------------------------------------------------------------------------
# Tests for GlobusTransferController
# --------------------------------------------------------------------------
def test_globus_transfer_controller_copy_success(mock_config832, mock_globus_endpoint):
    """
    Test a successful copy() operation using GlobusTransferController.
    We mock start_transfer to return True.
    """
    with patch("orchestration.transfer_controller.start_transfer", return_value=True) as mock_start_transfer:
        controller = GlobusTransferController(mock_config832)
        result = controller.copy(
            file_path="some_dir/test_file.txt",
            source=mock_globus_endpoint,
            destination=mock_globus_endpoint,
        )

        assert result is True, "Expected True when transfer completes successfully."
        mock_start_transfer.assert_called_once()

        # Verify arguments passed to start_transfer
        _, called_kwargs = mock_start_transfer.call_args
        assert called_kwargs["source_endpoint"] == mock_globus_endpoint
        assert called_kwargs["dest_endpoint"] == mock_globus_endpoint
        assert "max_wait_seconds" in called_kwargs, "max_wait_seconds should be passed to start_transfer."


def test_globus_transfer_controller_copy_failure(mock_config832, mock_globus_endpoint):
    """
    Test a failing copy() operation using GlobusTransferController.
    We mock start_transfer to return False, indicating a transfer failure.
    """
    with patch("orchestration.transfer_controller.start_transfer", return_value=False) as mock_start_transfer:
        controller = GlobusTransferController(mock_config832)
        result = controller.copy(
            file_path="some_dir/test_file.txt",
            source=mock_globus_endpoint,
            destination=mock_globus_endpoint,
        )
        assert result is False, "Expected False when transfer fails."
        mock_start_transfer.assert_called_once()


def test_globus_transfer_controller_copy_exception(mock_config832, mock_globus_endpoint):
    """
    Test copy() operation that raises a TransferAPIError exception in GlobusTransferController.
    """
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.reason = "Bad Request"

    with patch(
        "orchestration.transfer_controller.start_transfer",
        side_effect=globus_sdk.services.transfer.errors.TransferAPIError(mock_response, "Mocked Error")
    ) as mock_start_transfer:
        controller = GlobusTransferController(mock_config832)
        result = controller.copy(
            file_path="some_dir/test_file.txt",
            source=mock_globus_endpoint,
            destination=mock_globus_endpoint,
        )
        assert result is False, "Expected False when TransferAPIError is raised."
        mock_start_transfer.assert_called_once()


# --------------------------------------------------------------------------
# Tests for SimpleTransferController
# --------------------------------------------------------------------------
def test_simple_transfer_controller_no_file_path(mock_config832, mock_file_system_endpoint):
    """
    Test that copy() returns False if no file_path is provided.
    """
    controller = SimpleTransferController(mock_config832)
    result = controller.copy(
        file_path="",
        source=mock_file_system_endpoint,
        destination=mock_file_system_endpoint,
    )
    assert result is False, "Expected False when no file_path is provided."


def test_simple_transfer_controller_no_source_or_destination(mock_config832):
    """
    Test that copy() returns False if source or destination is None.
    """
    controller = SimpleTransferController(mock_config832)
    result = controller.copy(
        file_path="test.txt",
        source=None,
        destination=None,
    )
    assert result is False, "Expected False when either source or destination is None."


def test_simple_transfer_controller_copy_success(mock_config832, mock_file_system_endpoint):
    """
    Test a successful copy() operation using SimpleTransferController by mocking os.system
    to return 0 (indicating success).
    """
    with patch("os.system", return_value=0) as mock_os_system:
        controller = SimpleTransferController(mock_config832)
        result = controller.copy(
            file_path="some_dir/test_file.txt",
            source=mock_file_system_endpoint,
            destination=mock_file_system_endpoint,
        )

        assert result is True, "Expected True when os.system returns 0."
        mock_os_system.assert_called_once()
        command_called = mock_os_system.call_args[0][0]
        assert "cp -r" in command_called, "Expected cp command in os.system call."


def test_simple_transfer_controller_copy_failure(mock_config832, mock_file_system_endpoint):
    """
    Test a failing copy() operation using SimpleTransferController by mocking os.system
    to return a non-zero code.
    """
    with patch("os.system", return_value=1) as mock_os_system:
        controller = SimpleTransferController(mock_config832)
        result = controller.copy(
            file_path="some_dir/test_file.txt",
            source=mock_file_system_endpoint,
            destination=mock_file_system_endpoint,
        )

        assert result is False, "Expected False when os.system returns a non-zero code."
        mock_os_system.assert_called_once()
        command_called = mock_os_system.call_args[0][0]
        assert "cp -r" in command_called, "Expected cp command in os.system call."


def test_simple_transfer_controller_copy_exception(mock_config832, mock_file_system_endpoint):
    """
    Test a copy() operation that raises an exception in SimpleTransferController.
    """
    with patch("os.system", side_effect=Exception("Mocked cp error")) as mock_os_system:
        controller = SimpleTransferController(mock_config832)
        result = controller.copy(
            file_path="some_dir/test_file.txt",
            source=mock_file_system_endpoint,
            destination=mock_file_system_endpoint,
        )

        assert result is False, "Expected False when an exception is raised during copy."
        mock_os_system.assert_called_once()
