from abc import ABC, abstractmethod
from dotenv import load_dotenv
from enum import Enum
import logging
import os
import time
from typing import Generic, TypeVar

import globus_sdk

from orchestration.config import BeamlineConfig
from orchestration.globus.transfer import GlobusEndpoint, start_transfer
from orchestration.transfer_endpoints import FileSystemEndpoint, TransferEndpoint

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()


Endpoint = TypeVar("Endpoint", bound=TransferEndpoint)


class TransferController(Generic[Endpoint], ABC):
    """
    Abstract base class for transferring data between endpoints.

    This class defines the common interface that all transfer controllers must implement,
    regardless of the specific transfer mechanism they use.

    Args:
        config (BeamlineConfig): Configuration object containing endpoints and credentials
    """
    def __init__(
        self,
        config: BeamlineConfig
    ) -> None:
        self.config = config
        logger.debug(f"Initialized {self.__class__.__name__} with config for beamline {config.beamline_id}")

    @abstractmethod
    def copy(
        self,
        file_path: str = None,
        source: Endpoint = None,
        destination: Endpoint = None,
    ) -> bool:
        """
        Copy a file from a source endpoint to a destination endpoint.

        Args:
            file_path (str): The path of the file to copy, relative to the endpoint's root path
            source (Endpoint): The source endpoint from which to copy the file
            destination (Endpoint): The destination endpoint to which to copy the file

        Returns:
            bool: True if the transfer was successful, False otherwise.
        """
        pass


class GlobusTransferController(TransferController[GlobusEndpoint]):
    """
    Use Globus Transfer to move data between Globus endpoints.

    This controller handles the transfer of files between Globus endpoints using the
    Globus Transfer API. It manages authentication, transfer submissions, and status tracking.

    Args:
        config (BeamlineConfig): Configuration object containing Globus endpoints and credentials
    """

    def __init__(
        self,
        config: BeamlineConfig
    ) -> None:
        super().__init__(config)
        logger.debug(f"Initialized GlobusTransferController for beamline {config.beamline_id}")

    def copy(
        self,
        file_path: str = None,
        source: GlobusEndpoint = None,
        destination: GlobusEndpoint = None,
    ) -> bool:
        """
        Copy a file from a source Globus endpoint to a destination Globus endpoint.

        This method handles the full transfer process, including path normalization,
        submission to the Globus Transfer API, and waiting for completion or error.

        Args:
            file_path (str): The path of the file to copy, relative to the endpoint's root path
            source (GlobusEndpoint): The source Globus endpoint from which to copy the file
            destination (GlobusEndpoint): The destination Globus endpoint to which to copy the file

        Returns:
            bool: True if the transfer was successful, False otherwise

        Raises:
            globus_sdk.services.transfer.errors.TransferAPIError: If there are issues with the Globus API
        """
        if not file_path:
            logger.error("No file path provided for transfer")
            return False

        if not source or not destination:
            logger.error("Missing source or destination endpoint for transfer")
            return False

        logger.info(f"Transferring {file_path} from {source.name} to {destination.name}")

        # Normalize the file path by removing leading slashes if present
        if file_path[0] == "/":
            file_path = file_path[1:]
            logger.debug(f"Normalized file path to '{file_path}'")

        # Build full paths for source and destination
        source_path = os.path.join(source.root_path, file_path)
        dest_path = os.path.join(destination.root_path, file_path)
        logger.info(f"Transferring {source_path} to {dest_path}")

        # Start the timer for performance tracking
        start_time = time.time()
        success = False
        try:
            logger.info(f"Submitting Globus transfer task from {source.uuid} to {destination.uuid}")
            success = start_transfer(
                transfer_client=self.config.tc,
                source_endpoint=source,
                source_path=source_path,
                dest_endpoint=destination,
                dest_path=dest_path,
                max_wait_seconds=600,
                logger=logger,
            )
            if success:
                logger.info("Transfer completed successfully.")
            else:
                logger.error("Transfer failed.")
            return success
        except globus_sdk.services.transfer.errors.TransferAPIError as e:
            logger.error(f"Globus Transfer API error: {e}")
            logger.error(f"Status code: {e.status_code if hasattr(e, 'status_code') else 'unknown'}")
            logger.error(f"Error details: {e.data if hasattr(e, 'data') else e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during transfer: {str(e)}", exc_info=True)
            return False

        finally:
            # Stop the timer and calculate the duration
            elapsed_time = time.time() - start_time
            logger.info(f"Transfer process took {elapsed_time:.2f} seconds.")
            return success


class SimpleTransferController(TransferController[FileSystemEndpoint]):
    """
    Use a simple 'cp' command to move data within the same system.

    This controller is suitable for transfers between directories on the same
    file system, where network transfer protocols are not needed.

    Args:
        config (BeamlineConfig): Configuration object containing file system paths
    """
    def __init__(
        self,
        config: BeamlineConfig
    ) -> None:
        super().__init__(config)
        logger.debug(f"Initialized SimpleTransferController for beamline {config.beamline_id}")

    def copy(
        self,
        file_path: str = None,
        source: FileSystemEndpoint = None,
        destination: FileSystemEndpoint = None,
    ) -> bool:
        """
        Copy a file from a source directory to a destination directory using the 'cp' command.

        This method handles local file copying through the system's cp command,
        including path normalization and status tracking.

        Args:
            file_path (str): The path of the file to copy, relative to the endpoint's root path
            source (FileSystemEndpoint): The source file system location
            destination (FileSystemEndpoint): The destination file system location

        Returns:
            bool: True if the transfer was successful, False otherwise.
        """
        if not file_path:
            logger.error("No file_path provided for local copy operation")
            return False
        if not source or not destination:
            logger.error("Source or destination endpoint not provided for local copy operation")
            return False

        logger.info(f"Transferring {file_path} from {source.name} to {destination.name}")

        # Normalize file path by removing leading slash if present
        if file_path.startswith("/"):
            file_path = file_path[1:]
            logger.debug(f"Normalized file path to '{file_path}'")

        # Build full paths for source and destination
        source_path = os.path.join(source.root_path, file_path)
        dest_path = os.path.join(destination.root_path, file_path)
        logger.info(f"Transferring {source_path} to {dest_path}")

        # Start the timer
        start_time = time.time()

        try:
            # Check if source file/directory exists
            if not os.path.exists(source_path):
                logger.error(f"Source path does not exist: {source_path}")
                return False

            # Ensure destination directory exists
            dest_dir = os.path.dirname(dest_path)
            if not os.path.exists(dest_dir):
                logger.debug(f"Creating destination directory: {dest_dir}")
                os.makedirs(dest_dir, exist_ok=True)

            # Execute the cp command
            result = os.system(f"cp -r '{source_path}' '{dest_path}'")
            if result == 0:
                logger.info(f"Local copy of '{file_path}' completed successfully")
                return True
            else:
                logger.error(f"Local copy of '{file_path}' failed with exit code {result}")
                return False
        except Exception as e:
            logger.error(f"Unexpected error during local copy: {str(e)}", exc_info=True)
            return False
        finally:
            # Stop the timer and calculate the duration
            elapsed_time = time.time() - start_time
            logger.info(f"Local copy process took {elapsed_time:.2f} seconds")


class CopyMethod(Enum):
    """
    Enum representing different transfer methods.

    These values are used to select the appropriate transfer controller
    through the factory function get_transfer_controller().
    """
    GLOBUS = "globus"         # Transfer between Globus endpoints
    SIMPLE = "simple"         # Local filesystem copy
    CFS_TO_HPSS = "cfs_to_hpss"  # NERSC CFS to HPSS tape archive
    HPSS_TO_CFS = "hpss_to_cfs"  # HPSS tape archive to NERSC CFS


def get_transfer_controller(
    transfer_type: CopyMethod,
    config: BeamlineConfig
) -> TransferController:
    """
    Factory function to get the appropriate transfer controller based on the transfer type.

    Args:
        transfer_type (CopyMethod): The type of transfer to perform
        config (BeamlineConfig): The configuration object containing endpoint information

    Returns:
        TransferController: The appropriate transfer controller instance

    Raises:
        ValueError: If an invalid transfer type is provided
    """
    # Add explicit type checking to handle non-enum inputs
    if not isinstance(transfer_type, CopyMethod):
        error_msg = f"Invalid transfer type: {transfer_type}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.debug(f"Creating transfer controller of type: {transfer_type.name}")

    if transfer_type == CopyMethod.GLOBUS:
        logger.debug("Returning GlobusTransferController")
        return GlobusTransferController(config)
    elif transfer_type == CopyMethod.SIMPLE:
        logger.debug("Returning SimpleTransferController")
        return SimpleTransferController(config)
    elif transfer_type == CopyMethod.CFS_TO_HPSS:
        logger.debug("Importing and returning CFSToHPSSTransferController")
        from orchestration.hpss import CFSToHPSSTransferController
        from orchestration.sfapi import create_sfapi_client
        return CFSToHPSSTransferController(
            client=create_sfapi_client(),
            config=config
        )
    elif transfer_type == CopyMethod.HPSS_TO_CFS:
        logger.debug("Importing and returning HPSSToCFSTransferController")
        from orchestration.hpss import HPSSToCFSTransferController
        from orchestration.sfapi import create_sfapi_client
        return HPSSToCFSTransferController(
            client=create_sfapi_client(),
            config=config
        )
    else:
        error_msg = f"Invalid transfer type: {transfer_type}"
        logger.error(error_msg)
        raise ValueError(error_msg)


if __name__ == "__main__":
    from orchestration.flows.bl832.config import Config832
    config = Config832()
    transfer_type = CopyMethod.GLOBUS
    globus_transfer_controller = get_transfer_controller(transfer_type, config)
    globus_transfer_controller.copy(
        file_path="dabramov/test.txt",
        source=config.alcf832_raw,
        destination=config.alcf832_scratch
    )

    simple_transfer_controller = get_transfer_controller(CopyMethod.SIMPLE, config)
    success = simple_transfer_controller.copy(
        file_path="test.rtf",
        source=FileSystemEndpoint("source", "/Users/david/Documents/copy_test/test_source/"),
        destination=FileSystemEndpoint("destination", "/Users/david/Documents/copy_test/test_destination/")
    )

    if success:
        logger.info("Simple transfer succeeded.")
    else:
        logger.error("Simple transfer failed.")
