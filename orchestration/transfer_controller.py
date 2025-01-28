from abc import ABC, abstractmethod
from dataclasses import dataclass
from dotenv import load_dotenv
from enum import Enum
import logging
import os
import time
from typing import Generic, TypeVar

import globus_sdk

from orchestration.flows.bl832.config import Config832
from orchestration.globus.transfer import GlobusEndpoint, start_transfer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()


class TransferEndpoint(ABC):
    """
    Abstract base class for endpoints.
    """
    def __init__(
        self,
        name: str,
        root_path: str
    ) -> None:
        self.name = name
        self.root_path = root_path

    def name(self) -> str:
        """
        A human-readable or reference name for the endpoint.
        """
        return self.name

    def root_path(self) -> str:
        """
        Root path or base directory for this endpoint.
        """
        return self.root_path


@dataclass
class FileSystemEndpoint(TransferEndpoint):
    """
    A file system endpoint.

    Args:
        TransferEndpoint: Abstract class for endpoints.
    """
    def __init__(
        self,
        name: str,
        root_path: str
    ) -> None:
        super().__init__(name, root_path)

    def full_path(
        self,
        path_suffix: str
    ) -> str:
        """
        Constructs the full path by appending the path_suffix to the root_path.

        Args:
            path_suffix (str): The relative path to append.

        Returns:
            str: The full absolute path.
        """
        if path_suffix.startswith("/"):
            path_suffix = path_suffix[1:]
        return f"{self.root_path.rstrip('/')}/{path_suffix}"


Endpoint = TypeVar("Endpoint", bound=TransferEndpoint)


class TransferController(Generic[Endpoint], ABC):
    """
    Abstract class for transferring data.

    Args:
        ABC: Abstract Base Class
    """
    def __init__(
        self,
        config: Config832
    ) -> None:
        self.config = config

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
            file_path (str): The path of the file to copy.
            source (Endpoint): The source endpoint.
            destination (Endpoint): The destination endpoint.

        Returns:
            bool: True if the transfer was successful, False otherwise.
        """
        pass


class GlobusTransferController(TransferController[GlobusEndpoint]):
    def __init__(
        self,
        config: Config832
    ) -> None:
        super().__init__(config)
    """
    Use Globus Transfer to move data between endpoints.

    Args:
        TransferController: Abstract class for transferring data.
    """
    def copy(
        self,
        file_path: str = None,
        source: GlobusEndpoint = None,
        destination: GlobusEndpoint = None,
    ) -> bool:
        """
        Copy a file from a source endpoint to a destination endpoint.

        Args:
            file_path (str): The path of the file to copy.
            source (GlobusEndpoint): The source endpoint.
            destination (GlobusEndpoint): The destination endpoint.
            transfer_client (TransferClient): The Globus transfer client.
        """

        logger.info(f"Transferring {file_path} from {source.name} to {destination.name}")

        if file_path[0] == "/":
            file_path = file_path[1:]

        source_path = os.path.join(source.root_path, file_path)
        dest_path = os.path.join(destination.root_path, file_path)
        logger.info(f"Transferring {source_path} to {dest_path}")
        # Start the timer
        start_time = time.time()
        success = False
        try:
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
            logger.error(f"Failed to submit transfer: {e}")
            return success
        finally:
            # Stop the timer and calculate the duration
            elapsed_time = time.time() - start_time
            logger.info(f"Transfer process took {elapsed_time:.2f} seconds.")
            return success


class SimpleTransferController(TransferController[FileSystemEndpoint]):
    def __init__(self, config: Config832) -> None:
        super().__init__(config)
    """
    Use a simple 'cp' command to move data within the same system.

    Args:
        TransferController: Abstract class for transferring data.
    """

    def copy(
        self,
        file_path: str = "",
        source: FileSystemEndpoint = None,
        destination: FileSystemEndpoint = None,
    ) -> bool:
        """
        Copy a file from a source endpoint to a destination endpoint using the 'cp' command.

        Args:
            file_path (str): The path of the file to copy.
            source (FileSystemEndpoint): The source endpoint.
            destination (FileSystemEndpoint): The destination endpoint.

        Returns:
            bool: True if the transfer was successful, False otherwise.
        """
        if not file_path:
            logger.error("No file_path provided.")
            return False
        if not source or not destination:
            logger.error("Source or destination endpoint not provided.")
            return False

        logger.info(f"Transferring {file_path} from {source.name} to {destination.name}")

        if file_path.startswith("/"):
            file_path = file_path[1:]

        source_path = os.path.join(source.root_path, file_path)
        dest_path = os.path.join(destination.root_path, file_path)
        logger.info(f"Transferring {source_path} to {dest_path}")

        # Start the timer
        start_time = time.time()

        try:
            result = os.system(f"cp -r '{source_path}' '{dest_path}'")
            if result == 0:
                logger.info("Transfer completed successfully.")
                return True
            else:
                logger.error(f"Transfer failed with exit code {result}.")
                return False
        except Exception as e:
            logger.error(f"Transfer failed: {e}")
            return False
        finally:
            # Stop the timer and calculate the duration
            elapsed_time = time.time() - start_time
            logger.info(f"Transfer process took {elapsed_time:.2f} seconds.")


class CopyMethod(Enum):
    """
    Enum representing different transfer methods.
    Use enum names as strings to identify transfer methods, ensuring a standard set of values.
    """
    GLOBUS = "globus"
    SIMPLE = "simple"


def get_transfer_controller(
    transfer_type: CopyMethod,
    config: Config832
) -> TransferController:
    """
    Get the appropriate transfer controller based on the transfer type.

    Args:
        transfer_type (str): The type of transfer to perform.
        config (Config832): The configuration object.

    Returns:
        TransferController: The transfer controller object.
    """
    if transfer_type == CopyMethod.GLOBUS:
        return GlobusTransferController(config)
    elif transfer_type == CopyMethod.SIMPLE:
        return SimpleTransferController(config)
    else:
        raise ValueError(f"Invalid transfer type: {transfer_type}")


if __name__ == "__main__":
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
