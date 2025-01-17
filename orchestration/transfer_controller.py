from abc import ABC, abstractmethod
from dataclasses import dataclass
from dotenv import load_dotenv
from enum import Enum
import logging
import os
import time
from typing import Generic, Protocol, TypeVar

import globus_sdk

from orchestration.flows.bl832.config import Config832
from orchestration.globus.transfer import GlobusEndpoint, start_transfer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()


Endpoint = TypeVar("Endpoint", bound=GlobusEndpoint)


class BaseEndpoint(Protocol):
    """
    A protocol or abstract interface that all endpoints must implement or satisfy.
    """

    @property
    @abstractmethod
    def root_path(self) -> str:
        """
        Root path or base directory for this endpoint.
        """
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """
        A human-readable or reference name for the endpoint.
        """
        ...


@dataclass
class FileSystemEndpoint(BaseEndpoint):
    root_path: str
    name: str = "local"

    def full_path(self, path_suffix: str) -> str:
        if path_suffix.startswith("/"):
            path_suffix = path_suffix[1:]
        return f"{self.root_path.rstrip('/')}/{path_suffix}"


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
        pass


class GlobusTransfer(TransferController[GlobusEndpoint]):
    def __init__(
        self,
        config: Config832
    ) -> None:
        self.config = config
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
            return False
        finally:
            # Stop the timer and calculate the duration
            elapsed_time = time.time() - start_time
            logger.info(f"Transfer process took {elapsed_time:.2f} seconds.")
            return success


class SimpleTransfer(TransferController[FileSystemEndpoint]):
    def __init__(
        self,
        config: Config832
    ) -> None:
        self.config = config
    """
    Use a simple 'cp' command to move data within the same system.

    Args:
        TransferController: Abstract class for transferring data.
    """

    def copy(
        self,
        file_path: str = "",
        source: FileSystemEndpoint = "",
        destination: FileSystemEndpoint = "",
    ) -> bool:

        logger.info(f"Transferring {file_path} from {source} to {destination}")

        if file_path[0] == "/":
            file_path = file_path[1:]

        source_path = os.path.join(source, file_path)
        dest_path = os.path.join(destination, file_path)
        logger.info(f"Transferring {source_path} to {dest_path}")
        # Start the timer
        start_time = time.time()

        try:
            os.system(f"cp -r {source_path} {dest_path}")
            logger.info("Transfer completed successfully.")
            return True
        except Exception as e:
            logger.error(f"Transfer failed: {e}")
            return False
        finally:
            # Stop the timer and calculate the duration
            elapsed_time = time.time() - start_time
            logger.info(f"Transfer process took {elapsed_time:.2f} seconds.")
            return True


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
        return GlobusTransfer(config)
    elif transfer_type == CopyMethod.SIMPLE:
        return SimpleTransfer(config)
    else:
        raise ValueError(f"Invalid transfer type: {transfer_type}")


def main():
    config = Config832()
    transfer_type = CopyMethod.GLOBUS
    controller = get_transfer_controller(transfer_type, config)
    controller
