from abc import ABC, abstractmethod
from dotenv import load_dotenv
from enum import Enum
import datetime
import logging
import os
import time
from typing import Generic, TypeVar, Optional

import globus_sdk

from orchestration.flows.bl832.config import Config832
from orchestration.globus.transfer import GlobusEndpoint, start_transfer
from orchestration.prometheus_utils import push_metrics_to_prometheus

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
        collect_metrics: bool = True,
        machine_name: str = "default"
    ) -> bool:
        """
        Copy a file from a source endpoint to a destination endpoint.

        Args:
            file_path (str): The path of the file to copy.
            source (Endpoint): The source endpoint.
            destination (Endpoint): The destination endpoint.
            collect_metrics (bool): Whether to collect and push metrics to Prometheus.
            machine_name (str): The name of the machine for metrics labeling.

        Returns:
            bool: True if the transfer was successful, False otherwise.
        """
        pass


class GlobusTransferController(TransferController[GlobusEndpoint]):
    """
    Use Globus Transfer to move data between endpoints.

    Args:
        TransferController: Abstract class for transferring data.
    """
    def __init__(
        self,
        config: Config832
    ) -> None:
        super().__init__(config)
        
    def get_file_size(
        self,
        file_path: str,
        endpoint: GlobusEndpoint,
        transfer_client: Optional[globus_sdk.TransferClient] = None
    ) -> int:
        """
        Get the size of a file on a Globus endpoint.
        
        Args:
            file_path (str): The path of the file on the endpoint.
            endpoint (GlobusEndpoint): The Globus endpoint containing the file.
            transfer_client (TransferClient, optional): A pre-configured TransferClient.
                If None, uses self.config.tc.
        
        Returns:
            int: The file size in bytes, or 0 if unable to determine.
        """
        if transfer_client is None:
            transfer_client = self.config.tc
            
        try:
            # Strip leading slash if present
            if file_path.startswith('/'):
                file_path = file_path[1:]
                
            # Split the file path into directory and filename
            directory_path = os.path.dirname(file_path)
            filename = os.path.basename(file_path)
            
            # Log the actual paths we're using
            logger.info(f"Getting file size for: {filename} in directory: {directory_path} on endpoint {endpoint.name} ({endpoint.uuid})")
            
            # For paths that start with 'raw', make sure we use the correct full path
            # This is the exact path in the endpoint, not relative to root_path
            globus_dir_path = directory_path
            
            # If endpoint has a root_path and the directory doesn't include it, we need the full path
            # but ONLY if the endpoint doesn't automatically handle root_path in its configuration
            if hasattr(endpoint, 'full_path'):
                # Use full_path method if available (safer)
                globus_dir_path = endpoint.full_path(directory_path)
            else:
                # Otherwise manually construct, but be careful about double-slashes
                # root_path already includes the full absolute path on the endpoint
                # Make sure there's no double-slash when joining paths
                if endpoint.root_path.endswith('/') and directory_path.startswith('/'):
                    directory_path = directory_path[1:]
                globus_dir_path = os.path.join(endpoint.root_path, directory_path)
            
            logger.info(f"Listing directory on Globus endpoint: {globus_dir_path}")
            
            # Try listing the directory
            try:
                response = transfer_client.operation_ls(endpoint.uuid, path=globus_dir_path)
            except globus_sdk.GlobusAPIError as e:
                logger.error(f"First attempt failed: {e}")
                
                # Try with just raw directory_path as a fallback
                logger.info(f"Trying fallback with direct path: {directory_path}")
                try:
                    response = transfer_client.operation_ls(endpoint.uuid, path=directory_path)
                except globus_sdk.GlobusAPIError as second_e:
                    logger.error(f"Second attempt also failed: {second_e}")
                    
                    # Try with just a bare path as a last resort
                    if directory_path.startswith('raw/'):
                        bare_path = directory_path
                    else:
                        bare_path = f"/{directory_path}"
                        
                    logger.info(f"Trying last resort with bare path: {bare_path}")
                    response = transfer_client.operation_ls(endpoint.uuid, path=bare_path)
            
            # Find the file in the response and get its size
            for item in response:
                if item['name'] == filename:
                    logger.info(f"Found file: {filename}, size: {item['size']} bytes")
                    return item['size']
            
            # If the file wasn't found after looking through the directory
            logger.warning(f"File '{filename}' not found in directory listing of '{globus_dir_path}'")
            return 0
            
        except globus_sdk.GlobusAPIError as e:
            logger.error(f"Globus API error when getting file size: {e}")
            logger.error(f"Failed to get size for {file_path} on endpoint {endpoint.name}")
            return 0
        except Exception as e:
            logger.error(f"Error getting file size: {e}")
            return 0

    def collect_and_push_metrics(
        self,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        file_path: str,
        source: GlobusEndpoint,
        destination: GlobusEndpoint,
        file_size: int,
        success: bool,
        machine_name: str
    ) -> None:
        """
        Collect transfer metrics and push them to Prometheus.
        
        Args:
            start_time (datetime.datetime): Transfer start time.
            end_time (datetime.datetime): Transfer end time.
            file_path (str): The path of the transferred file.
            source (GlobusEndpoint): The source endpoint.
            destination (GlobusEndpoint): The destination endpoint.
            file_size (int): Size of the transferred file in bytes.
            success (bool): Whether the transfer was successful.
            machine_name (str): Name of the machine for metrics labeling.
        """
        try:
            # Convert datetime objects to ISO format strings
            start_timestamp = start_time.isoformat()
            end_timestamp = end_time.isoformat()
            
            # Calculate duration in seconds
            duration_seconds = (end_time - start_time).total_seconds()
            
            # Calculate transfer speed (bytes per second)
            transfer_speed = file_size / duration_seconds if duration_seconds > 0 and file_size > 0 else 0
            
            # Prepare metrics dictionary
            metrics = {
                "timestamp": start_timestamp,
                "end_timestamp": end_timestamp,
                "local_path": os.path.join(source.root_path, file_path),
                "remote_path": os.path.join(destination.root_path, file_path),
                "bytes_transferred": file_size,
                "duration_seconds": duration_seconds,
                "transfer_speed": transfer_speed,
                "status": "success" if success else "failed",
                "machine": machine_name
            }
            
            # Push metrics to Prometheus
            push_metrics_to_prometheus(metrics, logger)
            
        except Exception as e:
            logger.error(f"Error collecting or pushing metrics: {e}")

    def copy(
        self,
        file_path: str = None,
        source: GlobusEndpoint = None,
        destination: GlobusEndpoint = None,
        collect_metrics: bool = True,
        machine_name: str = "nersc"
    ) -> bool:
        """
        Copy a file from a source endpoint to a destination endpoint.

        Args:
            file_path (str): The path of the file to copy.
            source (GlobusEndpoint): The source endpoint.
            destination (GlobusEndpoint): The destination endpoint.
            collect_metrics (bool): Whether to collect and push metrics to Prometheus.
            machine_name (str): The name of the machine for metrics labeling.
            
        Returns:
            bool: True if the transfer was successful, False otherwise.
        """
        if not file_path:
            logger.error("No file_path provided")
            return False
            
        if not source or not destination:
            logger.error("Source or destination endpoint not provided")
            return False

        logger.info(f"Transferring {file_path} from {source.name} to {destination.name}")

        # Remove leading slash if present
        if file_path[0] == "/":
            file_path = file_path[1:]

        # Record start time for metrics if collecting metrics
        start_time = datetime.datetime.now()
        
        # Get file size before transfer if collecting metrics
        file_size = 0
        if collect_metrics:
            file_size = self.get_file_size(file_path, source)
            logger.info(f"File size: {file_size} bytes")

        source_path = os.path.join(source.root_path, file_path)
        dest_path = os.path.join(destination.root_path, file_path)
        logger.info(f"Transferring {source_path} to {dest_path}")
        logger.info(f"File size: {file_size} bytes")

        # Start the timer
        transfer_start_time = time.time()
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
                
        except globus_sdk.services.transfer.errors.TransferAPIError as e:
            logger.error(f"Failed to submit transfer: {e}")
            
        finally:
            # Stop the timer and calculate the duration
            transfer_end_time = time.time()
            elapsed_time = transfer_end_time - transfer_start_time
            transfer_rate = file_size / elapsed_time if elapsed_time > 0 and file_size > 0 else 0
            
            logger.info(f"Transfer process took {elapsed_time:.2f} seconds.")
            logger.info(f"Transfer rate: {transfer_rate:.2f} bytes/second")
            
            # Collect and push metrics if enabled
            if collect_metrics:
                end_time = datetime.datetime.now()
                self.collect_and_push_metrics(
                    start_time=start_time,
                    end_time=end_time,
                    file_path=file_path,
                    source=source,
                    destination=destination,
                    file_size=file_size,
                    success=success,
                    machine_name=machine_name
                )
                
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
        collect_metrics: bool = False,
        machine_name: str = "local"
    ) -> bool:
        """
        Copy a file from a source endpoint to a destination endpoint using the 'cp' command.

        Args:
            file_path (str): The path of the file to copy.
            source (FileSystemEndpoint): The source endpoint.
            destination (FileSystemEndpoint): The destination endpoint.
            collect_metrics (bool): Whether to collect and push metrics to Prometheus.
            machine_name (str): The name of the machine for metrics labeling.

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