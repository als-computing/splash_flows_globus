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
        path: str,
        endpoint: GlobusEndpoint,
        transfer_client: Optional[globus_sdk.TransferClient] = None,
        recursive: bool = True
    ) -> int:
        """
        Get the size of a file or directory on a Globus endpoint.
        
        Args:
            path (str): Path to file or directory on the endpoint
            endpoint (GlobusEndpoint): The Globus endpoint
            transfer_client (TransferClient, optional): TransferClient instance
            recursive (bool): Include subdirectories in size calculation
        
        Returns:
            int: Size in bytes (0 if error)
        """
        if transfer_client is None:
            transfer_client = self.config.tc
            
        try:
            # Normalize path
            path = path[1:] if path.startswith('/') else path
            
            # Get the full Globus path
            globus_path = self._get_full_globus_path(path, endpoint)
            
            # Try to get item details - first see if it's a file or directory
            try:
                # First try direct stat if endpoint supports it
                item_info = transfer_client.operation_stat(endpoint.uuid, path=globus_path)
                is_dir = item_info['type'] == 'dir'
            except (globus_sdk.GlobusAPIError, KeyError):
                # Fallback: check parent directory listing
                parent_dir = os.path.dirname(path)
                basename = os.path.basename(path)
                
                # Handle case when path is root
                if not parent_dir and not basename:
                    is_dir = True
                else:
                    # Try to list parent directory
                    parent_globus_path = self._get_full_globus_path(parent_dir, endpoint)
                    try:
                        parent_listing = transfer_client.operation_ls(endpoint.uuid, path=parent_globus_path)
                        # Find our item
                        item = next((i for i in parent_listing if i['name'] == basename), None)
                        if not item:
                            logger.warning(f"Path '{basename}' not found in directory '{parent_globus_path}'")
                            return 0
                        is_dir = item['type'] == 'dir'
                    except globus_sdk.GlobusAPIError:
                        # If we can't list parent, try direct listing - if it works, it's a directory
                        try:
                            transfer_client.operation_ls(endpoint.uuid, path=globus_path)
                            is_dir = True
                        except globus_sdk.GlobusAPIError:
                            logger.error(f"Could not determine if {path} is file or directory")
                            return 0
            
            # Calculate size based on type
            if not is_dir:
                # It's a file, get its size
                try:
                    item_info = transfer_client.operation_stat(endpoint.uuid, path=globus_path)
                    return item_info.get('size', 0)
                except globus_sdk.GlobusAPIError:
                    try:
                        # Try listing parent directory as fallback
                        parent_dir = os.path.dirname(path)
                        basename = os.path.basename(path)
                        parent_globus_path = self._get_full_globus_path(parent_dir, endpoint)
                        
                        parent_listing = transfer_client.operation_ls(endpoint.uuid, path=parent_globus_path)
                        item = next((i for i in parent_listing if i['name'] == basename), None)
                        return item.get('size', 0) if item else 0
                    except:
                        logger.error(f"Failed to get file size for {path}")
                        return 0
            else:
                # It's a directory, calculate total size
                total_size = 0
                
                try:
                    # List directory contents
                    dir_listing = transfer_client.operation_ls(endpoint.uuid, path=globus_path)
                    
                    # Process all items
                    for item in dir_listing:
                        if item['type'] == 'file':
                            total_size += item.get('size', 0)
                        elif item['type'] == 'dir' and recursive:
                            # Recursively get subdirectory size
                            subdir_path = os.path.join(path, item['name'])
                            total_size += self.get_file_size(
                                subdir_path, endpoint, transfer_client, recursive
                            )
                    
                    return total_size
                except globus_sdk.GlobusAPIError as e:
                    logger.error(f"Failed to list directory {path}: {e}")
                    return 0
                
        except Exception as e:
            logger.error(f"Error getting path size: {e}")
            return 0
            
    def _get_full_globus_path(self, path: str, endpoint: GlobusEndpoint) -> str:
        """Helper to get full path on Globus endpoint"""
        if hasattr(endpoint, 'full_path'):
            return endpoint.full_path(path)
        elif hasattr(endpoint, 'root_path'):
            # Manually construct path with root_path
            if endpoint.root_path.endswith('/') and path.startswith('/'):
                path = path[1:]
            return os.path.join(endpoint.root_path, path)
        return path
    
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