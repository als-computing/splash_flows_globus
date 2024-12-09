from abc import ABC, abstractmethod
from dotenv import load_dotenv
from enum import Enum
import logging
from typing import Optional

from orchestration.flows.bl832.config import Config832
from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()


class TomographyHPCController(ABC):
    """
    Abstract class for tomography HPC controllers.
    Provides interface methods for reconstruction and building multi-resolution datasets.

    Args:
        ABC: Abstract Base Class
    """
    def __init__(
        self,
        Config832: Optional[Config832] = None
    ) -> None:
        pass

    @abstractmethod
    def reconstruct(
        self,
        file_path: str = "",
    ) -> bool:
        """Perform tomography reconstruction

        :param file_path: Path to the file to reconstruct.
        :return: True if successful, False otherwise.
        """
        pass

    @abstractmethod
    def build_multi_resolution(
        self,
        file_path: str = "",
    ) -> bool:
        """Generate multi-resolution version of reconstructed tomography

        :param file_path: Path to the file for which to build multi-resolution data.
        :return: True if successful, False otherwise.
        """
        pass


class ALCFTomographyHPCController(TomographyHPCController):
    """
    Implementation of TomographyHPCController for ALCF.
    Methods here leverage Globus Compute for processing tasks.

    Args:
        TomographyHPCController (ABC): Abstract class for tomography HPC controllers.
    """

    def __init__(self) -> None:
        pass

    def reconstruct(
        self,
        file_path: str = "",
    ) -> bool:

        # uses Globus Compute to reconstruct the tomography
        pass

    def build_multi_resolution(
        self,
        file_path: str = "",
    ) -> bool:
        # uses Globus Compute to build multi-resolution tomography

        pass


class HPC(Enum):
    """
    Each HPC enum member directly stores a callable that returns a TomographyHPCController.
    """
    ALCF = ("ALCF", lambda: ALCFTomographyHPCController())
    NERSC = ("NERSC", lambda: NERSCTomographyHPCController(
        client=NERSCTomographyHPCController.create_nersc_client()
    ))
    # Ex: add more HPCs here
    # OLCF = ("OLCF", lambda: OLCFTomographyHPCController())


def get_controller(
    hpc_type: str = None
) -> TomographyHPCController:
    """
    Factory function to retrieve the appropriate HPC controller instance based on the given HPC type.

    :param hpc_type: The type of HPC environment as a string, (e.g. 'ALCF' or 'NERSC').
    :return: An instance of TomographyHPCController for the given HPC environment.
    :raises ValueError: If an invalid HPC type is provided.
    """
    if not hpc_type:
        raise ValueError("No HPC type provided.")

    # Convert the string to uppercase and remove whitespace to avoid errors and validate hpc_type against HPC enum.
    hpc_enum = HPC(hpc_type.strip().upper())

    # Access hpc_enum.value directly. As defined, it should be directly callable.
    return hpc_enum.value()  # Call the stored class to get a new instance of the selected Controller.


def do_it_all() -> None:
    controller = get_controller("ALCF")
    controller.reconstruct()
    controller.build_multi_resolution()

    file_path = ""
    controller = get_controller("NERSC")
    controller.reconstruct(
        file_path=file_path,
    )
    controller.build_multi_resolution(
        file_path=file_path,
    )


if __name__ == "__main__":
    do_it_all()
    logger.info("Done.")
