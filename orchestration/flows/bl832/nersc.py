from dotenv import load_dotenv
import logging
# import os
# from pathlib import Path
from prefect import flow
# from typing import Optional

# from orchestration.flows.bl832.config import Config832
from orchestration.flows.bl832.job_controller import get_controller
# from orchestration.nersc import NerscClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()


@flow(name="nersc_recon_flow")
def nersc_recon_flow(
    file_path: str,
) -> bool:
    """
    Perform tomography reconstruction on NERSC.

    :param file_path: Path to the file to reconstruct.
    """

    # To do: Implement file transfers, pruning, and other necessary steps

    controller = get_controller("NERSC")
    nersc_reconstruction_success = controller.reconstruct(
        file_path=file_path,
    )
    nersc_multi_res_success = controller.build_multi_resolution(
        file_path=file_path,
    )

    if nersc_reconstruction_success and nersc_multi_res_success:
        return True
    else:
        return False


if __name__ == "__main__":
    nersc_recon_flow(file_path="dabramov/20230606_151124_jong-seto_fungal-mycelia_roll-AQ_fungi1_fast.h5")
