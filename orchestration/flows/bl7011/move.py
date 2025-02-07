import logging
from prefect import flow

from orchestration.flows.bl7011.config import Config7011
from orchestration.transfer_controller import CopyMethod, get_transfer_controller

logger = logging.getLogger(__name__)


@flow(name="new_7011_file_flow")
def process_new_7011_file(
    file_path: str,
    config: Config7011
) -> None:
    """

    """

    logger.info(f"Processing new 733 file: {file_path}")

    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=Config7011()
    )

    transfer_controller.copy(
        file_path=file_path,
        source=config.data7011_raw,
        destination=config.nersc7011_alsdev_raw
    )
