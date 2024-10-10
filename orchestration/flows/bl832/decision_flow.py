from prefect import flow, task, get_run_logger
from prefect.blocks.system import JSON
from prefect.deployments.deployments import run_deployment


@task(name="setup_decision_settings")
def setup_decision_settings(alcf_recon: bool,
                            nersc_recon: bool,
                            nersc_move: bool) -> dict:
    """
    This task is used to define the settings for the decision making process of the BL832 beamline.
    """
    logger = get_run_logger()
    logger.info(f"Setting up decision settings: alcf_recon={alcf_recon}, "
                f"nersc_recon={nersc_recon}, nersc_move={nersc_move}")
    settings = {
        "process_new_832_ALCF_flow": alcf_recon,
        "nersc_recon": nersc_recon,
        "new_file_832": nersc_move
    }
    settings_json = JSON(value=settings)
    settings_json.save(name="decision-settings", overwrite=True)
    return settings


@task(name="run_specific_flow")
def run_specific_flow(flow_name: str) -> None:
    """
    This task is used to run a specific flow.
    """
    logger = get_run_logger()
    logger.info(f"Running {flow_name}")
    run_deployment(flow_name)


@flow(name="decision_flow")
def decision_flow():
    """
    This flow is used to define the decision making process of the BL832 beamline.
    """
    logger = get_run_logger()
    logger.info("Starting decision flow")
    decision_settings = JSON.load("decision-settings")
    for flow_name, run_flow in decision_settings.value.items():
        print(f"{flow_name}")
        print(f"{run_flow}")
        if run_flow:
            run_specific_flow(flow_name)
        else:
            logger.info(f"Skipping {flow_name}")


if __name__ == "__main__":
    """
    This script is used to define the flow for the decision making process of the BL832 beamline.
    """
    setup_decision_settings(alcf_recon=True, nersc_recon=False, nersc_move=True)
    decision_flow()
