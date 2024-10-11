import asyncio
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
    # Deployments can be called using the form "flow_name/deployment_name"
    settings = {
        "new_832_ALCF_flow/process_new_832_ALCF_flow": alcf_recon,
        "nersc_recon/nersc_recon": nersc_recon,  # This is a placeholder for the NERSC reconstruction flow
        "new_832_file_flow/new_file_832": nersc_move
    }
    settings_json = JSON(value=settings)
    settings_json.save(name="decision-settings", overwrite=True)
    return settings


@task(name="run_specific_flow")
async def run_specific_flow(flow_name: str) -> None:
    """
    This task is used to run a specific flow.
    """
    logger = get_run_logger()
    logger.info(f"Running {flow_name}")
    await run_deployment(flow_name)


@flow(name="decision_flow")
async def decision_flow(
    file_path: str,
    is_export_control: bool,
    send_to_nersc: bool,
    config: dict,
    send_to_alcf: bool,
    folder_name: str,
    file_name: str
):
    """
    This flow reads the decision settings and launches tasks accordingly.
    """
    logger = get_run_logger()
    logger.info("Starting decision flow")
    decision_settings = await JSON.load("decision-settings")

    tasks = []
    for flow_name, run_flow in decision_settings.value.items():
        logger.info(f"{flow_name}")
        logger.info(f"{run_flow}")
        if run_flow:
            tasks.append(run_specific_flow(flow_name))
        else:
            logger.info(f"Skipping {flow_name}")

    if tasks:
        await asyncio.gather(*tasks)
    else:
        logger.info("No tasks to run")


if __name__ == "__main__":
    """
    This script defines the flow for the decision making process of the BL832 beamline.
    """
    setup_decision_settings(alcf_recon=True, nersc_recon=False, nersc_move=True)
    asyncio.run(decision_flow())
