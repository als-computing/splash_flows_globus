import asyncio
from prefect import flow, task, get_run_logger
from prefect.blocks.system import JSON
from prefect.deployments.deployments import run_deployment
from pydantic import BaseModel, ValidationError, Field
from typing import Any, Optional, Union


class FlowParameterMapper:
    """
    Class to define and map the parameters required for each flow.
    """
    flow_parameters = {
        # From alcf.py
        "alcf_recon_flow/alcf_recon_flow": [
            "file_path",
            "is_export_control",
            "config"],
        # From move.py
        "new_832_file_flow/new_file_832": [
            "file_path",
            "is_export_control",
            "config"],
        # From nersc.py
        "nersc_recon_flow/nersc_recon_flow": [
            "file_path",
            "is_export_control",
            "config"]  # Placeholder parameters for NERSC reconstruction
    }

    @classmethod
    def get_flow_parameters(cls, flow_name: str, available_params: dict) -> dict:
        """
        Get a dictionary of parameters required for a specific flow based on available parameters.

        :param flow_name: Name of the flow to get parameters for.
        :param available_params: Dictionary of all available parameters.
        :return: Dictionary of parameters for the flow.
        """
        # Get the list of required parameters for the specified flow
        required_params = cls.flow_parameters.get(flow_name)
        if required_params is None:
            raise ValueError(f"Flow name '{flow_name}' not found in flow parameters mapping.")
        # Filter and return only those parameters that are available in the provided dictionary
        return {param: available_params[param] for param in required_params if param in available_params}


class DecisionFlowInputModel(BaseModel):
    """
    Pydantic model to validate input parameters for the decision flow.
    """
    file_path: Optional[str] = Field(default=None)
    is_export_control: Optional[bool] = Field(default=False)
    config: Optional[Union[dict, Any]] = Field(default_factory=dict)


@task(name="setup_decision_settings")
def setup_decision_settings(alcf_recon: bool, nersc_recon: bool, new_file_832: bool) -> dict:
    """
    This task is used to define the settings for the decision making process of the BL832 beamline.

    :param alcf_recon: Boolean indicating whether to run the ALCF reconstruction flow.
    :param nersc_recon: Boolean indicating whether to run the NERSC reconstruction flow.
    :param nersc_move: Boolean indicating whether to move files to NERSC.
    :return: A dictionary containing the settings for each flow.
    """
    logger = get_run_logger()
    try:
        logger.info(f"Setting up decision settings: alcf_recon={alcf_recon}, "
                    f"nersc_recon={nersc_recon}, new_file_832={new_file_832}")
        # Define which flows to run based on the input settings
        settings = {
            "alcf_recon_flow/alcf_recon_flow": alcf_recon,
            "nersc_recon_flow/nersc_recon_flow": nersc_recon,
            "new_832_file_flow/new_file_832": new_file_832
        }
        # Save the settings in a JSON block for later retrieval by other flows
        settings_json = JSON(value=settings)
        settings_json.save(name="decision-settings", overwrite=True)
    except Exception as e:
        logger.error(f"Failed to set up decision settings: {e}")
        raise
    return settings


@task(name="run_specific_flow")
async def run_specific_flow(flow_name: str, parameters: dict) -> None:
    """
    This task is used to run a specific flow with dynamically provided parameters.

    :param flow_name: Name of the flow to run.
    :param parameters: Dictionary of parameters to pass to the flow.
    """
    logger = get_run_logger()
    try:
        logger.info(f"Running {flow_name} with parameters: {parameters}")
        # Run the specified flow deployment with the provided parameters
        await run_deployment(name=flow_name, parameters=parameters)
    except Exception as e:
        logger.error(f"Failed to run flow {flow_name}: {e}")
        raise


@flow(name="dispatcher")
async def dispatcher(
    file_path: Optional[str] = None,
    is_export_control: bool = False,
    config: Optional[Union[dict, Any]] = None,
) -> None:
    """
    Dispatcher flow that reads decision settings and launches tasks accordingly.
    """
    logger = get_run_logger()
    try:
        inputs = DecisionFlowInputModel(
            file_path=file_path,
            is_export_control=is_export_control,
            config=config,
        )
    except ValidationError as e:
        logger.error(f"Invalid input parameters: {e}")
        raise

    # Run new_file_832 first (synchronously)
    available_params = inputs.dict()
    try:
        decision_settings = await JSON.load("decision-settings")
        if decision_settings.value.get("new_832_file_flow/new_file_832"):
            logger.info("Running new_file_832 flow...")
            await run_specific_flow("new_832_file_flow/new_file_832",
                                    FlowParameterMapper.get_flow_parameters(
                                        "new_832_file_flow/new_file_832",
                                        available_params))
            logger.info("Completed new_file_832 flow.")
    except Exception as e:
        logger.error(f"new_832_file_flow/new_file_832 flow failed: {e}")
        # Optionally, raise a specific ValueError
        raise ValueError("new_file_832 flow Failed") from e

    # Prepare ALCF and NERSC flows to run asynchronously, based on settings
    tasks = []
    if decision_settings.value.get("alcf_recon_flow/alcf_recon_flow"):
        alcf_params = FlowParameterMapper.get_flow_parameters("alcf_recon_flow/alcf_recon_flow", available_params)
        tasks.append(run_specific_flow("alcf_recon_flow/alcf_recon_flow", alcf_params))

    if decision_settings.value.get("nersc_recon_flow/nersc_recon_flow"):
        nersc_params = FlowParameterMapper.get_flow_parameters("nersc_recon_flow/nersc_recon_flow", available_params)
        tasks.append(run_specific_flow("nersc_recon_flow/nersc_recon_flow", nersc_params))

    # Run ALCF and NERSC flows in parallel, if any
    if tasks:
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Failed to run one or more tasks: {e}")
            raise
    else:
        logger.info("No ALCF or NERSC tasks to run based on decision settings.")

    return None


if __name__ == "__main__":
    """
    This script defines the flow for the decision making process of the BL832 beamline.
    It first sets up the decision settings, then executes the decision flow to run specific sub-flows as needed.
    """
    try:
        # Setup decision settings based on input parameters
        setup_decision_settings(alcf_recon=True, nersc_recon=False, new_file_832=False)
        # Run the main decision flow with the specified parameters
        # asyncio.run(dispatcher(
        #     config={},  # PYTEST, ALCF, NERSC
        #     is_export_control=False,  # ALCF & MOVE
        #     folder_name="folder",  # ALCF
        #     file_name="file",  # ALCF
        #     file_path="/path/to/file",  # MOVE
        #     send_to_alcf=True,  # ALCF
        #     send_to_nersc=True,  # MOVE
        #     )
        # )
    except Exception as e:
        logger = get_run_logger()
        logger.error(f"Failed to execute main flow: {e}")
