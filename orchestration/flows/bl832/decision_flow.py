import asyncio
from prefect import flow, task, get_run_logger
from prefect.blocks.system import JSON
from prefect.deployments.deployments import run_deployment
from pydantic import BaseModel, ValidationError
from typing import Any, Union


class FlowParameterMapper:
    """
    Class to define and map the parameters required for each flow.
    """
    flow_parameters = {
        # From alcf.py
        "new_832_ALCF_flow/process_new_832_ALCF_flow": [
            "folder_name",
            "file_name",
            "is_export_control",
            "send_to_alcf"],
        # From move.py
        "new_832_file_flow/new_file_832": [
            "file_path",
            "is_export_control",
            "send_to_nersc",
            "config"],
        # Placeholder parameters for NERSC reconstruction
        "nersc_recon/nersc_recon": [
            "file_path",
            "config",
            "send_to_nersc"]  # Placeholder parameters for NERSC reconstruction
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
    file_path: str
    is_export_control: bool
    send_to_nersc: bool
    config: Union[dict, Any]
    send_to_alcf: bool
    folder_name: str
    file_name: str


@task(name="setup_decision_settings")
def setup_decision_settings(alcf_recon: bool, nersc_recon: bool, nersc_move: bool) -> dict:
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
                    f"nersc_recon={nersc_recon}, nersc_move={nersc_move}")
        # Define which flows to run based on the input settings
        settings = {
            "new_832_ALCF_flow/process_new_832_ALCF_flow": alcf_recon,
            "nersc_recon/nersc_recon": nersc_recon,  # This is a placeholder for the NERSC reconstruction flow
            "new_832_file_flow/new_file_832": nersc_move
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


@flow(name="decision_flow")
async def decision_flow(
    file_path: str,
    is_export_control: bool,
    send_to_nersc: bool,
    config: Union[dict, Any],
    send_to_alcf: bool,
    folder_name: str,
    file_name: str
) -> None:
    """
    This flow reads the decision settings and launches tasks accordingly.
    It uses settings defined earlier to decide which sub-flows to execute.

    :param file_path: Path to the file to be processed.
    :param is_export_control: Boolean indicating whether the file is under export control.
    :param send_to_nersc: Boolean indicating whether to send data to NERSC.
    :param config: Configuration dictionary for the flow.
    :param send_to_alcf: Boolean indicating whether to send data to ALCF.
    :param folder_name: Name of the folder containing the files.
    :param file_name: Name of the file to be processed.
    """
    logger = get_run_logger()
    try:
        # Validate input parameters using pydantic model
        inputs = DecisionFlowInputModel(
            file_path=file_path,
            is_export_control=is_export_control,
            send_to_nersc=send_to_nersc,
            config=config,
            send_to_alcf=send_to_alcf,
            folder_name=folder_name,
            file_name=file_name
        )
    except ValidationError as e:
        logger.error(f"Invalid input parameters: {e}")
        raise

    try:
        logger.info("Starting decision flow")
        # Load the decision settings that were previously saved
        decision_settings = await JSON.load("decision-settings")
    except Exception as e:
        logger.error(f"Failed to load decision settings: {e}")
        raise

    # Create a dictionary of all available parameters for potential use in sub-flows
    available_params = inputs.dict()

    tasks = []
    # Iterate over each flow specified in the decision settings
    for flow_name, run_flow in decision_settings.value.items():
        try:
            logger.info(f"Evaluating flow: {flow_name}, run: {run_flow}")
            # If the decision setting indicates to run the flow
            if run_flow:
                # Get the required parameters for the flow using FlowParameterMapper
                flow_params = FlowParameterMapper.get_flow_parameters(flow_name, available_params)
                # Append the task to the list of tasks to run
                tasks.append(run_specific_flow(flow_name, flow_params))
            else:
                logger.info(f"Skipping {flow_name}")
        except KeyError as e:
            logger.error(f"Missing required parameter for flow {flow_name}: {e}")
            raise
        except ValueError as e:
            logger.error(f"Flow name not found: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while preparing flow {flow_name}: {e}")
            raise

    # Execute all the tasks concurrently, if there are any
    if tasks:
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Failed to run one or more tasks: {e}")
            raise
    else:
        logger.info("No tasks to run")

    return None

if __name__ == "__main__":
    """
    This script defines the flow for the decision making process of the BL832 beamline.
    It first sets up the decision settings, then executes the decision flow to run specific sub-flows as needed.
    """
    try:
        # Setup decision settings based on input parameters
        setup_decision_settings(alcf_recon=True, nersc_recon=False, nersc_move=True)
        # Run the main decision flow with the specified parameters
        asyncio.run(decision_flow(file_path="/path/to/file",
                                  is_export_control=False,
                                  send_to_nersc=True,
                                  config={},
                                  send_to_alcf=True,
                                  folder_name="folder",
                                  file_name="file"))
    except Exception as e:
        logger = get_run_logger()
        logger.error(f"Failed to execute main flow: {e}")
