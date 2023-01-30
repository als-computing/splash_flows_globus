import logging
import os

from prefect.client import OrionClient

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://prefect_server:4200/api")
API_SERVER_URL = os.getenv(
    "API_SERVER_URL", "http://data_orchestration_api:8000/orchestration/api/v1/jobs/"
)
API_KEY = os.getenv("API_KEY")

logger = logging.getLogger("orchestration.prefect")

async def launch_prefect_flow(config, job, logger=logger):
    try:
        client = OrionClient(PREFECT_API_URL)
        # get the deployment configured for this task
        for deployment in config['prefect']['deployments']:
            if job['type_spec'] == deployment['type_spec']:
                deployment_id = deployment['uuid']
                break
        if not deployment_id:
            raise KeyError(f"No deployment found in config for type_spec {job['type_spec']}")
        await client.create_flow_run_from_deployment(
            # @TODO this cam't be hardcoded
            deployment_id,
            parameters={"file_path": job['path'],
                        "job_id": job['id']})
    except Exception as e:
        logger.exception("Exception launching prefect flow")
