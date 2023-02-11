import datetime
import logging

import pendulum
from prefect import get_run_logger, task
from prefect.client import get_client
from prefect.orion.utilities.schemas import DateTimeTZ
from prefect.orion.schemas.states import Scheduled



logger = logging.getLogger("orchestration.prefect")

@task(name="Schedule Prefect Flow")
async def schedule_prefect_flow(deploymnent_name, flow_run_name, parameters, duration_from_now: datetime.timedelta):
    logger = get_run_logger()
    try:
        async with get_client() as client:
            deployment= await client.read_deployment_by_name(deploymnent_name)
            assert deployment, f"No deployment found in config for deploymnent_name {deploymnent_name}"
            date_time_tz = DateTimeTZ.now() + duration_from_now
            flow_run = await client.create_flow_run_from_deployment(
                deployment.id,
                state=Scheduled(scheduled_time=date_time_tz),
                parameters=parameters,
                name=flow_run_name)
    except Exception as e:
        logger.exception("Exception launching prefect flow")


if __name__ == "__main__":
    import asyncio
    import dotenv

    dotenv.load_dotenv()
    

    asyncio.run(
        schedule_prefect_flow.fn('test_832_transfers/test_transfers_832', "local test", None, 1)
    )