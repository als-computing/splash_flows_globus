import asyncio
import datetime
import logging

from prefect import get_run_logger, task
from prefect.client import get_client
from prefect.orion.utilities.schemas import DateTimeTZ
from prefect.orion.schemas.states import Scheduled


logger = logging.getLogger("orchestration.prefect")


async def schedule(
    deploymnent_name,
    flow_run_name,
    parameters,
    duration_from_now: datetime.timedelta,
    logger=logger,
):
    async with get_client() as client:
        deployment = await client.read_deployment_by_name(deploymnent_name)
        assert (
            deployment
        ), f"No deployment found in config for deploymnent_name {deploymnent_name}"
        date_time_tz = DateTimeTZ.now() + duration_from_now
        await client.create_flow_run_from_deployment(
            deployment.id,
            state=Scheduled(scheduled_time=date_time_tz),
            parameters=parameters,
            name=flow_run_name,
        )
    return


@task(name="Schedule Prefect Flow")
def schedule_prefect_flow(
    deploymnent_name, flow_run_name, parameters, duration_from_now: datetime.timedelta
):
    logger = get_run_logger()
    asyncio.run(
        schedule(deploymnent_name, flow_run_name, parameters, duration_from_now, logger)
    )
    return


if __name__ == "__main__":
    import dotenv

    dotenv.load_dotenv()

    asyncio.run(
        schedule_prefect_flow.fn(
            "test_832_transfers/test_transfers_832", "local test", None, 1
        )
    )
