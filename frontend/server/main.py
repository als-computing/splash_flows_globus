import uuid
from datetime import timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowFilter,
    FlowFilterName,
    FlowRunFilter,
    FlowRunFilterState,
    FlowRunFilterStateType,
)
from prefect.client.schemas.objects import State, StateType
from prefect.client.schemas.responses import SetStateStatus
from prefect.exceptions import PrefectException
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    DEPLOYMENT_ID: uuid.UUID = uuid.UUID("68fdb97e-72f1-49ff-b249-3e7464853f72")


cfg = Settings()


class FlowRequest(BaseModel):
    walltime_minutes: int


class FlowResponse(BaseModel):
    flow_run_id: uuid.UUID


class CancelFlowResponse(BaseModel):
    message: Optional[str] = None


@app.post("/flows/launch", response_model=FlowResponse)
async def launch_flow(request: FlowRequest):
    try:
        async with get_client() as client:
            # Create flow run with parameters
            flow_run = await client.create_flow_run_from_deployment(
                deployment_id=cfg.DEPLOYMENT_ID,
                parameters={"walltime": timedelta(minutes=request.walltime_minutes)},
            )
            return FlowResponse(flow_run_id=flow_run.id)
    except PrefectException as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@app.delete("/flows/{flow_run_id}", response_model=CancelFlowResponse)
async def cancel_flow(flow_run_id: uuid.UUID):
    try:
        async with get_client() as client:
            state = State(type=StateType.CANCELLING)
            result = await client.set_flow_run_state(
                flow_run_id=flow_run_id, state=state
            )

            if result.status == SetStateStatus.ABORT:
                raise HTTPException(
                    status_code=500,
                    detail=(
                        f"Flow run '{flow_run_id}' was unable to be cancelled. "
                        f"Reason: {result.details.reason}"
                    ),
                )

            return CancelFlowResponse(
                message=f"Flow run '{flow_run_id}' was successfully cancelled."
            )
    except PrefectException as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


FLOW_NAME = "nersc_streaming_flow"


class FlowRunInfo(BaseModel):
    id: uuid.UUID
    state: StateType | None = None


class FlowRunsResponse(BaseModel):
    flow_run_infos: list[FlowRunInfo]


@app.get("/flows/running", response_model=FlowRunsResponse)
async def get_running_flows():
    try:
        async with get_client() as client:
            # Create a flow filter to filter by flow name
            flow_filter = FlowFilter(name=FlowFilterName(any_=[FLOW_NAME]))
            flow_run_filter = FlowRunFilter(
                state=FlowRunFilterState(
                    type=FlowRunFilterStateType(
                        any_=[
                            StateType.SCHEDULED,
                            StateType.PENDING,
                            StateType.RUNNING,
                            StateType.CANCELLING,
                        ]
                    )
                )
            )

            # Get flow runs using the filter
            flow_runs = await client.read_flow_runs(
                flow_filter=flow_filter, flow_run_filter=flow_run_filter
            )

            # Extract flow run IDs and states
            flow_run_infos = [
                FlowRunInfo(id=flow_run.id, state=flow_run.state.type)
                for flow_run in flow_runs
            ]

            return FlowRunsResponse(flow_run_infos=flow_run_infos)
    except PrefectException as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
