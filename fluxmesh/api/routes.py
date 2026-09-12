from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import HTMLResponse

from fluxmesh.api.dashboard import DASHBOARD_HTML
from fluxmesh.core.models import (
    ClusterMetrics,
    JobRecord,
    JobStatus,
    JobSubmission,
    WorkerHeartbeat,
)
from fluxmesh.storage.broker import FluxBroker

router = APIRouter()
_broker_instance: Optional[FluxBroker] = None


async def get_broker() -> FluxBroker:
    global _broker_instance
    if _broker_instance is None:
        _broker_instance = FluxBroker()
        await _broker_instance.connect()
    return _broker_instance


def set_broker(broker: FluxBroker) -> None:
    global _broker_instance
    _broker_instance = broker


@router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard_view():
    return HTMLResponse(content=DASHBOARD_HTML)


@router.post("/api/v1/jobs", response_model=JobRecord, status_code=status.HTTP_201_CREATED)
async def submit_job(submission: JobSubmission, broker: FluxBroker = Depends(get_broker)):
    return await broker.submit_job(submission)


@router.get("/api/v1/jobs/{job_id}", response_model=JobRecord)
async def get_job(job_id: str, broker: FluxBroker = Depends(get_broker)):
    record = await broker.get_job(job_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return record


@router.post("/api/v1/jobs/{job_id}/cancel")
async def cancel_job(job_id: str, broker: FluxBroker = Depends(get_broker)):
    cancelled = await broker.cancel_job(job_id)
    if not cancelled:
        raise HTTPException(status_code=400, detail="Cannot cancel job (either not found or already completed/failed).")
    return {"job_id": job_id, "status": "CANCELLED"}


@router.get("/api/v1/workers", response_model=List[WorkerHeartbeat])
async def list_workers(broker: FluxBroker = Depends(get_broker)):
    return await broker.get_active_workers()


@router.get("/api/v1/dlq", response_model=List[JobRecord])
async def list_dlq(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    broker: FluxBroker = Depends(get_broker),
):
    return await broker.get_dlq_jobs(limit=limit, offset=offset)


@router.post("/api/v1/dlq/{job_id}/replay")
async def replay_dlq(job_id: str, broker: FluxBroker = Depends(get_broker)):
    replayed = await broker.replay_dlq_job(job_id)
    if not replayed:
        raise HTTPException(status_code=400, detail="Job not in DLQ or not found.")
    return {"job_id": job_id, "status": "REPLAYED"}


@router.get("/api/v1/metrics", response_model=ClusterMetrics)
async def get_metrics(broker: FluxBroker = Depends(get_broker)):
    return await broker.get_metrics()
