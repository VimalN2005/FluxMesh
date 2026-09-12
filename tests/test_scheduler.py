import asyncio
import pytest
from fluxmesh.core.models import JobPriority, JobStatus, JobSubmission
from fluxmesh.scheduler.scheduler import FluxScheduler
from fluxmesh.storage.broker import FluxBroker


@pytest.mark.asyncio
async def test_delayed_job_promotion(broker: FluxBroker):
    scheduler = FluxScheduler(broker=broker, tick_interval=0.1)

    # Submit with 0.3s delay
    sub = JobSubmission(task="webhook_delivery", delay_seconds=0.3, priority=JobPriority.HIGH)
    job = await broker.submit_job(sub)

    assert job.status == JobStatus.SCHEDULED
    assert job.scheduled_at is not None

    # Immediate dequeue should be empty
    popped = await broker.dequeue("worker-1", ["critical", "high", "default", "low"])
    assert popped is None

    # Wait for delay to elapse and trigger tick
    await asyncio.sleep(0.4)
    await scheduler.tick()

    # Should now be ready in high priority queue
    popped = await broker.dequeue("worker-1", ["critical", "high", "default", "low"])
    assert popped is not None
    assert popped.id == job.id
    assert popped.task == "webhook_delivery"
