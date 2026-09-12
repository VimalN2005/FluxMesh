import pytest
from fluxmesh.core.models import JobPriority, JobStatus, JobSubmission
from fluxmesh.storage.broker import FluxBroker


@pytest.mark.asyncio
async def test_job_idempotency_deduplication(broker: FluxBroker):
    key = "unique-charge-event-12345"
    sub1 = JobSubmission(task="webhook_delivery", kwargs={"url": "https://api.test/webhook"}, idempotency_key=key)
    sub2 = JobSubmission(task="webhook_delivery", kwargs={"url": "https://api.test/webhook"}, idempotency_key=key)

    job1 = await broker.submit_job(sub1)
    job2 = await broker.submit_job(sub2)

    # Identical IDs returned due to deduplication
    assert job1.id == job2.id

    # Queue contains only one entry
    popped1 = await broker.dequeue("worker-1", ["default"])
    assert popped1 is not None and popped1.id == job1.id

    popped2 = await broker.dequeue("worker-1", ["default"])
    assert popped2 is None


@pytest.mark.asyncio
async def test_job_cancellation(broker: FluxBroker):
    sub = JobSubmission(task="batch_process", priority=JobPriority.LOW)
    job = await broker.submit_job(sub)

    # Cancel pending job
    cancelled = await broker.cancel_job(job.id)
    assert cancelled is True

    record = await broker.get_job(job.id)
    assert record.status == JobStatus.CANCELLED

    # Dequeue must not return cancelled job
    popped = await broker.dequeue("worker-1", ["low"])
    assert popped is None
