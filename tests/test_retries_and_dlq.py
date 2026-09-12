import asyncio
import pytest
from fluxmesh.core.models import JobPriority, JobStatus, JobSubmission
from fluxmesh.scheduler.scheduler import FluxScheduler
from fluxmesh.storage.broker import FluxBroker
from fluxmesh.worker.worker import FluxWorker
import fluxmesh.tasks


@pytest.mark.asyncio
async def test_flaky_task_retry_and_recovery(broker: FluxBroker):
    scheduler = FluxScheduler(broker=broker, tick_interval=0.05)
    worker = FluxWorker(broker=broker, worker_id="retry-worker", concurrency=2)

    await worker.start()
    await scheduler.start()

    sub = JobSubmission(
        task="flaky_pipeline",
        kwargs={"fail_times": 1, "key": "test_retry_1"},
        max_retries=3,
        backoff_base_seconds=0.05,
        priority=JobPriority.DEFAULT,
    )
    job = await broker.submit_job(sub)

    # Wait for execution, failure, backoff retry, and ultimate success
    for _ in range(35):
        await asyncio.sleep(0.1)
        record = await broker.get_job(job.id)
        if record and record.status == JobStatus.COMPLETED:
            break

    assert record.status == JobStatus.COMPLETED
    assert record.attempts == 1  # Succeeded on retry
    assert record.result["status"] == "recovered"

    await worker.stop()
    await scheduler.stop()


@pytest.mark.asyncio
async def test_dlq_exhaustion_and_replay(broker: FluxBroker):
    scheduler = FluxScheduler(broker=broker, tick_interval=0.05)
    worker = FluxWorker(broker=broker, worker_id="dlq-worker", concurrency=2)
    await scheduler.start()
    await worker.start()

    sub = JobSubmission(
        task="failing_task",
        kwargs={"reason": "Crash simulated"},
        max_retries=1,
        backoff_base_seconds=0.05,
    )
    job = await broker.submit_job(sub)

    # Wait for retry exhaustion
    for _ in range(35):
        await asyncio.sleep(0.1)
        record = await broker.get_job(job.id)
        if record and record.status == JobStatus.DEAD_LETTER:
            break

    assert record.status == JobStatus.DEAD_LETTER
    assert record.attempts > 1

    # Check DLQ listing
    dlq_jobs = await broker.get_dlq_jobs()
    assert any(j.id == job.id for j in dlq_jobs)

    # Replay DLQ job
    replayed = await broker.replay_dlq_job(job.id)
    assert replayed is True

    replayed_record = await broker.get_job(job.id)
    assert replayed_record.status == JobStatus.PENDING
    assert replayed_record.attempts == 0

    await worker.stop()
    await scheduler.stop()
