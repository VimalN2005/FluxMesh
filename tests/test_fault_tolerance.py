import asyncio
import time
import pytest
from fluxmesh.core.models import JobPriority, JobStatus, JobSubmission, WorkerHeartbeat
from fluxmesh.scheduler.scheduler import FluxScheduler
from fluxmesh.storage.broker import FluxBroker
from fluxmesh.worker.worker import FluxWorker
import fluxmesh.tasks


@pytest.mark.asyncio
async def test_zombie_worker_reaping_and_recovery(broker: FluxBroker):
    scheduler = FluxScheduler(broker=broker, tick_interval=0.1)

    # 1. Register a fake worker that died 15s ago
    dead_wid = "worker-crashed-node-99"
    stale_hb = WorkerHeartbeat(
        worker_id=dead_wid,
        hostname="node-99",
        pid=9999,
        last_heartbeat=time.time() - 15.0,
        status="healthy",
    )
    await broker.register_heartbeat(stale_hb)

    # 2. Submit a job and simulate that the dead worker popped it before crashing
    sub = JobSubmission(task="batch_process", kwargs={"items": ["x", "y"]}, priority=JobPriority.HIGH)
    job = await broker.submit_job(sub)

    popped = await broker.dequeue(dead_wid, ["high"])
    assert popped is not None and popped.id == job.id

    # Verify job is in dead worker's in-flight set
    in_flight = await broker.redis.smembers(broker._in_flight_key(dead_wid))
    assert job.id in in_flight

    # 3. Scheduler detects dead worker and reaps
    reaped = await broker.reap_dead_workers(stale_threshold_seconds=5.0)
    assert job.id in reaped

    # Dead worker must be deleted from registry
    workers = await broker.get_active_workers()
    assert not any(w.worker_id == dead_wid for w in workers)

    # 4. Job must be re-enqueued to HIGH priority queue with incremented attempts
    recovered_job = await broker.get_job(job.id)
    assert recovered_job.status == JobStatus.PENDING
    assert recovered_job.attempts == 1

    # 5. A healthy worker can now pick it up and complete it
    healthy_worker = FluxWorker(broker=broker, worker_id="healthy-worker-1", concurrency=2)
    await healthy_worker.start()

    for _ in range(20):
        await asyncio.sleep(0.1)
        record = await broker.get_job(job.id)
        if record and record.status == JobStatus.COMPLETED:
            break

    assert record.status == JobStatus.COMPLETED
    assert record.result["total"] == 2
    await healthy_worker.stop()
