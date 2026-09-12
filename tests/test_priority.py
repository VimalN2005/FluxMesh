import pytest
from fluxmesh.core.models import JobPriority, JobStatus, JobSubmission
from fluxmesh.storage.broker import FluxBroker


@pytest.mark.asyncio
async def test_strict_priority_ordering(broker: FluxBroker):
    # Enqueue in reverse order: LOW -> DEFAULT -> HIGH -> CRITICAL
    sub_low = JobSubmission(task="batch_process", priority=JobPriority.LOW)
    sub_default = JobSubmission(task="batch_process", priority=JobPriority.DEFAULT)
    sub_high = JobSubmission(task="batch_process", priority=JobPriority.HIGH)
    sub_critical = JobSubmission(task="batch_process", priority=JobPriority.CRITICAL)

    j_low = await broker.submit_job(sub_low)
    j_def = await broker.submit_job(sub_default)
    j_high = await broker.submit_job(sub_high)
    j_crit = await broker.submit_job(sub_critical)

    # Worker dequeues with full priority queue list
    q_order = ["critical", "high", "default", "low"]
    
    pop1 = await broker.dequeue("worker-1", q_order)
    pop2 = await broker.dequeue("worker-1", q_order)
    pop3 = await broker.dequeue("worker-1", q_order)
    pop4 = await broker.dequeue("worker-1", q_order)

    assert pop1 is not None and pop1.id == j_crit.id
    assert pop1.status == JobStatus.RUNNING
    assert pop2 is not None and pop2.id == j_high.id
    assert pop3 is not None and pop3.id == j_def.id
    assert pop4 is not None and pop4.id == j_low.id

    # Queue is now empty
    pop5 = await broker.dequeue("worker-1", q_order)
    assert pop5 is None
