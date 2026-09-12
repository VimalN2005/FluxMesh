import asyncio
import hashlib
import time
from typing import Any, Dict, List
from fluxmesh.worker.registry import TaskContext, task


@task("batch_process")
async def batch_process(items: List[str], ctx: TaskContext) -> Dict[str, Any]:
    total = len(items)
    processed = []
    for idx, item in enumerate(items):
        await asyncio.sleep(0.01)
        processed.append(f"processed_{item.upper()}")
        percent = int(((idx + 1) / total) * 100)
        await ctx.update_progress(percent, f"Processed {idx + 1}/{total} records")

    return {"total": total, "processed_count": len(processed), "results": processed}


@task("webhook_delivery")
async def webhook_delivery(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    # Simulate network latency
    await asyncio.sleep(0.02)
    return {
        "status_code": 200,
        "url": url,
        "delivered_at": time.time(),
        "signature": hashlib.sha256(str(payload).encode()).hexdigest(),
    }


@task("heavy_computation")
def heavy_computation(iterations: int = 10000) -> Dict[str, Any]:
    h = "start"
    for i in range(iterations):
        h = hashlib.sha256(f"{h}_{i}".encode()).hexdigest()
    return {"iterations": iterations, "final_hash": h}


# In-memory transient counter for flaky pipeline simulation
_FLAKY_ATTEMPTS: Dict[str, int] = {}


@task("flaky_pipeline")
async def flaky_pipeline(fail_times: int = 2, key: str = "default") -> Dict[str, Any]:
    current = _FLAKY_ATTEMPTS.get(key, 0)
    if current < fail_times:
        _FLAKY_ATTEMPTS[key] = current + 1
        raise ConnectionResetError(f"Transient connection drop (Attempt {current + 1} of {fail_times})")

    _FLAKY_ATTEMPTS.pop(key, None)
    return {"status": "recovered", "attempts_before_success": current + 1}


@task("failing_task")
async def failing_task(reason: str = "Unrecoverable database syntax error") -> None:
    raise RuntimeError(reason)


@task("slow_task")
async def slow_task(duration: float = 2.0) -> Dict[str, Any]:
    await asyncio.sleep(duration)
    return {"completed_after": duration}
