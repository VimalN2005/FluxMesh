import inspect
from typing import Any, Callable, Dict, Optional


class TaskContext:
    def __init__(self, job_id: str, worker_id: str, attempts: int, broker: Any):
        self.job_id = job_id
        self.worker_id = worker_id
        self.attempts = attempts
        self.broker = broker

    async def update_progress(self, percent: int, message: Optional[str] = None) -> None:
        if self.broker:
            await self.broker.update_progress(self.job_id, percent, message)


TASK_REGISTRY: Dict[str, Callable] = {}


def task(name: Optional[str] = None):
    def decorator(fn: Callable):
        task_name = name or fn.__name__
        TASK_REGISTRY[task_name] = fn
        return fn
    return decorator


def get_task(name: str) -> Optional[Callable]:
    return TASK_REGISTRY.get(name)
