import asyncio
import inspect
import os
import random
import socket
import time
import traceback
import uuid
from typing import Any, Dict, List, Optional

from fluxmesh.core.config import settings
from fluxmesh.core.models import BackoffStrategy, JobRecord, WorkerHeartbeat
from fluxmesh.storage.broker import FluxBroker
from fluxmesh.worker.registry import TaskContext, get_task


class FluxWorker:
    def __init__(
        self,
        broker: Optional[FluxBroker] = None,
        worker_id: Optional[str] = None,
        queues: Optional[List[str]] = None,
        concurrency: Optional[int] = None,
        heartbeat_interval: Optional[float] = None,
    ):
        self.broker = broker or FluxBroker()
        self.worker_id = worker_id or f"worker-{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.queues = queues or ["critical", "high", "default", "low"]
        self.concurrency = concurrency or settings.DEFAULT_CONCURRENCY
        self.heartbeat_interval = heartbeat_interval or settings.WORKER_HEARTBEAT_INTERVAL

        self.is_running = False
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._active_tasks: Dict[str, asyncio.Task] = {}
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._poll_task: Optional[asyncio.Task] = None
        self.start_time = time.time()
        self.completed_count = 0
        self.failed_count = 0

    def calculate_backoff(self, record: JobRecord) -> float:
        attempt = record.attempts + 1
        base = record.backoff_base_seconds
        max_b = record.max_backoff_seconds

        if record.backoff_strategy == BackoffStrategy.FIXED:
            delay = base
        elif record.backoff_strategy == BackoffStrategy.LINEAR:
            delay = base * attempt
        else:  # EXPONENTIAL with Full Jitter
            delay = base * (2 ** attempt)

        delay = min(delay, max_b)
        # Apply jitter between 50% and 100% of delay to avoid thundering herd
        delay = delay * random.uniform(0.5, 1.0)
        return max(0.1, round(delay, 2))

    async def _heartbeat_loop(self) -> None:
        while self.is_running:
            try:
                hb = WorkerHeartbeat(
                    worker_id=self.worker_id,
                    hostname=socket.gethostname(),
                    pid=os.getpid(),
                    queues=self.queues,
                    concurrency=self.concurrency,
                    active_tasks=len(self._active_tasks),
                    completed_tasks=self.completed_count,
                    failed_tasks=self.failed_count,
                    uptime_seconds=round(time.time() - self.start_time, 1),
                    last_heartbeat=time.time(),
                    status="healthy",
                )
                await self.broker.register_heartbeat(hb)
            except Exception:
                pass
            await asyncio.sleep(self.heartbeat_interval)

    async def _execute_job(self, record: JobRecord) -> None:
        ctx = TaskContext(
            job_id=record.id,
            worker_id=self.worker_id,
            attempts=record.attempts,
            broker=self.broker,
        )

        task_fn = get_task(record.task)
        if not task_fn:
            error_msg = f"Task '{record.task}' is not registered."
            await self.broker.nack_job(self.worker_id, record.id, error_msg, error_msg, retry_delay=None)
            self.failed_count += 1
            return

        # Prepare arguments
        sig = inspect.signature(task_fn)
        kwargs = dict(record.kwargs)
        if "ctx" in sig.parameters:
            kwargs["ctx"] = ctx

        timeout = record.timeout_seconds or settings.DEFAULT_JOB_TIMEOUT

        try:
            if inspect.iscoroutinefunction(task_fn):
                res = await asyncio.wait_for(task_fn(*record.args, **kwargs), timeout=timeout)
            else:
                res = await asyncio.wait_for(
                    asyncio.to_thread(task_fn, *record.args, **kwargs),
                    timeout=timeout,
                )

            await self.broker.ack_job(self.worker_id, record.id, res)
            self.completed_count += 1

        except Exception as exc:
            self.failed_count += 1
            tb_str = traceback.format_exc()
            error_msg = f"{type(exc).__name__}: {str(exc)}"

            if record.attempts < record.max_retries:
                delay = self.calculate_backoff(record)
                await self.broker.nack_job(self.worker_id, record.id, error_msg, tb_str, retry_delay=delay)
            else:
                await self.broker.nack_job(self.worker_id, record.id, error_msg, tb_str, retry_delay=None)

    async def _handle_job_wrapper(self, record: JobRecord) -> None:
        try:
            await self._execute_job(record)
        finally:
            self._active_tasks.pop(record.id, None)
            self._semaphore.release()

    async def _poll_loop(self) -> None:
        while self.is_running:
            try:
                await self._semaphore.acquire()
                if not self.is_running:
                    self._semaphore.release()
                    break

                record = await self.broker.dequeue(self.worker_id, self.queues)
                if record is None:
                    self._semaphore.release()
                    await asyncio.sleep(0.05)
                    continue

                t = asyncio.create_task(self._handle_job_wrapper(record))
                self._active_tasks[record.id] = t

            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(0.1)

    async def start(self) -> None:
        await self.broker.connect()
        self.is_running = True
        self.start_time = time.time()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._poll_task = asyncio.create_task(self._poll_loop())

    async def stop(self, grace_period: float = 5.0) -> None:
        self.is_running = False
        if self._poll_task:
            self._poll_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

        # Wait for active tasks up to grace_period
        if self._active_tasks:
            active_list = list(self._active_tasks.values())
            done, pending = await asyncio.wait(active_list, timeout=grace_period)
            for p in pending:
                p.cancel()

        # Cleanup heartbeat entry
        try:
            if self.broker.redis:
                await self.broker.redis.hdel(self.broker._workers_key(), self.worker_id)
        except Exception:
            pass

    async def run_until_interrupted(self) -> None:
        await self.start()
        try:
            while self.is_running:
                await asyncio.sleep(0.5)
        except (KeyboardInterrupt, asyncio.CancelledError):
            await self.stop()
