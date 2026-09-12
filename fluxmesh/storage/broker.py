import json
import time
import uuid
from typing import Any, Dict, List, Optional
import redis.asyncio as aioredis

try:
    import fakeredis.aioredis as fake_aioredis
except ImportError:
    fake_aioredis = None

from fluxmesh.core.config import settings
from fluxmesh.core.models import (
    ClusterMetrics,
    JobPriority,
    JobRecord,
    JobStatus,
    JobSubmission,
    WorkerHeartbeat,
)
from fluxmesh.storage.lua_scripts import (
    DEQUEUE_JOB_LUA,
    PROMOTE_DELAYED_LUA,
    REAP_ZOMBIE_WORKER_LUA,
)


class FluxBroker:
    def __init__(self, redis_url: Optional[str] = None, use_fakeredis: Optional[bool] = None):
        self.redis_url = redis_url or settings.REDIS_URL
        self.use_fakeredis = use_fakeredis if use_fakeredis is not None else settings.USE_FAKEREDIS
        self.ns = settings.NAMESPACE
        self.redis: Optional[aioredis.Redis] = None
        self._dequeue_sha: Optional[str] = None
        self._promote_sha: Optional[str] = None
        self._reap_sha: Optional[str] = None

    async def connect(self) -> None:
        if self.redis is not None:
            return

        if self.use_fakeredis:
            try:
                # Try real Redis first if url provided
                client = aioredis.from_url(self.redis_url, decode_responses=True, socket_timeout=1.0)
                await client.ping()
                self.redis = client
            except Exception:
                if fake_aioredis:
                    self.redis = fake_aioredis.FakeRedis(decode_responses=True)
                else:
                    raise RuntimeError("Real Redis unreachable and fakeredis not installed.")
        else:
            self.redis = aioredis.from_url(self.redis_url, decode_responses=True)
            await self.redis.ping()

    async def close(self) -> None:
        if self.redis:
            await self.redis.aclose()
            self.redis = None

    def _queue_key(self, priority: str) -> str:
        return f"{self.ns}:queue:{priority.lower()}"

    def _job_key(self, job_id: str) -> str:
        return f"{self.ns}:jobs:{job_id}"

    def _delayed_key(self) -> str:
        return f"{self.ns}:delayed"

    def _dlq_key(self) -> str:
        return f"{self.ns}:dlq"

    def _in_flight_key(self, worker_id: str) -> str:
        return f"{self.ns}:in_flight:{worker_id}"

    def _workers_key(self) -> str:
        return f"{self.ns}:workers"

    def _idempotency_key(self, key: str) -> str:
        return f"{self.ns}:idempotency:{key}"

    def _lock_key(self, lock_name: str) -> str:
        return f"{self.ns}:lock:{lock_name}"

    async def submit_job(self, submission: JobSubmission) -> JobRecord:
        await self.connect()
        assert self.redis is not None

        # Idempotency deduplication check
        if submission.idempotency_key:
            idem_k = self._idempotency_key(submission.idempotency_key)
            existing_job_id = await self.redis.get(idem_k)
            if existing_job_id:
                existing_record = await self.get_job(existing_job_id)
                if existing_record:
                    return existing_record

        now = time.time()
        job_id = str(uuid.uuid4())

        scheduled_at = None
        status = JobStatus.PENDING

        if submission.delay_seconds is not None and submission.delay_seconds > 0:
            scheduled_at = now + submission.delay_seconds
            status = JobStatus.SCHEDULED
        elif submission.run_at is not None and submission.run_at > now:
            scheduled_at = submission.run_at
            status = JobStatus.SCHEDULED

        record = JobRecord(
            id=job_id,
            task=submission.task,
            args=submission.args,
            kwargs=submission.kwargs,
            priority=submission.priority,
            status=status,
            max_retries=submission.max_retries,
            attempts=0,
            backoff_strategy=submission.backoff_strategy,
            backoff_base_seconds=submission.backoff_base_seconds,
            max_backoff_seconds=submission.max_backoff_seconds,
            timeout_seconds=submission.timeout_seconds,
            created_at=now,
            scheduled_at=scheduled_at,
            tags=submission.tags,
            idempotency_key=submission.idempotency_key,
        )

        job_key = self._job_key(job_id)
        pipe = self.redis.pipeline()
        pipe.set(job_key, record.model_dump_json())

        if status == JobStatus.SCHEDULED and scheduled_at is not None:
            pipe.zadd(self._delayed_key(), {job_id: scheduled_at})
        else:
            q_name = self._queue_key(submission.priority.to_queue_name())
            pipe.lpush(q_name, job_id)

        if submission.idempotency_key:
            idem_k = self._idempotency_key(submission.idempotency_key)
            pipe.set(idem_k, job_id, ex=settings.IDEMPOTENCY_TTL_SECONDS)

        await pipe.execute()
        return record

    async def get_job(self, job_id: str) -> Optional[JobRecord]:
        await self.connect()
        assert self.redis is not None
        data = await self.redis.get(self._job_key(job_id))
        if not data:
            return None
        return JobRecord.model_validate_json(data)

    async def save_job(self, record: JobRecord) -> None:
        await self.connect()
        assert self.redis is not None
        await self.redis.set(self._job_key(record.id), record.model_dump_json())

    async def cancel_job(self, job_id: str) -> bool:
        await self.connect()
        assert self.redis is not None
        record = await self.get_job(job_id)
        if not record or record.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            return False

        if record.status == JobStatus.SCHEDULED:
            await self.redis.zrem(self._delayed_key(), job_id)
        elif record.status == JobStatus.PENDING:
            q_name = self._queue_key(record.priority.to_queue_name())
            await self.redis.lrem(q_name, 0, job_id)

        record.status = JobStatus.CANCELLED
        record.completed_at = time.time()
        await self.save_job(record)
        return True

    async def dequeue(self, worker_id: str, queues: List[str]) -> Optional[JobRecord]:
        await self.connect()
        assert self.redis is not None
        now = time.time()
        queue_keys = [self._queue_key(q) for q in queues]
        in_flight_k = self._in_flight_key(worker_id)
        job_prefix = f"{self.ns}:jobs:"

        res = await self.redis.eval(
            DEQUEUE_JOB_LUA,
            len(queue_keys),
            *queue_keys,
            now,
            in_flight_k,
            job_prefix,
        )

        if not res:
            return None

        job_id, job_data, _ = res
        record = JobRecord.model_validate_json(job_data)
        record.status = JobStatus.RUNNING
        record.worker_id = worker_id
        record.started_at = now
        await self.save_job(record)
        return record

    async def ack_job(self, worker_id: str, job_id: str, result: Any) -> None:
        await self.connect()
        assert self.redis is not None
        now = time.time()
        in_flight_k = self._in_flight_key(worker_id)

        pipe = self.redis.pipeline()
        pipe.srem(in_flight_k, job_id)
        await pipe.execute()

        record = await self.get_job(job_id)
        if record:
            record.status = JobStatus.COMPLETED
            record.completed_at = now
            record.result = result
            record.progress = 100
            await self.save_job(record)

    async def nack_job(
        self,
        worker_id: str,
        job_id: str,
        error: str,
        traceback_str: str,
        retry_delay: Optional[float] = None,
    ) -> None:
        await self.connect()
        assert self.redis is not None
        now = time.time()
        in_flight_k = self._in_flight_key(worker_id)
        await self.redis.srem(in_flight_k, job_id)

        record = await self.get_job(job_id)
        if not record:
            return

        record.attempts += 1
        record.error = error
        record.traceback = traceback_str

        if retry_delay is not None and record.attempts <= record.max_retries:
            record.status = JobStatus.SCHEDULED
            record.scheduled_at = now + retry_delay
            await self.save_job(record)
            await self.redis.zadd(self._delayed_key(), {job_id: record.scheduled_at})
        else:
            record.status = JobStatus.DEAD_LETTER
            record.completed_at = now
            await self.save_job(record)
            await self.redis.lpush(self._dlq_key(), job_id)

    async def update_progress(self, job_id: str, progress: int, message: Optional[str] = None) -> None:
        await self.connect()
        assert self.redis is not None
        record = await self.get_job(job_id)
        if record and record.status == JobStatus.RUNNING:
            record.progress = max(0, min(100, progress))
            if message is not None:
                record.progress_message = message
            await self.save_job(record)

    async def promote_delayed(self, batch_size: int = 100) -> int:
        await self.connect()
        assert self.redis is not None
        now = time.time()
        res = await self.redis.eval(
            PROMOTE_DELAYED_LUA,
            1,
            self._delayed_key(),
            now,
            batch_size,
            f"{self.ns}:jobs:",
            self._queue_key("critical"),
            self._queue_key("high"),
            self._queue_key("default"),
            self._queue_key("low"),
        )
        return int(res) if res else 0

    async def register_heartbeat(self, heartbeat: WorkerHeartbeat) -> None:
        await self.connect()
        assert self.redis is not None
        await self.redis.hset(self._workers_key(), heartbeat.worker_id, heartbeat.model_dump_json())

    async def get_active_workers(self) -> List[WorkerHeartbeat]:
        await self.connect()
        assert self.redis is not None
        raw_workers = await self.redis.hgetall(self._workers_key())
        results = []
        for _, raw in raw_workers.items():
            try:
                results.append(WorkerHeartbeat.model_validate_json(raw))
            except Exception:
                pass
        return results

    async def reap_dead_workers(self, stale_threshold_seconds: Optional[float] = None) -> List[str]:
        await self.connect()
        assert self.redis is not None
        threshold = stale_threshold_seconds or settings.WORKER_TTL_SECONDS
        now = time.time()
        workers = await self.get_active_workers()
        reaped_all: List[str] = []

        for w in workers:
            if now - w.last_heartbeat > threshold:
                in_flight_k = self._in_flight_key(w.worker_id)
                res = await self.redis.eval(
                    REAP_ZOMBIE_WORKER_LUA,
                    2,
                    in_flight_k,
                    self._dlq_key(),
                    f"{self.ns}:jobs:",
                    self._queue_key("critical"),
                    self._queue_key("high"),
                    self._queue_key("default"),
                    self._queue_key("low"),
                )
                if res and isinstance(res, list):
                    reaped_all.extend([r for r in res])
                await self.redis.hdel(self._workers_key(), w.worker_id)
        return reaped_all

    async def get_dlq_jobs(self, limit: int = 50, offset: int = 0) -> List[JobRecord]:
        await self.connect()
        assert self.redis is not None
        job_ids = await self.redis.lrange(self._dlq_key(), offset, offset + limit - 1)
        records = []
        for jid in job_ids:
            r = await self.get_job(jid)
            if r:
                records.append(r)
        return records

    async def replay_dlq_job(self, job_id: str) -> bool:
        await self.connect()
        assert self.redis is not None
        record = await self.get_job(job_id)
        if not record or record.status != JobStatus.DEAD_LETTER:
            return False

        # Remove from DLQ
        await self.redis.lrem(self._dlq_key(), 0, job_id)
        record.status = JobStatus.PENDING
        record.attempts = 0
        record.error = None
        record.traceback = None
        record.completed_at = None
        await self.save_job(record)

        q_name = self._queue_key(record.priority.to_queue_name())
        await self.redis.lpush(q_name, job_id)
        return True

    async def get_metrics(self) -> ClusterMetrics:
        await self.connect()
        assert self.redis is not None
        pipe = self.redis.pipeline()
        queues = ["critical", "high", "default", "low"]
        for q in queues:
            pipe.llen(self._queue_key(q))
        pipe.zcard(self._delayed_key())
        pipe.llen(self._dlq_key())
        pipe.hlen(self._workers_key())

        res = await pipe.execute()
        queue_depths = {
            "critical": res[0],
            "high": res[1],
            "default": res[2],
            "low": res[3],
        }
        delayed_count = res[4]
        dlq_count = res[5]
        active_workers = res[6]

        pending = sum(queue_depths.values())
        return ClusterMetrics(
            active_workers=active_workers,
            total_jobs=pending + delayed_count + dlq_count,
            pending_jobs=pending,
            running_jobs=0,
            completed_jobs=0,
            failed_jobs=0,
            dead_letter_jobs=dlq_count,
            queue_depths=queue_depths,
            timestamp=time.time(),
        )

    async def acquire_lock(self, lock_name: str, ttl_seconds: float = 5.0) -> Optional[str]:
        await self.connect()
        assert self.redis is not None
        token = str(uuid.uuid4())
        key = self._lock_key(lock_name)
        acquired = await self.redis.set(key, token, px=int(ttl_seconds * 1000), nx=True)
        return token if acquired else None

    async def release_lock(self, lock_name: str, token: str) -> bool:
        await self.connect()
        assert self.redis is not None
        lua = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        else
            return 0
        end
        """
        res = await self.redis.eval(lua, 1, self._lock_key(lock_name), token)
        return bool(res)
