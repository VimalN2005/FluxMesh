import asyncio
import time
from typing import Optional
from fluxmesh.core.config import settings
from fluxmesh.storage.broker import FluxBroker


class FluxScheduler:
    def __init__(
        self,
        broker: Optional[FluxBroker] = None,
        tick_interval: Optional[float] = None,
        leader_lock_ttl: float = 3.0,
    ):
        self.broker = broker or FluxBroker()
        self.tick_interval = tick_interval or settings.SCHEDULER_TICK_INTERVAL
        self.leader_lock_ttl = leader_lock_ttl
        self.is_running = False
        self.is_leader = False
        self._leader_token: Optional[str] = None
        self._loop_task: Optional[asyncio.Task] = None
        self.promoted_count = 0
        self.reaped_count = 0

    async def _try_acquire_leader(self) -> bool:
        token = await self.broker.acquire_lock("scheduler_leader", ttl_seconds=self.leader_lock_ttl)
        if token:
            self.is_leader = True
            self._leader_token = token
            return True
        self.is_leader = False
        self._leader_token = None
        return False

    async def _renew_leader_lock(self) -> None:
        if self.is_leader and self._leader_token:
            # Re-acquire or extend
            token = await self.broker.acquire_lock("scheduler_leader", ttl_seconds=self.leader_lock_ttl)
            if token:
                self._leader_token = token

    async def tick(self) -> None:
        # 1. Promote due delayed and retry jobs
        promoted = await self.broker.promote_delayed(batch_size=200)
        self.promoted_count += promoted

        # 2. Detect dead workers and reap their orphan/zombie jobs
        reaped = await self.broker.reap_dead_workers(stale_threshold_seconds=settings.WORKER_TTL_SECONDS)
        self.reaped_count += len(reaped)

    async def _run_loop(self) -> None:
        while self.is_running:
            try:
                # Leader election
                has_leadership = await self._try_acquire_leader()
                if has_leadership:
                    await self.tick()
            except asyncio.CancelledError:
                break
            except Exception:
                pass

            await asyncio.sleep(self.tick_interval)

    async def start(self) -> None:
        await self.broker.connect()
        self.is_running = True
        self._loop_task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self.is_running = False
        if self._loop_task:
            self._loop_task.cancel()
        if self.is_leader and self._leader_token:
            try:
                await self.broker.release_lock("scheduler_leader", self._leader_token)
            except Exception:
                pass

    async def run_until_interrupted(self) -> None:
        await self.start()
        try:
            while self.is_running:
                await asyncio.sleep(0.5)
        except (KeyboardInterrupt, asyncio.CancelledError):
            await self.stop()
