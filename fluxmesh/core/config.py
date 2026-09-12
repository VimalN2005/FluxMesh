import os
from pydantic import BaseModel


class Settings(BaseModel):
    REDIS_URL: str = os.getenv("FLUXMESH_REDIS_URL", "redis://localhost:6379/0")
    USE_FAKEREDIS: bool = os.getenv("FLUXMESH_USE_FAKEREDIS", "auto").lower() in ("true", "1", "auto")
    NAMESPACE: str = os.getenv("FLUXMESH_NAMESPACE", "fluxmesh")
    WORKER_HEARTBEAT_INTERVAL: float = float(os.getenv("FLUXMESH_WORKER_HEARTBEAT_INTERVAL", "2.0"))
    WORKER_TTL_SECONDS: float = float(os.getenv("FLUXMESH_WORKER_TTL_SECONDS", "6.0"))
    SCHEDULER_TICK_INTERVAL: float = float(os.getenv("FLUXMESH_SCHEDULER_TICK_INTERVAL", "0.2"))
    DEFAULT_CONCURRENCY: int = int(os.getenv("FLUXMESH_DEFAULT_CONCURRENCY", "4"))
    DEFAULT_JOB_TIMEOUT: float = float(os.getenv("FLUXMESH_DEFAULT_JOB_TIMEOUT", "300.0"))
    IDEMPOTENCY_TTL_SECONDS: int = int(os.getenv("FLUXMESH_IDEMPOTENCY_TTL_SECONDS", "86400"))


settings = Settings()
