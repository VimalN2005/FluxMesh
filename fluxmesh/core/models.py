from enum import Enum, IntEnum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class JobPriority(IntEnum):
    CRITICAL = 0
    HIGH = 1
    DEFAULT = 2
    LOW = 3

    @classmethod
    def from_str(cls, val: str) -> "JobPriority":
        mapping = {
            "critical": cls.CRITICAL,
            "high": cls.HIGH,
            "default": cls.DEFAULT,
            "low": cls.LOW,
        }
        return mapping.get(val.lower(), cls.DEFAULT)

    def to_queue_name(self) -> str:
        names = {
            self.CRITICAL: "critical",
            self.HIGH: "high",
            self.DEFAULT: "default",
            self.LOW: "low",
        }
        return names[self]


class JobStatus(str, Enum):
    PENDING = "PENDING"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    DEAD_LETTER = "DEAD_LETTER"


class BackoffStrategy(str, Enum):
    EXPONENTIAL = "exponential"
    FIXED = "fixed"
    LINEAR = "linear"


class JobSubmission(BaseModel):
    task: str
    args: List[Any] = Field(default_factory=list)
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    priority: JobPriority = JobPriority.DEFAULT
    max_retries: int = 3
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    backoff_base_seconds: float = 1.0
    max_backoff_seconds: float = 60.0
    timeout_seconds: Optional[float] = 60.0
    delay_seconds: Optional[float] = None
    run_at: Optional[float] = None
    idempotency_key: Optional[str] = None
    tags: List[str] = Field(default_factory=list)


class JobRecord(BaseModel):
    id: str
    task: str
    args: List[Any] = Field(default_factory=list)
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    priority: JobPriority = JobPriority.DEFAULT
    status: JobStatus = JobStatus.PENDING
    max_retries: int = 3
    attempts: int = 0
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    backoff_base_seconds: float = 1.0
    max_backoff_seconds: float = 60.0
    timeout_seconds: Optional[float] = 60.0
    created_at: float
    scheduled_at: Optional[float] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    worker_id: Optional[str] = None
    progress: int = 0
    progress_message: Optional[str] = None
    result: Any = None
    error: Optional[str] = None
    traceback: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    idempotency_key: Optional[str] = None


class WorkerHeartbeat(BaseModel):
    worker_id: str
    hostname: str
    pid: int
    queues: List[str] = Field(default_factory=lambda: ["critical", "high", "default", "low"])
    concurrency: int = 4
    active_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    uptime_seconds: float = 0.0
    last_heartbeat: float
    status: str = "healthy"


class ClusterMetrics(BaseModel):
    active_workers: int = 0
    total_jobs: int = 0
    pending_jobs: int = 0
    running_jobs: int = 0
    completed_jobs: int = 0
    failed_jobs: int = 0
    dead_letter_jobs: int = 0
    queue_depths: Dict[str, int] = Field(default_factory=dict)
    timestamp: float = 0.0
