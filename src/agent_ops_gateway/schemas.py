from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

Severity = Literal["low", "medium", "high", "critical"]


class WebhookEventRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str = Field(min_length=1, max_length=64)
    title: str = Field(min_length=1, max_length=200)
    body: str = Field(min_length=1, max_length=4000)
    metadata: dict[str, str] = Field(default_factory=dict)


class LLMTriageOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    severity: Severity
    recommended_queue: str = Field(min_length=1, max_length=64)
    confidence: float = Field(ge=0.0, le=1.0)
    requires_human_approval: bool
    summary: str = Field(min_length=1, max_length=500)


class TriageResponse(BaseModel):
    event_id: str
    idempotency_key: str
    replayed: bool
    severity: Severity
    recommended_queue: str
    confidence: float
    requires_human_approval: bool
    policy_overridden: bool
    fallback_used: bool
    summary: str
    dlq_job_id: str | None = None


class DLQJobResponse(BaseModel):
    id: str
    event_id: str
    idempotency_key: str
    error_code: str
    error_detail: str
    attempts: int
    status: str
    created_at: datetime
    replayed_at: datetime | None = None


class HealthResponse(BaseModel):
    status: str
    database: str
    llm_mode: str


class MetricsResponse(BaseModel):
    events_received: int
    events_replayed: int
    llm_succeeded: int
    policy_overridden: int
    dlq_enqueued: int
    dlq_replayed: int
    pending_dlq_jobs: int
