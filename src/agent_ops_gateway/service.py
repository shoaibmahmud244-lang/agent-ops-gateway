import hashlib
import json
import threading
import time
import uuid
from datetime import UTC, datetime

from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from agent_ops_gateway.config import Settings
from agent_ops_gateway.logging_utils import get_logger, log_event
from agent_ops_gateway.models import DeadLetterJob, TriageEvent
from agent_ops_gateway.provider import ProviderError, TriageProvider
from agent_ops_gateway.schemas import (
    DLQJobResponse,
    LLMTriageOutput,
    MetricsResponse,
    Severity,
    TriageResponse,
    WebhookEventRequest,
)


class IdempotencyConflictError(Exception):
    pass


class DLQNotFoundError(Exception):
    pass


class DLQAlreadyReplayedError(Exception):
    pass


class DLQReplayFailedError(Exception):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


class MetricsStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, int] = {
            "events_received": 0,
            "events_replayed": 0,
            "llm_succeeded": 0,
            "policy_overridden": 0,
            "dlq_enqueued": 0,
            "dlq_replayed": 0,
        }

    def inc(self, key: str) -> None:
        with self._lock:
            self._counters[key] += 1

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counters)


def canonical_payload_hash(event: WebhookEventRequest) -> str:
    canonical = json.dumps(event.model_dump(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _to_severity(value: str) -> Severity:
    if value == "low":
        return "low"
    if value == "high":
        return "high"
    if value == "critical":
        return "critical"
    return "medium"


def _event_to_response(row: TriageEvent, *, replayed: bool) -> TriageResponse:
    return TriageResponse(
        event_id=row.id,
        idempotency_key=row.idempotency_key,
        replayed=replayed,
        severity=_to_severity(row.severity),
        recommended_queue=row.recommended_queue,
        confidence=row.confidence,
        requires_human_approval=row.requires_human_approval,
        policy_overridden=row.policy_overridden,
        fallback_used=row.fallback_used,
        summary=row.summary,
        dlq_job_id=row.dlq_job_id,
    )


class GatewayService:
    def __init__(
        self,
        settings: Settings,
        provider: TriageProvider,
        metrics: MetricsStore,
    ) -> None:
        self._settings = settings
        self._provider = provider
        self._metrics = metrics
        self._logger = get_logger()

    def _run_provider_with_retries(
        self, event: WebhookEventRequest
    ) -> tuple[LLMTriageOutput | None, int, str, str]:
        attempts = self._settings.llm_max_retries + 1
        last_code = "UNKNOWN_ERROR"
        last_detail = ""

        for attempt in range(1, attempts + 1):
            try:
                raw = self._provider.triage(event)
                decision = LLMTriageOutput.model_validate_json(raw)
                return decision, attempt, "", ""
            except ProviderError as exc:
                last_code = exc.code
                last_detail = exc.detail
            except ValidationError as exc:
                last_code = "SCHEMA_VALIDATION_FAILED"
                last_detail = str(exc)

            if attempt < attempts and self._settings.retry_backoff_seconds > 0:
                time.sleep(self._settings.retry_backoff_seconds * (2 ** (attempt - 1)))

        return None, attempts, last_code, last_detail[:1000]

    def _apply_policy_floor(self, decision: LLMTriageOutput) -> tuple[bool, bool]:
        requires_human = decision.requires_human_approval
        overridden = False
        low_confidence = decision.confidence < self._settings.confidence_threshold
        if (decision.severity == "critical" or low_confidence) and not requires_human:
            requires_human = True
            overridden = True
        return requires_human, overridden

    def process_event(
        self,
        db: Session,
        idempotency_key: str,
        event: WebhookEventRequest,
    ) -> tuple[TriageResponse, int]:
        started = time.perf_counter()
        self._metrics.inc("events_received")
        payload_hash = canonical_payload_hash(event)

        existing = db.scalar(
            select(TriageEvent).where(TriageEvent.idempotency_key == idempotency_key)
        )
        if existing is not None:
            if existing.payload_hash != payload_hash:
                raise IdempotencyConflictError(
                    "Idempotency-Key was already used with a different payload."
                )
            self._metrics.inc("events_replayed")
            log_event(
                self._logger,
                "triage_replayed",
                idempotency_key=idempotency_key,
                event_id=existing.id,
            )
            return _event_to_response(existing, replayed=True), 200

        decision, attempts, err_code, err_detail = self._run_provider_with_retries(event)
        now = datetime.now(UTC)
        event_id = str(uuid.uuid4())

        if decision is not None:
            requires_human, overridden = self._apply_policy_floor(decision)
            row = TriageEvent(
                id=event_id,
                idempotency_key=idempotency_key,
                payload_hash=payload_hash,
                source=event.source,
                title=event.title,
                severity=decision.severity,
                recommended_queue=decision.recommended_queue,
                confidence=decision.confidence,
                requires_human_approval=requires_human,
                policy_overridden=overridden,
                fallback_used=False,
                summary=decision.summary,
                dlq_job_id=None,
                created_at=now,
            )
            status_code = 200
        else:
            dlq_id = str(uuid.uuid4())
            dlq_job = DeadLetterJob(
                id=dlq_id,
                event_id=event_id,
                idempotency_key=idempotency_key,
                raw_payload_json=event.model_dump_json(),
                error_code=err_code,
                error_detail=err_detail,
                attempts=attempts,
                status="pending_replay",
                created_at=now,
                replayed_at=None,
            )
            row = TriageEvent(
                id=event_id,
                idempotency_key=idempotency_key,
                payload_hash=payload_hash,
                source=event.source,
                title=event.title,
                severity="medium",
                recommended_queue="manual-triage",
                confidence=0.0,
                requires_human_approval=True,
                policy_overridden=False,
                fallback_used=True,
                summary=(
                    f"Deterministic fallback activated after {attempts} failed provider attempt(s)."
                ),
                dlq_job_id=dlq_id,
                created_at=now,
            )
            db.add(dlq_job)
            status_code = 202

        db.add(row)
        try:
            db.commit()
        except IntegrityError as exc:
            db.rollback()
            concurrent = db.scalar(
                select(TriageEvent).where(TriageEvent.idempotency_key == idempotency_key)
            )
            if concurrent is None or concurrent.payload_hash != payload_hash:
                raise IdempotencyConflictError(
                    "Concurrent Idempotency-Key conflict with different payload."
                ) from exc
            self._metrics.inc("events_replayed")
            return _event_to_response(concurrent, replayed=True), 200

        if decision is not None:
            self._metrics.inc("llm_succeeded")
            if row.policy_overridden:
                self._metrics.inc("policy_overridden")
        else:
            self._metrics.inc("dlq_enqueued")

        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        log_event(
            self._logger,
            "triage_completed",
            event_id=row.id,
            idempotency_key=idempotency_key,
            severity=row.severity,
            fallback_used=row.fallback_used,
            policy_overridden=row.policy_overridden,
            attempts=attempts,
            elapsed_ms=elapsed_ms,
        )
        return _event_to_response(row, replayed=False), status_code

    def list_dlq(self, db: Session, status: str | None = None) -> list[DLQJobResponse]:
        stmt = select(DeadLetterJob).order_by(DeadLetterJob.created_at.desc())
        if status is not None:
            stmt = stmt.where(DeadLetterJob.status == status)
        jobs = db.scalars(stmt).all()
        return [
            DLQJobResponse(
                id=j.id,
                event_id=j.event_id,
                idempotency_key=j.idempotency_key,
                error_code=j.error_code,
                error_detail=j.error_detail,
                attempts=j.attempts,
                status=j.status,
                created_at=j.created_at,
                replayed_at=j.replayed_at,
            )
            for j in jobs
        ]

    def replay_dlq_job(self, db: Session, job_id: str) -> TriageResponse:
        job = db.get(DeadLetterJob, job_id)
        if job is None:
            raise DLQNotFoundError(f"DLQ job {job_id} not found.")
        if job.status == "replayed":
            raise DLQAlreadyReplayedError(f"DLQ job {job_id} was already replayed.")

        event = WebhookEventRequest.model_validate_json(job.raw_payload_json)
        decision, attempts, err_code, err_detail = self._run_provider_with_retries(event)
        if decision is None:
            job.attempts += attempts
            job.error_code = err_code
            job.error_detail = err_detail
            db.commit()
            raise DLQReplayFailedError(err_code, err_detail)

        triage_row = db.get(TriageEvent, job.event_id)
        if triage_row is None:
            raise DLQNotFoundError(f"Linked event {job.event_id} not found.")

        requires_human, overridden = self._apply_policy_floor(decision)
        triage_row.severity = decision.severity
        triage_row.recommended_queue = decision.recommended_queue
        triage_row.confidence = decision.confidence
        triage_row.requires_human_approval = requires_human
        triage_row.policy_overridden = overridden
        triage_row.fallback_used = False
        triage_row.summary = decision.summary

        job.status = "replayed"
        job.replayed_at = datetime.now(UTC)
        db.commit()

        self._metrics.inc("dlq_replayed")
        self._metrics.inc("llm_succeeded")
        if overridden:
            self._metrics.inc("policy_overridden")

        log_event(
            self._logger,
            "dlq_replayed",
            job_id=job.id,
            event_id=triage_row.id,
            severity=triage_row.severity,
        )
        return _event_to_response(triage_row, replayed=False)

    def get_metrics(self, db: Session) -> MetricsResponse:
        pending = db.scalar(
            select(func.count(DeadLetterJob.id)).where(DeadLetterJob.status == "pending_replay")
        )
        snap = self._metrics.snapshot()
        return MetricsResponse(
            events_received=snap["events_received"],
            events_replayed=snap["events_replayed"],
            llm_succeeded=snap["llm_succeeded"],
            policy_overridden=snap["policy_overridden"],
            dlq_enqueued=snap["dlq_enqueued"],
            dlq_replayed=snap["dlq_replayed"],
            pending_dlq_jobs=int(pending or 0),
        )
