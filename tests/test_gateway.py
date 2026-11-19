import json
import os
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from agent_ops_gateway.config import Settings
from agent_ops_gateway.db import Base, build_engine
from agent_ops_gateway.main import create_app
from agent_ops_gateway.provider import MockTriageProvider, ProviderError, TriageProvider
from agent_ops_gateway.schemas import WebhookEventRequest

API_KEY = "test-secret-key"
AUTH_HEADERS = {"X-API-Key": API_KEY}


def _test_database_url() -> str:
    return os.environ.get("TEST_DATABASE_URL", "sqlite:///:memory:")


def _reset_external_db(db_url: str) -> None:
    if db_url.startswith("sqlite"):
        return
    engine = build_engine(db_url)
    Base.metadata.create_all(bind=engine)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dead_letter_jobs, triage_events"))
    engine.dispose()


class CountingProvider:
    def __init__(self, inner: TriageProvider) -> None:
        self._inner = inner
        self.calls = 0

    def triage(self, event: WebhookEventRequest) -> str:
        self.calls += 1
        return self._inner.triage(event)


class SwitchableProvider:
    def __init__(self) -> None:
        self.mode = "malformed"
        self.calls = 0

    def triage(self, event: WebhookEventRequest) -> str:
        self.calls += 1
        if self.mode == "malformed":
            # Missing required fields + unexpected extra key.
            return json.dumps({"severity": "high", "hallucinated_key": True})
        if self.mode == "timeout":
            raise ProviderError("PROVIDER_TIMEOUT", "upstream LLM timed out")
        return MockTriageProvider().triage(event)


def _make_client(provider: TriageProvider) -> Iterator[TestClient]:
    db_url = _test_database_url()
    _reset_external_db(db_url)
    settings = Settings(
        api_key=API_KEY,
        database_url=db_url,
        llm_mode="mock",
        llm_max_retries=2,
        retry_backoff_seconds=0.0,
        confidence_threshold=0.70,
    )
    app = create_app(settings=settings, provider=provider)
    with TestClient(app) as client:
        yield client


def test_health_and_auth_enforcement() -> None:
    for client in _make_client(MockTriageProvider()):
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["database"] == "connected"

        unauth = client.post(
            "/v1/events/triage",
            headers={"Idempotency-Key": "evt-unauth"},
            json={"source": "jira", "title": "Test", "body": "No key"},
        )
        assert unauth.status_code == 401


def test_idempotent_replay_and_conflict() -> None:
    counting = CountingProvider(MockTriageProvider())
    payload = {
        "source": "zendesk",
        "title": "Invoice copy needed",
        "body": "Customer requested invoice PDF for October.",
        "metadata": {"region": "apac", "tier": "pro"},
    }
    for client in _make_client(counting):
        first = client.post(
            "/v1/events/triage",
            headers={**AUTH_HEADERS, "Idempotency-Key": "evt-100"},
            json=payload,
        )
        assert first.status_code == 200
        first_body = first.json()
        assert first_body["replayed"] is False
        assert first_body["recommended_queue"] == "finance-ops"
        assert counting.calls == 1

        # Same payload with differently ordered metadata keys must still replay cleanly.
        reordered = {
            **payload,
            "metadata": {"tier": "pro", "region": "apac"},
        }
        second = client.post(
            "/v1/events/triage",
            headers={**AUTH_HEADERS, "Idempotency-Key": "evt-100"},
            json=reordered,
        )
        assert second.status_code == 200
        assert second.json()["replayed"] is True
        assert second.json()["event_id"] == first_body["event_id"]
        assert counting.calls == 1

        # Same key with a modified body must fail with 409 Conflict.
        conflict = client.post(
            "/v1/events/triage",
            headers={**AUTH_HEADERS, "Idempotency-Key": "evt-100"},
            json={**payload, "title": "Tampered title"},
        )
        assert conflict.status_code == 409


@pytest.mark.parametrize(
    ("title", "expected_severity"),
    [
        ("Production outage on API", "critical"),
        ("Ambiguous network alert", "medium"),
    ],
)
def test_policy_floor_overrides_critical_and_low_confidence(
    title: str, expected_severity: str
) -> None:
    for client in _make_client(MockTriageProvider()):
        resp = client.post(
            "/v1/events/triage",
            headers={**AUTH_HEADERS, "Idempotency-Key": f"policy-{expected_severity}"},
            json={"source": "pagerduty", "title": title, "body": "Needs review"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["severity"] == expected_severity
        assert body["requires_human_approval"] is True
        assert body["policy_overridden"] is True


def test_malformed_llm_routes_to_dlq_and_replays_when_provider_recovers() -> None:
    switchable = SwitchableProvider()
    for client in _make_client(switchable):
        resp = client.post(
            "/v1/events/triage",
            headers={**AUTH_HEADERS, "Idempotency-Key": "evt-dlq-1"},
            json={
                "source": "n8n",
                "title": "Invoice sync failure",
                "body": "Billing webhook returned 500",
            },
        )
        assert resp.status_code == 202
        body = resp.json()
        assert body["fallback_used"] is True
        assert body["recommended_queue"] == "manual-triage"
        assert body["requires_human_approval"] is True
        assert body["dlq_job_id"] is not None
        assert switchable.calls == 3  # 1 initial + 2 retries

        dlq_list = client.get("/v1/dlq", headers=AUTH_HEADERS, params={"status": "pending_replay"})
        assert dlq_list.status_code == 200
        jobs = dlq_list.json()
        assert len(jobs) == 1
        assert jobs[0]["error_code"] == "SCHEMA_VALIDATION_FAILED"

        # Provider recovers -> replay resolves the event and closes the DLQ job.
        switchable.mode = "ok"
        replay = client.post(f"/v1/dlq/{body['dlq_job_id']}/replay", headers=AUTH_HEADERS)
        assert replay.status_code == 200
        replayed_body = replay.json()
        assert replayed_body["fallback_used"] is False
        assert replayed_body["recommended_queue"] == "finance-ops"

        # Replaying an already resolved DLQ job returns 409.
        replay_again = client.post(f"/v1/dlq/{body['dlq_job_id']}/replay", headers=AUTH_HEADERS)
        assert replay_again.status_code == 409

        metrics = client.get("/metrics").json()
        assert metrics["dlq_enqueued"] == 1
        assert metrics["dlq_replayed"] == 1
        assert metrics["pending_dlq_jobs"] == 0


def test_provider_timeout_routes_to_dlq() -> None:
    switchable = SwitchableProvider()
    switchable.mode = "timeout"
    for client in _make_client(switchable):
        resp = client.post(
            "/v1/events/triage",
            headers={**AUTH_HEADERS, "Idempotency-Key": "evt-timeout-1"},
            json={"source": "datadog", "title": "CPU spike", "body": "99% for 10m"},
        )
        assert resp.status_code == 202
        jobs = client.get("/v1/dlq", headers=AUTH_HEADERS).json()
        assert len(jobs) == 1
        assert jobs[0]["error_code"] == "PROVIDER_TIMEOUT"


def test_webhook_rejects_unexpected_fields() -> None:
    for client in _make_client(MockTriageProvider()):
        resp = client.post(
            "/v1/events/triage",
            headers={**AUTH_HEADERS, "Idempotency-Key": "evt-bad-schema"},
            json={
                "source": "jira",
                "title": "Valid",
                "body": "Valid",
                "unexpected": "reject-me",
            },
        )
        assert resp.status_code == 422
