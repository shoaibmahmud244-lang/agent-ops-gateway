import json
from typing import Protocol

import httpx

from agent_ops_gateway.config import Settings
from agent_ops_gateway.schemas import WebhookEventRequest


class ProviderError(Exception):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


class TriageProvider(Protocol):
    def triage(self, event: WebhookEventRequest) -> str:
        """Return a raw JSON string conforming to LLMTriageOutput."""
        ...


class MockTriageProvider:
    """Deterministic zero-key triage provider for local dev, CI, and demos."""

    def triage(self, event: WebhookEventRequest) -> str:
        text = f"{event.title} {event.body}".lower()
        if any(k in text for k in ("outage", "breach", "ransomware", "production down")):
            payload = {
                "severity": "critical",
                "recommended_queue": "secops-oncall",
                "confidence": 0.92,
                # Deliberately False so deterministic policy override is testable end-to-end.
                "requires_human_approval": False,
                "summary": f"Critical incident reported from {event.source}: {event.title}",
            }
        elif any(k in text for k in ("ambiguous", "unclear", "unsure")):
            payload = {
                "severity": "medium",
                "recommended_queue": "it-service-desk",
                "confidence": 0.45,
                "requires_human_approval": False,
                "summary": f"Low-confidence ticket from {event.source}: {event.title}",
            }
        elif any(k in text for k in ("invoice", "billing", "refund")):
            payload = {
                "severity": "low",
                "recommended_queue": "finance-ops",
                "confidence": 0.89,
                "requires_human_approval": False,
                "summary": f"Billing request from {event.source}: {event.title}",
            }
        else:
            payload = {
                "severity": "medium",
                "recommended_queue": "it-service-desk",
                "confidence": 0.84,
                "requires_human_approval": False,
                "summary": f"Operational ticket from {event.source}: {event.title}",
            }
        return json.dumps(payload)


class OpenAICompatibleProvider:
    """OpenAI-compatible JSON-mode provider with bounded timeouts."""

    def __init__(self, settings: Settings) -> None:
        if not settings.llm_api_key:
            raise ValueError("LLM_API_KEY must be set when LLM_MODE=openai")
        self._base_url = settings.llm_base_url.rstrip("/")
        self._api_key = settings.llm_api_key
        self._model = settings.llm_model
        self._timeout = settings.llm_timeout_seconds

    def triage(self, event: WebhookEventRequest) -> str:
        system_prompt = (
            "You are an IT operations triage classifier. Return ONLY a JSON object with keys: "
            "severity (low|medium|high|critical), recommended_queue (string), "
            "confidence (0.0 to 1.0), requires_human_approval (boolean), summary (string)."
        )
        body = {
            "model": self._model,
            "temperature": 0,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": event.model_dump_json()},
            ],
        }
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        try:
            with httpx.Client(timeout=self._timeout) as client:
                resp = client.post(
                    f"{self._base_url}/chat/completions",
                    headers=headers,
                    json=body,
                )
                resp.raise_for_status()
        except httpx.TimeoutException as exc:
            raise ProviderError("PROVIDER_TIMEOUT", str(exc)) from exc
        except httpx.HTTPStatusError as exc:
            raise ProviderError(
                f"PROVIDER_HTTP_{exc.response.status_code}",
                exc.response.text[:400],
            ) from exc
        except httpx.HTTPError as exc:
            raise ProviderError("PROVIDER_NETWORK_ERROR", str(exc)) from exc

        try:
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            if not isinstance(content, str):
                raise ProviderError("PROVIDER_BAD_ENVELOPE", "Model message content is not text")
            return content
        except (KeyError, IndexError, ValueError, TypeError) as exc:
            raise ProviderError("PROVIDER_BAD_ENVELOPE", str(exc)) from exc


def build_provider(settings: Settings) -> TriageProvider:
    if settings.llm_mode == "openai":
        return OpenAICompatibleProvider(settings)
    return MockTriageProvider()
