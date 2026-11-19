import hmac
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response, status
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from agent_ops_gateway.config import Settings
from agent_ops_gateway.db import Base, build_engine, build_session_factory
from agent_ops_gateway.provider import TriageProvider, build_provider
from agent_ops_gateway.schemas import (
    DLQJobResponse,
    HealthResponse,
    MetricsResponse,
    TriageResponse,
    WebhookEventRequest,
)
from agent_ops_gateway.service import (
    DLQAlreadyReplayedError,
    DLQNotFoundError,
    DLQReplayFailedError,
    GatewayService,
    IdempotencyConflictError,
    MetricsStore,
)


def create_app(
    settings: Settings | None = None,
    provider: TriageProvider | None = None,
) -> FastAPI:
    cfg = settings or Settings()
    engine = build_engine(cfg.database_url)
    session_factory = build_session_factory(engine)
    active_provider = provider or build_provider(cfg)
    metrics = MetricsStore()
    service = GatewayService(cfg, active_provider, metrics)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        Base.metadata.create_all(bind=engine)
        yield
        engine.dispose()

    app = FastAPI(
        title="Agent Ops Gateway",
        version="0.1.0",
        lifespan=lifespan,
    )

    def get_db() -> Iterator[Session]:
        db = session_factory()
        try:
            yield db
        finally:
            db.close()

    def verify_api_key(
        x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
    ) -> None:
        if not x_api_key or not hmac.compare_digest(x_api_key, cfg.api_key):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing X-API-Key header.",
            )

    DBSession = Annotated[Session, Depends(get_db)]
    AuthRequired = Depends(verify_api_key)

    @app.get("/health", response_model=HealthResponse)
    def health(db: DBSession) -> HealthResponse:
        try:
            db.execute(text("SELECT 1"))
        except SQLAlchemyError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Database unreachable: {exc}",
            ) from exc
        return HealthResponse(status="ok", database="connected", llm_mode=cfg.llm_mode)

    @app.get("/metrics", response_model=MetricsResponse)
    def get_metrics(db: DBSession) -> MetricsResponse:
        return service.get_metrics(db)

    @app.post(
        "/v1/events/triage",
        response_model=TriageResponse,
        dependencies=[AuthRequired],
    )
    def triage_event(
        payload: WebhookEventRequest,
        response: Response,
        db: DBSession,
        idempotency_key: Annotated[
            str,
            Header(alias="Idempotency-Key", min_length=1, max_length=128),
        ],
    ) -> TriageResponse:
        try:
            result, code = service.process_event(db, idempotency_key.strip(), payload)
        except IdempotencyConflictError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=str(exc),
            ) from exc
        response.status_code = code
        return result

    @app.get(
        "/v1/dlq",
        response_model=list[DLQJobResponse],
        dependencies=[AuthRequired],
    )
    def list_dlq(
        db: DBSession,
        job_status: Annotated[str | None, Query(alias="status")] = None,
    ) -> list[DLQJobResponse]:
        return service.list_dlq(db, status=job_status)

    @app.post(
        "/v1/dlq/{job_id}/replay",
        response_model=TriageResponse,
        dependencies=[AuthRequired],
    )
    def replay_dlq(job_id: str, db: DBSession) -> TriageResponse:
        try:
            return service.replay_dlq_job(db, job_id)
        except DLQNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except DLQAlreadyReplayedError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except DLQReplayFailedError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"{exc.code}: {exc.detail}",
            ) from exc

    return app


app = create_app()
