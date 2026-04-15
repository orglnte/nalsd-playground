"""
platformd HTTP API.

FastAPI app with pydantic request/response models, so the wire contract
is self-documenting via the auto-generated OpenAPI spec at `/openapi.json`
and `/docs`. That spec is the coding-agent contract — not a separately
maintained document, but a by-product of the server definition.

Session model: `POST /sessions` creates a server-side `Session` keyed by
a bearer token. Subsequent RPCs (`POST /acquire`,
`POST /drop-to-scaling-only`, `POST /scale-hint`) carry the token in
`Authorization: Bearer`. Session state (ACQUIRING / OPERATIONAL /
SHUTDOWN) lives in the daemon's `Session` object keyed by that token —
identical semantics to the earlier socket-bound session, just without
the socket. A client reconnecting after a process restart obtains a
fresh session back in ACQUIRING state.

Trust boundary: the `POST /sessions` handler delegates to a
`BootstrapVerifier` (see `platformd.auth`); authenticated `service_id`
drives scope lookup, not anything the client provided at the RPC layer.
"""

from __future__ import annotations

import logging
import secrets
import threading
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any

import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field

from platform_api.errors import PlatformError
from platform_api.protocol import code_for
from platform_api.types import (
    BlockType,
    ComputeSpec,
    Credentials,
    Persistence,
    StorageSpec,
)
from platformd.auth import BootstrapVerifier, IdentityRejectedError, TrustingVerifier
from platformd.config import DaemonConfig
from platformd.engine_protocol import Engine
from platformd.identities import Identities
from platformd.scope_store import ScopeNotFoundError, ScopeStore
from platformd.session import EnforcingSession, RecordingSession, Session

log = logging.getLogger("platformd.server")

EngineFactory = Callable[[str], Engine]


# -- wire models --


class HelloRequest(BaseModel):
    service_id: str = Field(..., description="claimed service identity; verifier-checked")


class SessionCreated(BaseModel):
    session_id: str
    token: str = Field(..., description="bearer token for subsequent RPCs")
    state: str = Field(..., description="privilege state of the new session")


class ComputeModel(BaseModel):
    memory_mb: int


class StorageModel(BaseModel):
    size_mb: int | None = None
    persistence: str = Persistence.EPHEMERAL.value


class AcquireRequest(BaseModel):
    block_type: str
    name: str
    compute: ComputeModel | None = None
    storage: StorageModel | None = None
    rps: int | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class CredentialsResponse(BaseModel):
    block_type: str
    name: str
    host: str
    port: int
    username: str | None = None
    password: str | None = None
    database: str | None = None
    extras: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_credentials(cls, c: Credentials) -> CredentialsResponse:
        return cls(
            block_type=c.block_type.value,
            name=c.name,
            host=c.host,
            port=c.port,
            username=c.username,
            password=c.password,
            database=c.database,
            extras=dict(c.extras),
        )


class ScaleHintRequest(BaseModel):
    name: str
    load_factor: float


# -- session registry --


class SessionRegistry:
    """Server-side store of live sessions, keyed by an opaque bearer
    token. The token is a high-entropy random string; nothing meaningful
    is encoded in it. Sessions expire when explicitly closed
    (`DELETE /sessions`) or when the process restarts."""

    def __init__(self) -> None:
        self._sessions: dict[str, Session] = {}
        self._tokens: dict[str, str] = {}
        self._lock = threading.Lock()

    def register(self, session: Session) -> tuple[str, str]:
        session_id = f"sess_{uuid.uuid4().hex[:12]}"
        token = secrets.token_urlsafe(32)
        with self._lock:
            self._sessions[session_id] = session
            self._tokens[token] = session_id
        return session_id, token

    def get_by_token(self, token: str) -> Session:
        with self._lock:
            session_id = self._tokens.get(token)
            if session_id is None:
                raise KeyError("unknown session token")
            return self._sessions[session_id]

    def drop(self, token: str) -> None:
        with self._lock:
            session_id = self._tokens.pop(token, None)
            if session_id is None:
                return
            session = self._sessions.pop(session_id, None)
        if session is not None:
            try:
                session.shutdown()
            except Exception:
                log.exception("error during session shutdown")


# -- server class --


_HTTP_STATUS_FOR_CODE: dict[str, int] = {
    "privilege_dropped": status.HTTP_403_FORBIDDEN,
    "unknown_block": status.HTTP_403_FORBIDDEN,
    "quota_exceeded": status.HTTP_403_FORBIDDEN,
    "invalid_state": status.HTTP_409_CONFLICT,
    "invalid_request": status.HTTP_400_BAD_REQUEST,
    "provisioning": status.HTTP_502_BAD_GATEWAY,
    "readiness_timeout": status.HTTP_504_GATEWAY_TIMEOUT,
}


def _raise_as_http(exc: BaseException) -> None:
    code = code_for(exc)
    http_status = _HTTP_STATUS_FOR_CODE.get(code, status.HTTP_500_INTERNAL_SERVER_ERROR)
    raise HTTPException(http_status, detail={"code": code, "message": str(exc)})


class Server:
    """Owns the FastAPI app, session registry, per-service engines, and
    the uvicorn server lifecycle.

    `self.app` is exposed directly for in-process testing
    (`fastapi.testclient.TestClient`). `start() / serve_forever() / stop()`
    wrap uvicorn for tests and production that need a real HTTP port.
    """

    def __init__(
        self,
        config: DaemonConfig,
        identities: Identities,
        scope_store: ScopeStore,
        engine_factory: EngineFactory,
        verifier: BootstrapVerifier | None = None,
    ) -> None:
        self._config = config
        self._identities = identities
        self._scope_store = scope_store
        self._engine_factory = engine_factory
        self._engines: dict[str, Engine] = {}
        self._verifier = verifier or TrustingVerifier(identities.known)
        self._registry = SessionRegistry()
        self.app = self._build_app()
        self._uvicorn: uvicorn.Server | None = None

    def _engine_for(self, service_id: str) -> Engine:
        if service_id not in self._engines:
            self._engines[service_id] = self._engine_factory(service_id)
        return self._engines[service_id]

    def _session_for(self, service_id: str) -> Session:
        mode = self._config.mode_for(service_id)
        engine = self._engine_for(service_id)
        if mode == "record":
            recording_output = self._config.scope_dir / f"{service_id}.recorded.toml"
            return RecordingSession(service_id, engine, recording_output)
        scope = self._scope_store.get(service_id)
        return EnforcingSession(service_id, engine, scope)

    def _build_app(self) -> FastAPI:
        app = FastAPI(
            title="platformd",
            version="0.2.0",
            description=(
                "Runtime infrastructure-from-code control plane. "
                "Services call `POST /sessions` to authenticate (via a "
                "pluggable `BootstrapVerifier`), then `POST /acquire` to "
                "provision blocks until `POST /drop-to-scaling-only` "
                "irreversibly flips the session to OPERATIONAL."
            ),
        )

        def _bearer_session(
            authorization: Annotated[str | None, Header()] = None,
        ) -> Session:
            if not authorization or not authorization.lower().startswith("bearer "):
                raise HTTPException(
                    status.HTTP_401_UNAUTHORIZED,
                    detail={"code": "unauthenticated", "message": "missing Bearer token"},
                )
            token = authorization.split(None, 1)[1].strip()
            try:
                return self._registry.get_by_token(token)
            except KeyError as e:
                raise HTTPException(
                    status.HTTP_401_UNAUTHORIZED,
                    detail={"code": "unauthenticated", "message": "unknown session token"},
                ) from e

        def _bearer_token(
            authorization: Annotated[str | None, Header()] = None,
        ) -> str:
            if not authorization or not authorization.lower().startswith("bearer "):
                raise HTTPException(
                    status.HTTP_401_UNAUTHORIZED,
                    detail={"code": "unauthenticated", "message": "missing Bearer token"},
                )
            return authorization.split(None, 1)[1].strip()

        @app.post(
            "/sessions",
            response_model=SessionCreated,
            status_code=status.HTTP_201_CREATED,
            summary="Create a platform session",
        )
        def create_session(req: HelloRequest) -> SessionCreated:
            try:
                service_id = self._verifier.verify({"service_id": req.service_id})
            except IdentityRejectedError as e:
                raise HTTPException(
                    status.HTTP_401_UNAUTHORIZED,
                    detail={"code": "identity_rejected", "message": str(e)},
                ) from e
            try:
                session = self._session_for(service_id)
            except (ScopeNotFoundError, ValueError) as e:
                log.warning("session creation rejected for %s: %s", service_id, e)
                raise HTTPException(
                    status.HTTP_403_FORBIDDEN,
                    detail={"code": "invalid_request", "message": str(e)},
                ) from e
            session_id, token = self._registry.register(session)
            log.info(
                "session start: service=%s session=%s mode=%s",
                service_id,
                session_id,
                self._config.mode_for(service_id),
            )
            return SessionCreated(
                session_id=session_id,
                token=token,
                state=session.state.value,
            )

        @app.post(
            "/acquire",
            response_model=CredentialsResponse,
            summary="Acquire a platform block",
        )
        def acquire(
            req: AcquireRequest,
            session: Session = Depends(_bearer_session),  # noqa: B008
        ) -> CredentialsResponse:
            try:
                spec_compute = (
                    ComputeSpec(memory_mb=req.compute.memory_mb) if req.compute else None
                )
                spec_storage = (
                    StorageSpec(
                        size_mb=req.storage.size_mb,
                        persistence=Persistence(req.storage.persistence),
                    )
                    if req.storage
                    else None
                )
                creds = session.acquire(
                    BlockType(req.block_type),
                    name=req.name,
                    compute=spec_compute,
                    storage=spec_storage,
                    rps=req.rps,
                    **req.params,
                )
            except PlatformError as e:
                _raise_as_http(e)
            except ValueError as e:
                _raise_as_http(e)
            return CredentialsResponse.from_credentials(creds)

        @app.post(
            "/drop-to-scaling-only",
            status_code=status.HTTP_204_NO_CONTENT,
            summary="Drop to scaling-only privilege state",
        )
        def drop_to_scaling_only(
            session: Session = Depends(_bearer_session),  # noqa: B008
        ) -> None:
            try:
                session.drop_to_scaling_only()
            except PlatformError as e:
                _raise_as_http(e)

        @app.post(
            "/scale-hint",
            status_code=status.HTTP_204_NO_CONTENT,
            summary="Hint to the daemon that a block's load is changing",
        )
        def scale_hint(
            req: ScaleHintRequest,
            session: Session = Depends(_bearer_session),  # noqa: B008
        ) -> None:
            try:
                session.scale_hint(req.name, load_factor=req.load_factor)
            except PlatformError as e:
                _raise_as_http(e)
            except ValueError as e:
                _raise_as_http(e)

        @app.delete(
            "/sessions",
            status_code=status.HTTP_204_NO_CONTENT,
            summary="End the current session",
        )
        def close_session(
            token: str = Depends(_bearer_token),
        ) -> None:
            # DELETE intentionally does not use the session dependency —
            # we're removing the token, and a 401 here would be misleading
            # if the client just disconnected with an already-stale token.
            self._registry.drop(token)

        return app

    def start(self) -> None:
        config = uvicorn.Config(
            self.app,
            host=self._config.listen_host,
            port=self._config.listen_port,
            log_level="info",
            access_log=False,
            lifespan="off",
        )
        self._uvicorn = uvicorn.Server(config)

    def serve_forever(self) -> None:
        assert self._uvicorn is not None, "start() must be called first"
        log.info(
            "platformd listening on %s:%d",
            self._config.listen_host,
            self._config.listen_port,
        )
        self._uvicorn.run()

    def stop(self) -> None:
        if self._uvicorn is not None:
            self._uvicorn.should_exit = True


def default_engine_factory(service_id: str) -> Engine:
    from platformd.engine import PulumiDockerEngine

    return PulumiDockerEngine(service_id=service_id)


def build_server(config_path: Path) -> Server:
    from platformd.config import load_daemon_config
    from platformd.identities import load_identities

    config = load_daemon_config(config_path)
    identities = load_identities(config.identities_path)
    scope_store = ScopeStore(scope_dir=config.scope_dir)
    return Server(
        config=config,
        identities=identities,
        scope_store=scope_store,
        engine_factory=default_engine_factory,
    )
