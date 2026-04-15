"""Tests for platform_api.Client against the FastAPI platformd app.

The Client takes an injected `httpx.Client` so tests can feed in an
`httpx.Client` backed by `ASGITransport`, which calls the FastAPI app
in-process — no threads, no sockets. Same client code path as production,
same HTTP wire shape."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from platform_api import (
    BlockSpec,
    BlockType,
    Client,
    Credentials,
    InvalidStateError,
    PrivilegeDroppedError,
    UnknownBlockError,
)
from platformd.config import load_daemon_config
from platformd.identities import load_identities
from platformd.scope_store import ScopeStore
from platformd.server import Server


@dataclass
class FakeEngine:
    provisioned: list[BlockSpec] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.provisioned is None:
            self.provisioned = []

    def provision(self, spec: BlockSpec, *, existing_leases: dict[str, BlockSpec]) -> Credentials:
        self.provisioned.append(spec)
        return Credentials(
            block_type=spec.block_type,
            name=spec.name,
            host="127.0.0.1",
            port=10000 + len(self.provisioned),
            username="u",
            password="p",
            database="d" if spec.block_type == BlockType.TRANSACTIONAL_STORE else None,
        )

    def destroy(self) -> None:
        pass


@pytest.fixture
def daemon(tmp_path: Path):
    scope_dir = tmp_path / "scopes"
    scope_dir.mkdir()
    (scope_dir / "demo.toml").write_text(
        'service_id = "demo"\n'
        'allowed_blocks = ["transactional-store", "object-store"]\n'
        "max_blocks = 4\n"
    )
    cfg_path = tmp_path / "platformd.toml"
    cfg_path.write_text(
        'listen_address = "127.0.0.1:0"\n'
        'scope_dir = "scopes"\n'
        'identities_path = "identities.toml"\n'
    )
    (tmp_path / "identities.toml").write_text(
        '[[identities]]\nservice_id = "demo"\n'
    )

    config = load_daemon_config(cfg_path)
    identities = load_identities(config.identities_path)
    store = ScopeStore(scope_dir=config.scope_dir)
    engines: dict[str, FakeEngine] = {}

    def factory(service_id: str) -> FakeEngine:
        engines.setdefault(service_id, FakeEngine())
        return engines[service_id]

    server = Server(
        config=config,
        identities=identities,
        scope_store=store,
        engine_factory=factory,
    )

    http = TestClient(server.app)
    try:
        yield http, engines
    finally:
        http.close()


def _client(http: TestClient, service_id: str = "demo") -> Client:
    return Client(service_id, base_url="http://testserver", http_client=http)


def test_client_acquire_round_trip(daemon) -> None:
    http, engines = daemon
    with _client(http) as client:
        creds = client.acquire(BlockType.TRANSACTIONAL_STORE, name="photos", database="photos")
    assert creds.block_type is BlockType.TRANSACTIONAL_STORE
    assert creds.name == "photos"
    assert engines["demo"].provisioned[0].name == "photos"
    assert engines["demo"].provisioned[0].params == {"database": "photos"}


def test_client_decodes_unknown_block_error(daemon) -> None:
    http, _ = daemon
    with _client(http) as client, pytest.raises(UnknownBlockError):
        client.acquire(BlockType.EPHEMERAL_KV_CACHE, name="cache")


def test_daemon_rejects_acquire_after_drop(daemon) -> None:
    """The client holds no PrivilegeState of its own — every call round-trips
    and the daemon is the sole authority. This test proves the daemon
    enforces the drop: acquire → drop → acquire, second call rejected by
    the daemon with PrivilegeDroppedError, and nothing was provisioned
    for it."""
    http, engines = daemon
    with _client(http) as client:
        client.acquire(BlockType.TRANSACTIONAL_STORE, name="db")
        client.drop_to_scaling_only()
        with pytest.raises(PrivilegeDroppedError):
            client.acquire(BlockType.OBJECT_STORE, name="images")
    provisioned_names = [spec.name for spec in engines["demo"].provisioned]
    assert provisioned_names == ["db"]


def test_client_scale_hint_requires_drop(daemon) -> None:
    http, _ = daemon
    with _client(http) as client:
        client.acquire(BlockType.TRANSACTIONAL_STORE, name="db")
        with pytest.raises(InvalidStateError):
            client.scale_hint("db", load_factor=0.5)
        client.drop_to_scaling_only()
        client.scale_hint("db", load_factor=0.5)  # no raise


def test_client_string_block_type_accepted(daemon) -> None:
    http, _ = daemon
    with _client(http) as client:
        creds = client.acquire("object-store", name="images")
    assert creds.block_type is BlockType.OBJECT_STORE


def test_new_session_resets_privilege_state(daemon) -> None:
    """A service that reconnects (new POST /sessions) starts back in
    ACQUIRING. Enables the restart-the-service demo where a new acquire()
    call lands after the first run."""
    http, _ = daemon
    with _client(http) as client:
        client.acquire(BlockType.TRANSACTIONAL_STORE, name="db")
        client.drop_to_scaling_only()
        with pytest.raises(PrivilegeDroppedError):
            client.acquire(BlockType.OBJECT_STORE, name="images")

    # New client = new session = ACQUIRING again.
    with _client(http) as client:
        creds = client.acquire(BlockType.OBJECT_STORE, name="images")
        assert creds.block_type is BlockType.OBJECT_STORE


def test_client_rejects_unknown_service_id_at_connect(daemon) -> None:
    from platform_api.errors import PlatformError

    http, _ = daemon
    client = _client(http, service_id="intruder")
    with pytest.raises(PlatformError, match="intruder"):
        client.connect()
