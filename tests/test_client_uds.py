"""Tests for platform_api.Client (UDS client) against a running daemon."""

from __future__ import annotations

import os
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest

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
from platformd.identities import Identities
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
    short_sock = Path(f"/tmp/nalsd-ptd-client-{uuid.uuid4().hex[:8]}.sock")

    scope_dir = tmp_path / "scopes"
    scope_dir.mkdir()
    (scope_dir / "demo.toml").write_text(
        'service_id = "demo"\n'
        'allowed_blocks = ["transactional-store", "object-store"]\n'
        "max_blocks = 4\n"
    )
    cfg_path = tmp_path / "platformd.toml"
    cfg_path.write_text(
        f'socket_path = "{short_sock}"\nscope_dir = "scopes"\nidentities_path = "identities.toml"\n'
    )
    (tmp_path / "identities.toml").write_text(
        f'[[identities]]\nuid = {os.getuid()}\nservice_id = "demo"\n'
    )

    config = load_daemon_config(cfg_path)
    identities = Identities(by_uid={os.getuid(): "demo"})
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
    server.start()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield short_sock, engines
    finally:
        server.stop()
        thread.join(timeout=2)


def test_client_acquire_round_trip(daemon) -> None:
    sock_path, engines = daemon
    with Client("demo", socket_path=sock_path) as client:
        creds = client.acquire(BlockType.TRANSACTIONAL_STORE, name="photos", database="photos")
    assert creds.block_type is BlockType.TRANSACTIONAL_STORE
    assert creds.name == "photos"
    assert engines["demo"].provisioned[0].name == "photos"
    assert engines["demo"].provisioned[0].params == {"database": "photos"}


def test_client_decodes_unknown_block_error(daemon) -> None:
    sock_path, _ = daemon
    with Client("demo", socket_path=sock_path) as client, pytest.raises(UnknownBlockError):
        client.acquire(BlockType.EPHEMERAL_KV_CACHE, name="cache")


def test_daemon_rejects_acquire_after_drop(daemon) -> None:
    """The client holds no PrivilegeState of its own — every call round-trips
    and the daemon is the sole authority. This test proves the daemon
    enforces the drop: acquire → drop → acquire, second call rejected by
    the daemon with PrivilegeDroppedError, and nothing was provisioned
    for it."""
    sock_path, engines = daemon
    with Client("demo", socket_path=sock_path) as client:
        client.acquire(BlockType.TRANSACTIONAL_STORE, name="db")
        client.drop_to_scaling_only()
        with pytest.raises(PrivilegeDroppedError):
            client.acquire(BlockType.OBJECT_STORE, name="images")
    provisioned_names = [spec.name for spec in engines["demo"].provisioned]
    assert provisioned_names == ["db"]


def test_client_scale_hint_requires_drop(daemon) -> None:
    sock_path, _ = daemon
    with Client("demo", socket_path=sock_path) as client:
        client.acquire(BlockType.TRANSACTIONAL_STORE, name="db")
        with pytest.raises(InvalidStateError):
            client.scale_hint("db", load_factor=0.5)
        client.drop_to_scaling_only()
        client.scale_hint("db", load_factor=0.5)  # no raise


def test_client_string_block_type_accepted(daemon) -> None:
    sock_path, _ = daemon
    with Client("demo", socket_path=sock_path) as client:
        creds = client.acquire("object-store", name="images")
    assert creds.block_type is BlockType.OBJECT_STORE
