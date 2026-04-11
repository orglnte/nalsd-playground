from __future__ import annotations

import json
import os
import socket
import tempfile
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

import tomllib

from platform_api import (
    BlockSpec,
    BlockType,
    Credentials,
    PrivilegeDroppedError,
    QuotaExceededError,
    ServiceScope,
    UnknownBlockError,
)
from platform_api.scope import load_scope
from platformd.config import load_daemon_config
from platformd.identities import Identities, UnknownPeerError, load_identities
from platformd.scope_store import ScopeNotFoundError, ScopeStore
from platformd.server import Server
from platformd.session import RECORD_MAX_BLOCKS, Session


@dataclass
class FakeEngine:
    provisioned: list[BlockSpec] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.provisioned is None:
            self.provisioned = []

    def provision(
        self, spec: BlockSpec, *, existing_leases: dict[str, BlockSpec]
    ) -> Credentials:
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


# --- identities ---

def _write(tmp_path: Path, name: str, body: str) -> Path:
    p = tmp_path / name
    p.write_text(body)
    return p


def test_identities_load_and_lookup(tmp_path: Path) -> None:
    p = _write(
        tmp_path,
        "identities.toml",
        """
[[identities]]
uid = 501
service_id = "photoshare"

[[identities]]
uid = 1000
service_id = "billing"
""",
    )
    ids = load_identities(p)
    assert ids.service_for_uid(501) == "photoshare"
    assert ids.service_for_uid(1000) == "billing"


def test_identities_unknown_uid_raises(tmp_path: Path) -> None:
    p = _write(
        tmp_path,
        "identities.toml",
        '[[identities]]\nuid = 501\nservice_id = "photoshare"\n',
    )
    ids = load_identities(p)
    with pytest.raises(UnknownPeerError):
        ids.service_for_uid(99)


def test_identities_rejects_duplicate_uid(tmp_path: Path) -> None:
    p = _write(
        tmp_path,
        "identities.toml",
        """
[[identities]]
uid = 501
service_id = "a"

[[identities]]
uid = 501
service_id = "b"
""",
    )
    with pytest.raises(ValueError, match="duplicate uid"):
        load_identities(p)


def test_identities_rejects_empty(tmp_path: Path) -> None:
    p = _write(tmp_path, "identities.toml", "")
    with pytest.raises(ValueError, match="non-empty"):
        load_identities(p)


# --- scope store ---

def test_scope_store_loads_by_service_id(tmp_path: Path) -> None:
    scope_dir = tmp_path / "scopes"
    scope_dir.mkdir()
    (scope_dir / "photoshare.toml").write_text(
        'service_id = "photoshare"\n'
        'allowed_blocks = ["transactional-store"]\n'
    )
    store = ScopeStore(scope_dir=scope_dir)
    scope = store.get("photoshare")
    assert scope.service_id == "photoshare"


def test_scope_store_missing_file(tmp_path: Path) -> None:
    store = ScopeStore(scope_dir=tmp_path)
    with pytest.raises(ScopeNotFoundError):
        store.get("nobody")


def test_scope_store_rejects_service_id_mismatch(tmp_path: Path) -> None:
    """A scope file on disk declaring a different service_id is a
    deployment error — the store rejects it rather than silently trusting
    the filename OR the file's content."""
    (tmp_path / "photoshare.toml").write_text(
        'service_id = "other"\n'
        'allowed_blocks = ["transactional-store"]\n'
    )
    store = ScopeStore(scope_dir=tmp_path)
    with pytest.raises(ValueError, match="expected 'photoshare'"):
        store.get("photoshare")


# --- daemon config ---

def test_daemon_config_defaults_to_enforce(tmp_path: Path) -> None:
    cfg_path = tmp_path / "platformd.toml"
    cfg_path.write_text(
        'socket_path = "run/platformd.sock"\n'
        'scope_dir = "scopes"\n'
        'identities_path = "identities.toml"\n'
    )
    config = load_daemon_config(cfg_path)
    assert config.mode_for("anything") == "enforce"
    assert config.socket_path == (tmp_path / "run/platformd.sock").resolve()


def test_daemon_config_parses_service_modes(tmp_path: Path) -> None:
    cfg_path = tmp_path / "platformd.toml"
    cfg_path.write_text(
        'socket_path = "x.sock"\n'
        'scope_dir = "s"\n'
        'identities_path = "i.toml"\n'
        "\n[service.photoshare]\nmode = \"record\"\n"
    )
    config = load_daemon_config(cfg_path)
    assert config.mode_for("photoshare") == "record"
    assert config.mode_for("other") == "enforce"


def test_daemon_config_rejects_invalid_mode(tmp_path: Path) -> None:
    cfg_path = tmp_path / "platformd.toml"
    cfg_path.write_text(
        'socket_path = "x.sock"\n'
        'scope_dir = "s"\n'
        'identities_path = "i.toml"\n'
        "\n[service.photoshare]\nmode = \"audit\"\n"
    )
    with pytest.raises(ValueError, match="invalid mode"):
        load_daemon_config(cfg_path)


# --- session ---

def _scope(**overrides: Any) -> ServiceScope:
    defaults: dict[str, Any] = {
        "service_id": "demo",
        "allowed_blocks": {BlockType.TRANSACTIONAL_STORE, BlockType.OBJECT_STORE},
        "max_blocks": 4,
    }
    defaults.update(overrides)
    return ServiceScope(**defaults)


def test_session_scope_must_match_service_id() -> None:
    scope = ServiceScope(service_id="other", allowed_blocks={BlockType.OBJECT_STORE})
    with pytest.raises(ValueError, match="does not match"):
        Session(service_id="demo", scope=scope, engine=FakeEngine())


def test_session_enforces_scope() -> None:
    scope = ServiceScope(
        service_id="demo",
        allowed_blocks={BlockType.TRANSACTIONAL_STORE},
    )
    session = Session(service_id="demo", scope=scope, engine=FakeEngine())
    with pytest.raises(UnknownBlockError):
        session.acquire(BlockType.OBJECT_STORE, name="images")


def test_session_drop_blocks_subsequent_acquire() -> None:
    session = Session(service_id="demo", scope=_scope(), engine=FakeEngine())
    session.acquire(BlockType.TRANSACTIONAL_STORE, name="db", database="db")
    session.drop_to_scaling_only()
    with pytest.raises(PrivilegeDroppedError):
        session.acquire(BlockType.OBJECT_STORE, name="images")


# --- record mode ---

def test_session_record_mode_requires_output_path() -> None:
    with pytest.raises(ValueError, match="record mode requires"):
        Session(service_id="demo", engine=FakeEngine(), mode="record")


def test_session_record_mode_rejects_unknown_mode() -> None:
    with pytest.raises(ValueError, match="unknown session mode"):
        Session(
            service_id="demo",
            engine=FakeEngine(),
            scope=_scope(),
            mode="audit",
        )


def test_session_record_mode_enforce_mode_still_requires_scope() -> None:
    with pytest.raises(ValueError, match="enforce mode requires"):
        Session(service_id="demo", engine=FakeEngine(), mode="enforce")


def test_session_record_mode_accumulates_and_writes_on_drop(tmp_path: Path) -> None:
    out = tmp_path / "demo.recorded.toml"
    engine = FakeEngine()
    session = Session(
        service_id="demo",
        engine=engine,
        mode="record",
        recording_output=out,
    )
    # No scope required and no BlockType restriction — record captures all.
    session.acquire(BlockType.TRANSACTIONAL_STORE, name="db", database="db")
    session.acquire(BlockType.OBJECT_STORE, name="images")
    session.acquire(BlockType.EPHEMERAL_KV_CACHE, name="cache")
    session.drop_to_scaling_only()

    assert out.is_file()
    parsed = tomllib.loads(out.read_text())
    assert parsed["service_id"] == "demo"
    assert set(parsed["allowed_blocks"]) == {
        "transactional-store",
        "object-store",
        "ephemeral-kv-cache",
    }
    assert parsed["max_blocks"] == 3

    # Header comments must be present so the operator knows it's a draft.
    text = out.read_text()
    assert "recorded by platformd" in text
    assert "rename to demo.toml" in text


def test_session_record_mode_writes_on_shutdown_without_drop(tmp_path: Path) -> None:
    """A service that crashes or disconnects without dropping still leaves
    a usable recording — the operator can see what it asked for before
    the crash."""
    out = tmp_path / "demo.recorded.toml"
    session = Session(
        service_id="demo",
        engine=FakeEngine(),
        mode="record",
        recording_output=out,
    )
    session.acquire(BlockType.TRANSACTIONAL_STORE, name="db", database="db")
    session.shutdown()

    assert out.is_file()
    parsed = tomllib.loads(out.read_text())
    assert parsed["allowed_blocks"] == ["transactional-store"]


def test_session_record_mode_no_acquires_skips_write(tmp_path: Path) -> None:
    out = tmp_path / "demo.recorded.toml"
    session = Session(
        service_id="demo",
        engine=FakeEngine(),
        mode="record",
        recording_output=out,
    )
    session.drop_to_scaling_only()
    assert not out.exists()


def test_session_record_mode_ceiling_enforced(tmp_path: Path) -> None:
    """Record mode still has a hard ceiling so a runaway service cannot
    drive arbitrary resource usage just because an operator opted in."""
    out = tmp_path / "demo.recorded.toml"
    session = Session(
        service_id="demo",
        engine=FakeEngine(),
        mode="record",
        recording_output=out,
    )
    for i in range(RECORD_MAX_BLOCKS):
        session.acquire(BlockType.TRANSACTIONAL_STORE, name=f"db{i}", database="d")
    with pytest.raises(QuotaExceededError, match="record-mode ceiling"):
        session.acquire(BlockType.TRANSACTIONAL_STORE, name="one-too-many")


def test_recorded_file_does_not_auto_load_as_scope(tmp_path: Path) -> None:
    """Even if the daemon writes photoshare.recorded.toml into the scope
    directory, ScopeStore must not pick it up. Promotion is explicit:
    operator renames it to photoshare.toml."""
    scope_dir = tmp_path / "scopes"
    scope_dir.mkdir()
    (scope_dir / "photoshare.recorded.toml").write_text(
        'service_id = "photoshare"\n'
        'allowed_blocks = ["transactional-store"]\n'
    )
    store = ScopeStore(scope_dir=scope_dir)
    with pytest.raises(ScopeNotFoundError):
        store.get("photoshare")


# --- server end-to-end over a real UDS, with a fake engine ---

class _ClientConn:
    def __init__(self, sock_path: Path) -> None:
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.connect(str(sock_path))
        self._f = self._sock.makefile("rwb", buffering=0)

    def call(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        request = {"id": 1, "method": method, "params": params or {}}
        self._sock.sendall((json.dumps(request) + "\n").encode("utf-8"))
        line = self._f.readline()
        return json.loads(line.decode("utf-8"))

    def close(self) -> None:
        self._f.close()
        self._sock.close()


@pytest.fixture
def running_server(tmp_path: Path):
    # macOS sun_path is limited to 104 bytes — pytest's tmp_path is too
    # deep. Put the socket in a short /tmp path and keep the scopes/config
    # under pytest's tmp_path as usual.
    short_sock = Path(f"/tmp/nalsd-ptd-{uuid.uuid4().hex[:8]}.sock")

    scope_dir = tmp_path / "scopes"
    scope_dir.mkdir()
    (scope_dir / "demo.toml").write_text(
        'service_id = "demo"\n'
        'allowed_blocks = ["transactional-store", "object-store"]\n'
        "max_blocks = 4\n"
    )
    cfg_path = tmp_path / "platformd.toml"
    cfg_path.write_text(
        f'socket_path = "{short_sock}"\n'
        f'scope_dir = "scopes"\n'
        f'identities_path = "identities.toml"\n'
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
        yield config.socket_path, engines
    finally:
        server.stop()
        thread.join(timeout=2)


def test_server_acquire_over_uds(running_server) -> None:
    sock_path, engines = running_server
    conn = _ClientConn(sock_path)
    try:
        resp = conn.call(
            "Acquire",
            {
                "block_type": "transactional-store",
                "name": "db",
                "params": {"database": "demo"},
            },
        )
    finally:
        conn.close()
    assert "error" not in resp, resp
    assert resp["result"]["block_type"] == "transactional-store"
    assert resp["result"]["name"] == "db"
    assert engines["demo"].provisioned[0].name == "db"


def test_server_rejects_unknown_block(running_server) -> None:
    sock_path, _ = running_server
    conn = _ClientConn(sock_path)
    try:
        # Scope allows transactional-store and object-store but not kv.
        resp = conn.call(
            "Acquire",
            {"block_type": "ephemeral-kv-cache", "name": "c"},
        )
    finally:
        conn.close()
    assert resp["error"]["code"] == "unknown_block"


def test_server_privilege_drop_is_per_connection(running_server) -> None:
    """Disconnect resets PrivilegeState — a new connection gets ACQUIRING
    again. This is deliberate: a service-process restart must re-enter
    the ACQUIRING phase so the v1 → v1.1 infrastructure-change demo works.
    Within a single connection, the daemon enforces the drop regardless
    of whatever the client reports."""
    sock_path, _ = running_server
    conn = _ClientConn(sock_path)
    try:
        conn.call("Acquire", {"block_type": "transactional-store", "name": "db"})
        conn.call("DropToScalingOnly")
        rejected = conn.call(
            "Acquire", {"block_type": "object-store", "name": "images"}
        )
        assert rejected["error"]["code"] == "privilege_dropped"
    finally:
        conn.close()

    # Reconnect — privilege state should reset.
    conn2 = _ClientConn(sock_path)
    try:
        resp = conn2.call(
            "Acquire", {"block_type": "object-store", "name": "images"}
        )
    finally:
        conn2.close()
    assert "error" not in resp, resp
    assert resp["result"]["name"] == "images"


@pytest.fixture
def recording_server(tmp_path: Path):
    """Daemon in record mode for service 'demo' — NO scope file on disk."""
    short_sock = Path(f"/tmp/nalsd-ptd-rec-{uuid.uuid4().hex[:8]}.sock")

    scope_dir = tmp_path / "scopes"
    scope_dir.mkdir()
    # Intentionally no demo.toml — record mode must not need one.

    cfg_path = tmp_path / "platformd.toml"
    cfg_path.write_text(
        f'socket_path = "{short_sock}"\n'
        f'scope_dir = "scopes"\n'
        f'identities_path = "identities.toml"\n'
        "\n[service.demo]\nmode = \"record\"\n"
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
        yield config.socket_path, scope_dir, engines
    finally:
        server.stop()
        thread.join(timeout=2)


def test_server_record_mode_end_to_end(recording_server) -> None:
    """Full loop: mode=record, no scope file, service connects, acquires
    blocks that would normally require a scope, drops — daemon writes
    demo.recorded.toml next to where the enforced scope would live."""
    sock_path, scope_dir, engines = recording_server
    conn = _ClientConn(sock_path)
    try:
        r1 = conn.call(
            "Acquire",
            {"block_type": "transactional-store", "name": "db"},
        )
        r2 = conn.call(
            "Acquire",
            {"block_type": "ephemeral-kv-cache", "name": "cache"},
        )
        r3 = conn.call("DropToScalingOnly")
    finally:
        conn.close()

    assert "error" not in r1, r1
    assert "error" not in r2, r2
    assert "error" not in r3, r3

    recorded = scope_dir / "demo.recorded.toml"
    assert recorded.is_file(), "daemon must write recorded scope on drop"

    # Parses as valid TOML and captures what the service actually did.
    parsed = tomllib.loads(recorded.read_text())
    assert parsed["service_id"] == "demo"
    assert set(parsed["allowed_blocks"]) == {
        "transactional-store",
        "ephemeral-kv-cache",
    }
    assert parsed["max_blocks"] == 2

    # The recorded file must be loadable as a ServiceScope — promotion
    # is just a rename operation.
    promoted = scope_dir / "demo.toml"
    promoted.write_text(recorded.read_text())
    scope = load_scope(promoted)
    assert scope.service_id == "demo"
    assert BlockType.TRANSACTIONAL_STORE in scope.allowed_blocks
    assert BlockType.EPHEMERAL_KV_CACHE in scope.allowed_blocks


def test_server_record_mode_then_promote_flow(recording_server) -> None:
    """After a recording run, the daemon session ends. Renaming the
    .recorded file to .toml and reconnecting should work — except this
    server is still configured with mode=record so it would still record.
    This test captures the scope-load step only."""
    sock_path, scope_dir, _ = recording_server
    conn = _ClientConn(sock_path)
    try:
        conn.call("Acquire", {"block_type": "object-store", "name": "images"})
        conn.call("DropToScalingOnly")
    finally:
        conn.close()

    recorded = scope_dir / "demo.recorded.toml"
    parsed = tomllib.loads(recorded.read_text())
    assert parsed["allowed_blocks"] == ["object-store"]
