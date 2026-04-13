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
from platformd.scope_loader import load_scope
from platformd.config import load_daemon_config
from platformd.identities import Identities, UnknownPeerError, load_identities
from platformd.scope_store import ScopeNotFoundError, ScopeStore
from platformd.server import Server
from platformd.session import RECORD_MAX_BLOCKS, EnforcingSession, RecordingSession, Session


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


@pytest.mark.parametrize(
    "bad_service_id",
    [
        "../admin",
        "photoshare/../admin",
        "/etc/passwd",
        "",
        ".hidden",
        "photoshare.recorded",  # collision with the record-mode suffix
    ],
)
def test_identities_rejects_unsafe_service_id(
    tmp_path: Path, bad_service_id: str
) -> None:
    """service_id is composed into filesystem paths by ScopeStore and the
    recording-output path. Unsafe values must be rejected at load time
    so no downstream code has to re-validate."""
    p = _write(
        tmp_path,
        "identities.toml",
        f'[[identities]]\nuid = 501\nservice_id = "{bad_service_id}"\n',
    )
    with pytest.raises(ValueError, match="not a safe identifier"):
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
        EnforcingSession("demo", FakeEngine(), scope)


def test_session_enforces_scope() -> None:
    scope = ServiceScope(
        service_id="demo",
        allowed_blocks={BlockType.TRANSACTIONAL_STORE},
    )
    session = EnforcingSession("demo", FakeEngine(), scope)
    with pytest.raises(UnknownBlockError):
        session.acquire(BlockType.OBJECT_STORE, name="images")


def test_session_drop_blocks_subsequent_acquire() -> None:
    session = EnforcingSession("demo", FakeEngine(), _scope())
    session.acquire(BlockType.TRANSACTIONAL_STORE, name="db", database="db")
    session.drop_to_scaling_only()
    with pytest.raises(PrivilegeDroppedError):
        session.acquire(BlockType.OBJECT_STORE, name="images")


# --- record mode ---

def test_session_subclass_constructors_enforce_invariants() -> None:
    """EnforcingSession requires a scope; RecordingSession requires an output
    path. These are enforced by the constructor signatures — no mode string."""
    # EnforcingSession without scope → TypeError (positional arg missing)
    with pytest.raises(TypeError):
        EnforcingSession("demo", FakeEngine())  # type: ignore[call-arg]
    # RecordingSession without output → TypeError
    with pytest.raises(TypeError):
        RecordingSession("demo", FakeEngine())  # type: ignore[call-arg]


def test_session_record_mode_accumulates_and_writes_on_drop(tmp_path: Path) -> None:
    out = tmp_path / "demo.recorded.toml"
    engine = FakeEngine()
    session = RecordingSession("demo", engine, out)
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
    session = RecordingSession("demo", FakeEngine(), out)
    session.acquire(BlockType.TRANSACTIONAL_STORE, name="db", database="db")
    session.shutdown()

    assert out.is_file()
    parsed = tomllib.loads(out.read_text())
    assert parsed["allowed_blocks"] == ["transactional-store"]


def test_session_record_mode_drop_then_shutdown_writes_once(tmp_path: Path) -> None:
    """drop_to_scaling_only() writes the recording; a subsequent shutdown()
    must not write again (the _recording_written guard), so the operator
    sees exactly one authoritative draft per recording run."""
    out = tmp_path / "demo.recorded.toml"
    session = RecordingSession("demo", FakeEngine(), out)
    session.acquire(BlockType.TRANSACTIONAL_STORE, name="db", database="db")
    session.drop_to_scaling_only()
    first = out.read_text()
    session.shutdown()
    second = out.read_text()
    assert first == second, "shutdown must not rewrite the recording after drop"


def test_session_record_mode_no_acquires_skips_write(tmp_path: Path) -> None:
    out = tmp_path / "demo.recorded.toml"
    session = RecordingSession("demo", FakeEngine(), out)
    session.drop_to_scaling_only()
    assert not out.exists()


def test_session_record_mode_ceiling_enforced(tmp_path: Path) -> None:
    """Record mode still has a hard ceiling so a runaway service cannot
    drive arbitrary resource usage just because an operator opted in."""
    out = tmp_path / "demo.recorded.toml"
    session = RecordingSession("demo", FakeEngine(), out)
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


def _run_server(
    tmp_path: Path, *, record_for: set[str], scope_files: dict[str, str]
) -> tuple[Server, Path, Path, dict[str, FakeEngine], threading.Thread]:
    """Spin up a Server wired to tmp_path with the given mode configuration.

    record_for    — set of service_ids to run in record mode.
    scope_files   — service_id → TOML contents for scope files to place
                    on disk before the server starts.

    Returns the running server, the socket path, the scope dir, the
    engine registry, and the serving thread so the caller can stop()
    cleanly.
    """
    short_sock = Path(f"/tmp/nalsd-ptd-s9-{uuid.uuid4().hex[:8]}.sock")
    scope_dir = tmp_path / "scopes"
    scope_dir.mkdir(exist_ok=True)
    for svc, body in scope_files.items():
        (scope_dir / f"{svc}.toml").write_text(body)

    cfg_lines = [
        f'socket_path = "{short_sock}"',
        'scope_dir = "scopes"',
        'identities_path = "identities.toml"',
    ]
    for svc in record_for:
        cfg_lines.append("")
        cfg_lines.append(f"[service.{svc}]")
        cfg_lines.append('mode = "record"')
    cfg_path = tmp_path / "platformd.toml"
    cfg_path.write_text("\n".join(cfg_lines) + "\n")
    (tmp_path / "identities.toml").write_text(
        f'[[identities]]\nuid = {os.getuid()}\nservice_id = "photoshare"\n'
    )

    config = load_daemon_config(cfg_path)
    identities = Identities(by_uid={os.getuid(): "photoshare"})
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
    return server, config.socket_path, scope_dir, engines, thread


def test_full_record_then_enforce_flow(tmp_path: Path) -> None:
    """The Stage 9 bootstrap story: a new service comes up under a
    recording daemon, acquires whatever it needs, drops; the operator
    reviews the recorded scope, renames it to promote, flips the daemon
    to enforce; the service reconnects and the daemon now honours the
    scope as a hard authorization boundary — the same acquires succeed,
    a NEW acquire outside the recorded scope is rejected.

    This exercises every Stage 8 seam end-to-end against a real UDS
    with a FakeEngine. The Docker E2E in test_e2e_privilege_drop.py
    covers the in-process path; this is the split-topology equivalent
    for the record→promote→enforce lifecycle."""

    # --- Phase 1: record ---
    server, sock_path, scope_dir, _, thread = _run_server(
        tmp_path, record_for={"photoshare"}, scope_files={}
    )
    try:
        conn = _ClientConn(sock_path)
        try:
            r1 = conn.call(
                "Acquire",
                {"block_type": "transactional-store", "name": "photos"},
            )
            assert "error" not in r1, r1
            r2 = conn.call("DropToScalingOnly")
            assert "error" not in r2, r2
        finally:
            conn.close()
    finally:
        server.stop()
        thread.join(timeout=2)

    recorded = scope_dir / "photoshare.recorded.toml"
    assert recorded.is_file()
    # Operator reviews, then promotes by renaming.
    promoted = scope_dir / "photoshare.toml"
    recorded.rename(promoted)
    assert not recorded.exists()
    assert promoted.is_file()

    # --- Phase 2: enforce (new daemon, same scope dir) ---
    server2, sock_path2, _, _, thread2 = _run_server(
        tmp_path, record_for=set(), scope_files={}
    )
    try:
        conn = _ClientConn(sock_path2)
        try:
            # Within recorded scope — allowed.
            ok = conn.call(
                "Acquire",
                {"block_type": "transactional-store", "name": "photos"},
            )
            assert "error" not in ok, ok

            # Outside recorded scope — must be rejected. object-store
            # was never seen during recording, so enforce must refuse.
            rejected = conn.call(
                "Acquire",
                {"block_type": "object-store", "name": "images"},
            )
            assert rejected["error"]["code"] == "unknown_block", rejected
        finally:
            conn.close()
    finally:
        server2.stop()
        thread2.join(timeout=2)
