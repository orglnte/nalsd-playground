from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from platform_api import (
    BlockSpec,
    BlockType,
    Credentials,
    PrivilegeDroppedError,
    QuotaExceededError,
    ServiceScope,
    UnknownBlockError,
)
from platformd.auth import IdentityRejectedError, TrustingVerifier
from platformd.config import load_daemon_config
from platformd.identities import load_identities
from platformd.scope_loader import load_scope
from platformd.scope_store import ScopeNotFoundError, ScopeStore
from platformd.server import Server
from platformd.session import RECORD_MAX_BLOCKS, EnforcingSession, RecordingSession


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
service_id = "photoshare"

[[identities]]
service_id = "billing"
""",
    )
    ids = load_identities(p)
    assert ids.is_known("photoshare")
    assert ids.is_known("billing")
    assert not ids.is_known("unknown")


def test_identities_rejects_duplicate_service_id(tmp_path: Path) -> None:
    p = _write(
        tmp_path,
        "identities.toml",
        """
[[identities]]
service_id = "photoshare"

[[identities]]
service_id = "photoshare"
""",
    )
    with pytest.raises(ValueError, match="duplicate service_id"):
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
def test_identities_rejects_unsafe_service_id(tmp_path: Path, bad_service_id: str) -> None:
    """service_id is composed into filesystem paths by ScopeStore and the
    recording-output path. Unsafe values must be rejected at load time
    so no downstream code has to re-validate."""
    p = _write(
        tmp_path,
        "identities.toml",
        f'[[identities]]\nservice_id = "{bad_service_id}"\n',
    )
    with pytest.raises(ValueError, match=r"(not a safe identifier|must be a string)"):
        load_identities(p)


# --- TrustingVerifier ---


def test_trusting_verifier_accepts_known_service() -> None:
    v = TrustingVerifier(known_service_ids=frozenset({"photoshare"}))
    assert v.verify({"service_id": "photoshare"}) == "photoshare"


def test_trusting_verifier_rejects_unknown_service() -> None:
    v = TrustingVerifier(known_service_ids=frozenset({"photoshare"}))
    with pytest.raises(IdentityRejectedError, match="unknown service_id"):
        v.verify({"service_id": "intruder"})


def test_trusting_verifier_rejects_missing_service_id() -> None:
    v = TrustingVerifier(known_service_ids=frozenset({"photoshare"}))
    with pytest.raises(IdentityRejectedError, match="missing or invalid"):
        v.verify({})


# --- scope store ---


def test_scope_store_loads_by_service_id(tmp_path: Path) -> None:
    scope_dir = tmp_path / "scopes"
    scope_dir.mkdir()
    (scope_dir / "photoshare.toml").write_text(
        'service_id = "photoshare"\nallowed_blocks = ["transactional-store"]\n'
    )
    store = ScopeStore(scope_dir=scope_dir)
    scope = store.get("photoshare")
    assert scope.service_id == "photoshare"


def test_scope_store_missing_file(tmp_path: Path) -> None:
    store = ScopeStore(scope_dir=tmp_path)
    with pytest.raises(ScopeNotFoundError):
        store.get("nobody")


def test_scope_store_rejects_service_id_mismatch(tmp_path: Path) -> None:
    (tmp_path / "photoshare.toml").write_text(
        'service_id = "other"\nallowed_blocks = ["transactional-store"]\n'
    )
    store = ScopeStore(scope_dir=tmp_path)
    with pytest.raises(ValueError, match="expected 'photoshare'"):
        store.get("photoshare")


# --- daemon config ---


def test_daemon_config_defaults_to_enforce(tmp_path: Path) -> None:
    cfg_path = tmp_path / "platformd.toml"
    cfg_path.write_text(
        'listen_address = "127.0.0.1:8443"\n'
        'scope_dir = "scopes"\n'
        'identities_path = "identities.toml"\n'
    )
    config = load_daemon_config(cfg_path)
    assert config.mode_for("anything") == "enforce"
    assert config.listen_host == "127.0.0.1"
    assert config.listen_port == 8443


def test_daemon_config_parses_service_modes(tmp_path: Path) -> None:
    cfg_path = tmp_path / "platformd.toml"
    cfg_path.write_text(
        'listen_address = "127.0.0.1:9000"\n'
        'scope_dir = "s"\n'
        'identities_path = "i.toml"\n'
        '\n[service.photoshare]\nmode = "record"\n'
    )
    config = load_daemon_config(cfg_path)
    assert config.mode_for("photoshare") == "record"
    assert config.mode_for("other") == "enforce"


def test_daemon_config_rejects_invalid_mode(tmp_path: Path) -> None:
    cfg_path = tmp_path / "platformd.toml"
    cfg_path.write_text(
        'listen_address = "127.0.0.1:9000"\n'
        'scope_dir = "s"\n'
        'identities_path = "i.toml"\n'
        '\n[service.photoshare]\nmode = "audit"\n'
    )
    with pytest.raises(ValueError, match="invalid mode"):
        load_daemon_config(cfg_path)


def test_daemon_config_rejects_bad_listen_address(tmp_path: Path) -> None:
    cfg_path = tmp_path / "platformd.toml"
    cfg_path.write_text(
        'listen_address = "not-a-host-port"\n'
        'scope_dir = "s"\n'
        'identities_path = "i.toml"\n'
    )
    with pytest.raises(ValueError, match="listen_address"):
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
    with pytest.raises(TypeError):
        EnforcingSession("demo", FakeEngine())  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        RecordingSession("demo", FakeEngine())  # type: ignore[call-arg]


def test_session_record_mode_accumulates_and_writes_on_drop(tmp_path: Path) -> None:
    out = tmp_path / "demo.recorded.toml"
    engine = FakeEngine()
    session = RecordingSession("demo", engine, out)
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

    text = out.read_text()
    assert "recorded by platformd" in text
    assert "rename to demo.toml" in text


def test_session_record_mode_writes_on_shutdown_without_drop(tmp_path: Path) -> None:
    out = tmp_path / "demo.recorded.toml"
    session = RecordingSession("demo", FakeEngine(), out)
    session.acquire(BlockType.TRANSACTIONAL_STORE, name="db", database="db")
    session.shutdown()

    assert out.is_file()
    parsed = tomllib.loads(out.read_text())
    assert parsed["allowed_blocks"] == ["transactional-store"]


def test_session_record_mode_drop_then_shutdown_writes_once(tmp_path: Path) -> None:
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
    out = tmp_path / "demo.recorded.toml"
    session = RecordingSession("demo", FakeEngine(), out)
    for i in range(RECORD_MAX_BLOCKS):
        session.acquire(BlockType.TRANSACTIONAL_STORE, name=f"db{i}", database="d")
    with pytest.raises(QuotaExceededError, match="record-mode ceiling"):
        session.acquire(BlockType.TRANSACTIONAL_STORE, name="one-too-many")


def test_recorded_file_does_not_auto_load_as_scope(tmp_path: Path) -> None:
    scope_dir = tmp_path / "scopes"
    scope_dir.mkdir()
    (scope_dir / "photoshare.recorded.toml").write_text(
        'service_id = "photoshare"\nallowed_blocks = ["transactional-store"]\n'
    )
    store = ScopeStore(scope_dir=scope_dir)
    with pytest.raises(ScopeNotFoundError):
        store.get("photoshare")


# --- server end-to-end over HTTP, with a fake engine ---


class _HttpSession:
    """Thin TestClient wrapper that owns a single platformd session for
    the duration of the test call. Call create() to obtain the bearer
    token; call acquire/drop/scale_hint to exercise the RPC surface."""

    def __init__(self, client: TestClient, service_id: str = "demo") -> None:
        self._client = client
        self._service_id = service_id
        self.token: str | None = None
        self.session_id: str | None = None

    def create(self, service_id: str | None = None) -> dict[str, Any]:
        sid = service_id or self._service_id
        resp = self._client.post("/sessions", json={"service_id": sid})
        body = resp.json()
        if resp.status_code == 201:
            self.token = body["token"]
            self.session_id = body["session_id"]
        return {"status": resp.status_code, "body": body}

    def acquire(self, **body: Any) -> dict[str, Any]:
        resp = self._client.post("/acquire", json=body, headers=self._headers())
        return self._envelope(resp)

    def drop(self) -> dict[str, Any]:
        resp = self._client.post("/drop-to-scaling-only", headers=self._headers())
        return self._envelope(resp)

    def scale_hint(self, name: str, load_factor: float) -> dict[str, Any]:
        resp = self._client.post(
            "/scale-hint",
            json={"name": name, "load_factor": load_factor},
            headers=self._headers(),
        )
        return self._envelope(resp)

    def close(self) -> None:
        if self.token is None:
            return
        self._client.delete("/sessions", headers=self._headers())
        self.token = None
        self.session_id = None

    def _headers(self) -> dict[str, str]:
        assert self.token is not None, "call create() first"
        return {"Authorization": f"Bearer {self.token}"}

    def _envelope(self, resp: Any) -> dict[str, Any]:
        payload: dict[str, Any] = {"status": resp.status_code}
        if resp.status_code == 204:
            return payload
        body = resp.json()
        if resp.status_code < 300:
            payload["result"] = body
        else:
            detail = body.get("detail", {}) if isinstance(body, dict) else {}
            payload["error"] = detail if isinstance(detail, dict) else {"message": str(detail)}
        return payload


def _build_server(
    tmp_path: Path,
    *,
    service_id: str = "demo",
    scope_files: dict[str, str] | None = None,
    record_for: set[str] | None = None,
) -> tuple[Server, dict[str, FakeEngine]]:
    scope_dir = tmp_path / "scopes"
    scope_dir.mkdir(exist_ok=True)
    for svc, body in (scope_files or {}).items():
        (scope_dir / f"{svc}.toml").write_text(body)

    cfg_lines = [
        'listen_address = "127.0.0.1:0"',
        'scope_dir = "scopes"',
        'identities_path = "identities.toml"',
    ]
    for svc in record_for or ():
        cfg_lines.append("")
        cfg_lines.append(f"[service.{svc}]")
        cfg_lines.append('mode = "record"')
    cfg_path = tmp_path / "platformd.toml"
    cfg_path.write_text("\n".join(cfg_lines) + "\n")
    (tmp_path / "identities.toml").write_text(
        f'[[identities]]\nservice_id = "{service_id}"\n'
    )

    config = load_daemon_config(cfg_path)
    identities = load_identities(config.identities_path)
    store = ScopeStore(scope_dir=config.scope_dir)

    engines: dict[str, FakeEngine] = {}

    def factory(sid: str) -> FakeEngine:
        engines.setdefault(sid, FakeEngine())
        return engines[sid]

    server = Server(
        config=config,
        identities=identities,
        scope_store=store,
        engine_factory=factory,
    )
    return server, engines


@pytest.fixture
def running_server(tmp_path: Path):
    server, engines = _build_server(
        tmp_path,
        scope_files={
            "demo": (
                'service_id = "demo"\n'
                'allowed_blocks = ["transactional-store", "object-store"]\n'
                "max_blocks = 4\n"
            )
        },
    )
    with TestClient(server.app) as client:
        yield client, engines


def test_server_acquire_over_http(running_server) -> None:
    client, engines = running_server
    session = _HttpSession(client, service_id="demo")
    session.create()
    resp = session.acquire(
        block_type="transactional-store",
        name="db",
        params={"database": "demo"},
    )
    assert "error" not in resp, resp
    assert resp["result"]["block_type"] == "transactional-store"
    assert resp["result"]["name"] == "db"
    assert engines["demo"].provisioned[0].name == "db"


def test_server_rejects_unknown_block(running_server) -> None:
    client, _ = running_server
    session = _HttpSession(client, service_id="demo")
    session.create()
    resp = session.acquire(block_type="ephemeral-kv-cache", name="c")
    assert resp["error"]["code"] == "unknown_block"


def test_server_rejects_unknown_service_id(running_server) -> None:
    client, _ = running_server
    session = _HttpSession(client, service_id="intruder")
    resp = session.create()
    assert resp["status"] == 401
    assert resp["body"]["detail"]["code"] == "identity_rejected"


def test_server_rejects_missing_bearer(running_server) -> None:
    client, _ = running_server
    resp = client.post("/acquire", json={"block_type": "transactional-store", "name": "x"})
    assert resp.status_code == 401


def test_server_rejects_unknown_token(running_server) -> None:
    client, _ = running_server
    resp = client.post(
        "/acquire",
        json={"block_type": "transactional-store", "name": "x"},
        headers={"Authorization": "Bearer not-a-real-token"},
    )
    assert resp.status_code == 401


def test_server_privilege_drop_is_per_session(running_server) -> None:
    """Service-process restart → new POST /sessions call → new session
    starts back in ACQUIRING. This is deliberate: a service restart must
    re-enter the ACQUIRING phase so the demo where a new acquire() lands
    after the first run works. Within a single session the daemon enforces
    the drop regardless of what the client claims."""
    client, _ = running_server

    s1 = _HttpSession(client, service_id="demo")
    s1.create()
    s1.acquire(block_type="transactional-store", name="db")
    s1.drop()
    rejected = s1.acquire(block_type="object-store", name="images")
    assert rejected["error"]["code"] == "privilege_dropped"
    s1.close()

    # New session — privilege state resets.
    s2 = _HttpSession(client, service_id="demo")
    s2.create()
    ok = s2.acquire(block_type="object-store", name="images")
    assert "error" not in ok, ok
    assert ok["result"]["name"] == "images"


@pytest.fixture
def recording_server(tmp_path: Path):
    """Daemon in record mode for service 'demo' — NO scope file on disk."""
    server, engines = _build_server(
        tmp_path,
        service_id="demo",
        record_for={"demo"},
    )
    scope_dir = tmp_path / "scopes"
    with TestClient(server.app) as client:
        yield client, scope_dir, engines


def test_server_record_mode_end_to_end(recording_server) -> None:
    client, scope_dir, _engines = recording_server
    session = _HttpSession(client, service_id="demo")
    session.create()
    r1 = session.acquire(block_type="transactional-store", name="db")
    r2 = session.acquire(block_type="ephemeral-kv-cache", name="cache")
    r3 = session.drop()

    assert "error" not in r1, r1
    assert "error" not in r2, r2
    assert "error" not in r3, r3

    recorded = scope_dir / "demo.recorded.toml"
    assert recorded.is_file(), "daemon must write recorded scope on drop"

    parsed = tomllib.loads(recorded.read_text())
    assert parsed["service_id"] == "demo"
    assert set(parsed["allowed_blocks"]) == {"transactional-store", "ephemeral-kv-cache"}
    assert parsed["max_blocks"] == 2

    promoted = scope_dir / "demo.toml"
    promoted.write_text(recorded.read_text())
    scope = load_scope(promoted)
    assert scope.service_id == "demo"
    assert BlockType.TRANSACTIONAL_STORE in scope.allowed_blocks
    assert BlockType.EPHEMERAL_KV_CACHE in scope.allowed_blocks


def test_full_record_then_enforce_flow(tmp_path: Path) -> None:
    """A new service comes up under a recording daemon, acquires whatever
    it needs, drops; the operator reviews the recorded scope, renames it to
    promote, flips the daemon to enforce; the service reconnects and the
    daemon now honours the scope as a hard authorization boundary — same
    acquires succeed, a NEW acquire outside the recorded scope is rejected."""

    # --- Phase 1: record ---
    server, _ = _build_server(
        tmp_path,
        service_id="photoshare",
        record_for={"photoshare"},
    )
    scope_dir = tmp_path / "scopes"
    with TestClient(server.app) as client:
        session = _HttpSession(client, service_id="photoshare")
        session.create()
        r1 = session.acquire(block_type="transactional-store", name="photos")
        assert "error" not in r1, r1
        r2 = session.drop()
        assert "error" not in r2, r2

    recorded = scope_dir / "photoshare.recorded.toml"
    assert recorded.is_file()
    promoted = scope_dir / "photoshare.toml"
    recorded.rename(promoted)

    # --- Phase 2: enforce (new Server instance, same scope dir) ---
    server2, _ = _build_server(
        tmp_path,
        service_id="photoshare",
    )
    with TestClient(server2.app) as client:
        session = _HttpSession(client, service_id="photoshare")
        session.create()
        ok = session.acquire(block_type="transactional-store", name="photos")
        assert "error" not in ok, ok
        rejected = session.acquire(block_type="object-store", name="images")
        assert rejected["error"]["code"] == "unknown_block", rejected


# --- destroy CLI ---


def test_destroy_cli_requires_matching_confirmation(monkeypatch, capsys):
    from platformd.__main__ import main

    monkeypatch.setattr("builtins.input", lambda _prompt: "wrong-name")
    rc = main(["destroy", "--service-id", "photoshare"])
    assert rc == 1
    out = capsys.readouterr().out
    assert "Cancelled" in out


def test_destroy_cli_requires_matching_confirmation_eof(monkeypatch, capsys):
    from platformd.__main__ import main

    def _eof(_prompt):
        raise EOFError()

    monkeypatch.setattr("builtins.input", _eof)
    rc = main(["destroy", "--service-id", "photoshare"])
    assert rc == 1
    assert "Cancelled" in capsys.readouterr().out


def test_destroy_cli_yes_flag_skips_confirmation(monkeypatch):
    from platformd.__main__ import main

    calls: list[str] = []

    class _FakeEngine:
        def __init__(self, service_id: str, **_kw) -> None:
            calls.append(service_id)

        def destroy(self) -> None:
            calls.append("destroyed")

    monkeypatch.setattr("platformd.engine.PulumiDockerEngine", _FakeEngine)
    rc = main(["destroy", "--service-id", "ghost-service", "--yes"])
    assert rc == 0
    assert calls == ["ghost-service", "destroyed"]


def test_destroy_cli_proceeds_on_correct_confirmation(monkeypatch):
    from platformd.__main__ import main

    calls: list[str] = []

    class _FakeEngine:
        def __init__(self, service_id: str, **_kw) -> None:
            calls.append(service_id)

        def destroy(self) -> None:
            calls.append("destroyed")

    monkeypatch.setattr("builtins.input", lambda _prompt: "photoshare")
    monkeypatch.setattr("platformd.engine.PulumiDockerEngine", _FakeEngine)
    rc = main(["destroy", "--service-id", "photoshare"])
    assert rc == 0
    assert calls == ["photoshare", "destroyed"]
