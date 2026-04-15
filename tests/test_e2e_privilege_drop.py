"""
End-to-end: real platformd + real PulumiDockerEngine + real Docker
container + real platform_api.Client HTTP round-trip.

Spins up a platformd.Server (FastAPI + uvicorn) in a background thread
with a PulumiDockerEngine as its engine factory, connects a
platform_api.Client over HTTP, acquires a transactional-store block
(which actually provisions a Postgres container via Pulumi), drops
privileges, and asserts the daemon rejects a second acquire even though
the engine is fully operational.

Skips cleanly if Docker isn't reachable. Uses a dedicated service_id
(`e2edrop`) so it doesn't collide with photoshare state. Binds
DEFAULT_HOST_PORTS[TRANSACTIONAL_STORE] — don't run alongside
photoshare.
"""

from __future__ import annotations

import contextlib
import shutil
import socket
import subprocess
import threading
import time
from pathlib import Path

import pytest

from platform_api import BlockType, Client, PrivilegeDroppedError
from platformd.config import load_daemon_config
from platformd.engine import PulumiDockerEngine
from platformd.identities import load_identities
from platformd.scope_store import ScopeStore
from platformd.server import Server

SERVICE_ID = "e2edrop"


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    result = subprocess.run(["docker", "info"], capture_output=True, text=True)
    return result.returncode == 0


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_http(host: str, port: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.2)
            try:
                s.connect((host, port))
                return
            except OSError:
                time.sleep(0.05)
    raise TimeoutError(f"daemon did not start listening on {host}:{port}")


@pytest.fixture
def real_daemon(tmp_path: Path):
    """Run a Server in a thread with a real PulumiDockerEngine factory.

    Yields (base_url, engines_by_service_id). Teardown stops the server
    and destroys every engine's stack so Pulumi-managed containers do not
    leak between test runs.
    """
    port = _pick_free_port()
    host = "127.0.0.1"

    scope_dir = tmp_path / "scopes"
    scope_dir.mkdir()
    (scope_dir / f"{SERVICE_ID}.toml").write_text(
        f'service_id = "{SERVICE_ID}"\n'
        'allowed_blocks = ["transactional-store", "object-store"]\n'
        "max_blocks = 4\n",
        encoding="utf-8",
    )

    cfg_path = tmp_path / "platformd.toml"
    cfg_path.write_text(
        f'listen_address = "{host}:{port}"\n'
        'scope_dir = "scopes"\n'
        'identities_path = "identities.toml"\n',
        encoding="utf-8",
    )
    (tmp_path / "identities.toml").write_text(
        f'[[identities]]\nservice_id = "{SERVICE_ID}"\n',
        encoding="utf-8",
    )

    config = load_daemon_config(cfg_path)
    identities = load_identities(config.identities_path)
    store = ScopeStore(scope_dir=config.scope_dir)

    engines: dict[str, PulumiDockerEngine] = {}

    def factory(service_id: str) -> PulumiDockerEngine:
        engines.setdefault(service_id, PulumiDockerEngine(service_id=service_id))
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
        _wait_for_http(host, port)
        yield f"http://{host}:{port}", engines
    finally:
        server.stop()
        thread.join(timeout=5)
        for eng in engines.values():
            with contextlib.suppress(Exception):
                eng.destroy()


def test_privilege_drop_blocks_acquire_end_to_end(real_daemon) -> None:
    if not _docker_available():
        pytest.skip("docker daemon not reachable")

    base_url, _ = real_daemon
    client = Client(SERVICE_ID, base_url=base_url)
    client.connect()
    try:
        db = client.acquire(BlockType.TRANSACTIONAL_STORE, name="db", database="e2e")
        assert db.host == "127.0.0.1"
        assert db.database == "e2e"
        assert db.block_type is BlockType.TRANSACTIONAL_STORE

        client.drop_to_scaling_only()

        with pytest.raises(PrivilegeDroppedError):
            client.acquire(BlockType.OBJECT_STORE, name="store")
    finally:
        client.close()
