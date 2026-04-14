"""
End-to-end: real platformd + real PulumiDockerEngine + real Docker
container + real platform_api.Client UDS round-trip.

This is the flagship test of the daemon-split architecture. It spins up
a platformd.Server in a background thread with a PulumiDockerEngine as
its engine factory, connects a platform_api.Client over a Unix domain
socket, acquires a transactional-store block (which actually provisions
a Postgres container via Pulumi), drops privileges, and asserts the
daemon rejects a second acquire even though the engine is fully
operational. The previous version of this test drove the in-process
engine directly — so it proved nothing about the UDS trust boundary
the split was supposed to enforce.

Skips cleanly if Docker isn't reachable. Uses a dedicated service_id
(`e2edrop`) so it doesn't collide with photoshare state. Binds
DEFAULT_HOST_PORTS[TRANSACTIONAL_STORE] — don't run alongside
photoshare.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import subprocess
import threading
import uuid
from pathlib import Path

import pytest

from platform_api import BlockType, Client, PrivilegeDroppedError
from platformd.config import load_daemon_config
from platformd.engine import PulumiDockerEngine
from platformd.identities import Identities
from platformd.scope_store import ScopeStore
from platformd.server import Server

SERVICE_ID = "e2edrop"


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    result = subprocess.run(["docker", "info"], capture_output=True, text=True)
    return result.returncode == 0


@pytest.fixture
def real_daemon(tmp_path: Path):
    """Run a Server in a thread with a real PulumiDockerEngine factory.

    Yields (socket_path, engines_by_service_id). Teardown stops the
    server and destroys every engine's stack so Pulumi-managed
    containers do not leak between test runs.
    """
    short_sock = Path(f"/tmp/nalsd-e2e-{uuid.uuid4().hex[:8]}.sock")

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
        f'socket_path = "{short_sock}"\n'
        'scope_dir = "scopes"\n'
        'identities_path = "identities.toml"\n',
        encoding="utf-8",
    )
    (tmp_path / "identities.toml").write_text(
        f'[[identities]]\nuid = {os.getuid()}\nservice_id = "{SERVICE_ID}"\n',
        encoding="utf-8",
    )

    config = load_daemon_config(cfg_path)
    identities = Identities(by_uid={os.getuid(): SERVICE_ID})
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
        yield config.socket_path, engines
    finally:
        server.stop()
        thread.join(timeout=5)
        # Tear down any Pulumi stack the test provisioned so the
        # containers do not survive the test run.
        for eng in engines.values():
            with contextlib.suppress(Exception):
                eng.destroy()


def test_privilege_drop_blocks_acquire_end_to_end(real_daemon) -> None:
    if not _docker_available():
        pytest.skip("docker daemon not reachable")

    sock_path, _ = real_daemon
    client = Client(SERVICE_ID, socket_path=sock_path)
    client.connect()
    try:
        # Real provision: Pulumi actually pulls postgres:16-alpine (if not
        # cached), starts a container, and the engine's readiness poll
        # waits for SELECT 1 to succeed. The daemon returns the
        # credentials only once the wire protocol responds.
        db = client.acquire(BlockType.TRANSACTIONAL_STORE, name="db", database="e2e")
        assert db.host == "127.0.0.1"
        assert db.database == "e2e"
        assert db.block_type is BlockType.TRANSACTIONAL_STORE

        # Drop privileges. The daemon's Session transitions to
        # OPERATIONAL and will refuse any further Acquire on this
        # connection.
        client.drop_to_scaling_only()

        # Second acquire: the engine could provision this — it has a
        # live stack and a healthy Pulumi connection — but the daemon
        # must refuse because the session already dropped. The trust
        # boundary is what rejects this, not any client-side state.
        with pytest.raises(PrivilegeDroppedError):
            client.acquire(BlockType.OBJECT_STORE, name="store")
    finally:
        client.close()
