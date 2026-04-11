"""
End-to-end: real PulumiDockerEngine, real Docker container, real drop.

Proves the full stack enforces the privilege drop — not just the unit-level
state machine with a FakeEngine. Acquires a postgres block, drops privileges,
and asserts the second acquire is rejected even though the engine is fully
operational and could provision the next block.

Skips cleanly if Docker isn't available. Uses a dedicated service_id so it
doesn't collide with photoshare state, but it does bind
`DEFAULT_HOST_PORTS[TRANSACTIONAL_STORE]` — don't run alongside photoshare.
"""

from __future__ import annotations

import shutil
import subprocess

import pytest

from platform_api import (
    BlockType,
    PlatformClient,
    PrivilegeDroppedError,
    ServiceScope,
)
from platform_api.engine import PulumiDockerEngine


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    result = subprocess.run(
        ["docker", "info"], capture_output=True, text=True
    )
    return result.returncode == 0


def test_privilege_drop_blocks_acquire_end_to_end():
    if not _docker_available():
        pytest.skip("docker daemon not reachable")

    scope = ServiceScope(
        service_id="e2e-drop-test",
        allowed_blocks={
            BlockType.TRANSACTIONAL_STORE,
            BlockType.OBJECT_STORE,
        },
        max_blocks=4,
    )
    engine = PulumiDockerEngine(service_id="e2e-drop-test")
    client = PlatformClient(
        service_id="e2e-drop-test", scope=scope, engine=engine
    )
    try:
        db = client.acquire(
            BlockType.TRANSACTIONAL_STORE, name="db", database="e2e"
        )
        assert db.host == "127.0.0.1"
        assert db.database == "e2e"

        client.drop_to_scaling_only()

        with pytest.raises(PrivilegeDroppedError):
            client.acquire(BlockType.OBJECT_STORE, name="store")
    finally:
        client.shutdown(destroy=True)
