"""Unit tests for PulumiDockerEngine internals that don't need Pulumi.

The engine constructor calls _setup_pulumi_env() which only mutates env
vars and creates a state dir — safe in tests. Pulumi itself is not
invoked unless we call provision() or destroy().
"""

from __future__ import annotations

from platform_api.types import BlockSpec, BlockType, ComputeSpec
from platformd.blocks import backend_for
from platformd.engine import PulumiDockerEngine


def _engine() -> PulumiDockerEngine:
    return PulumiDockerEngine(service_id="test-svc")


def test_build_credentials_surfaces_postgres_capacity_hints() -> None:
    eng = _engine()
    spec = BlockSpec(name="db", block_type=BlockType.TRANSACTIONAL_STORE)
    creds = eng._build_credentials(spec, backend_for(spec))
    assert creds.extras["max_connections"] == 20
    assert creds.extras["shared_buffers_mb"] == 16
    assert creds.extras["rps"] == 100
    assert creds.extras["memory_mb"] == 96
    assert creds.extras["storage_mb"] == 4096


def test_build_credentials_scales_postgres_hints_with_memory() -> None:
    eng = _engine()
    spec = BlockSpec(
        name="db",
        block_type=BlockType.TRANSACTIONAL_STORE,
        compute=ComputeSpec(memory_mb=384),
    )
    creds = eng._build_credentials(spec, backend_for(spec))
    # scale = 384 / 96 = 4
    assert creds.extras["max_connections"] == 80
    assert creds.extras["shared_buffers_mb"] == 64
    assert creds.extras["rps"] == 400
    assert creds.extras["memory_mb"] == 384


def test_build_credentials_redis_hints() -> None:
    eng = _engine()
    spec = BlockSpec(name="cache", block_type=BlockType.EPHEMERAL_KV_CACHE)
    creds = eng._build_credentials(spec, backend_for(spec))
    assert creds.extras["maxmemory_mb"] == 16  # half of 32
    assert creds.extras["rps"] == 100
    assert creds.extras["memory_mb"] == 32
    assert creds.extras["storage_mb"] == 32


def test_build_credentials_object_store_keeps_bucket_and_adds_capacity() -> None:
    eng = _engine()
    spec = BlockSpec(name="photos", block_type=BlockType.OBJECT_STORE)
    creds = eng._build_credentials(spec, backend_for(spec))
    assert creds.extras["bucket"] == "photos"
    assert creds.extras["rps"] == 100
    assert creds.extras["memory_mb"] == 256
    assert creds.extras["storage_mb"] == 4096
