from __future__ import annotations

import pytest

from platform_api.blocks import DEFAULT_HOST_PORTS, backend_for
from platform_api.types import BlockSpec, BlockType


def _spec(
    block_type: BlockType,
    *,
    name: str = "test",
    profile: str = "minimal",
    **params,
) -> BlockSpec:
    return BlockSpec(
        name=name, block_type=block_type, profile=profile, params=params
    )


def test_transactional_store_minimal_profile():
    config = backend_for(_spec(BlockType.TRANSACTIONAL_STORE))
    assert "postgres" in config.image
    assert config.container_port == 5432
    assert config.host_port == DEFAULT_HOST_PORTS[BlockType.TRANSACTIONAL_STORE]
    assert "POSTGRES_PASSWORD" in config.env_vars
    assert "POSTGRES_DB" in config.env_vars
    assert "shared_buffers=16MB" in " ".join(config.command or [])
    assert config.memory_mb == 96
    assert config.readiness.kind == "postgres"
    assert config.username is not None
    assert config.password is not None
    assert config.database is not None


def test_object_store_minimal_profile():
    config = backend_for(_spec(BlockType.OBJECT_STORE))
    assert "minio" in config.image
    assert config.container_port == 9000
    assert config.host_port == DEFAULT_HOST_PORTS[BlockType.OBJECT_STORE]
    assert "MINIO_ROOT_USER" in config.env_vars
    assert "MINIO_ROOT_PASSWORD" in config.env_vars
    assert config.command == ["server", "/data", "--quiet"]
    assert config.memory_mb == 192
    assert config.readiness.kind == "minio"
    assert config.database is None


def test_ephemeral_kv_cache_minimal_profile():
    config = backend_for(_spec(BlockType.EPHEMERAL_KV_CACHE))
    assert "redis" in config.image
    assert config.container_port == 6379
    assert config.host_port == DEFAULT_HOST_PORTS[BlockType.EPHEMERAL_KV_CACHE]
    assert config.command is not None
    assert "--maxmemory" in config.command
    assert config.memory_mb == 32
    assert config.readiness.kind == "redis"


def test_transactional_store_custom_credentials():
    config = backend_for(
        _spec(
            BlockType.TRANSACTIONAL_STORE,
            username="alice",
            password="secret",
            database="photos",
        )
    )
    assert config.env_vars["POSTGRES_USER"] == "alice"
    assert config.env_vars["POSTGRES_PASSWORD"] == "secret"
    assert config.env_vars["POSTGRES_DB"] == "photos"
    assert config.database == "photos"


def test_unsupported_profile_raises():
    with pytest.raises(ValueError, match="unsupported profile"):
        backend_for(
            _spec(BlockType.TRANSACTIONAL_STORE, profile="production")
        )


def test_minimal_footprint_total_under_target():
    """
    Top-priority design goal: total memory limits for all three blocks at
    minimal profile must stay tight enough that the full system (demo app +
    blocks) fits under the ~200 MB RSS target. Current budget is 320 MB of
    container limits (96+192+32) — measured v1.1 RSS was ~189 MB on macOS
    arm64. The 330 bound here lets an individual block grow by up to ~10 MB
    before this test fails; any larger drift is a design regression.
    """
    total = (
        backend_for(_spec(BlockType.TRANSACTIONAL_STORE)).memory_mb
        + backend_for(_spec(BlockType.OBJECT_STORE)).memory_mb
        + backend_for(_spec(BlockType.EPHEMERAL_KV_CACHE)).memory_mb
    )
    assert total <= 330, f"minimal-profile memory budget blown: {total} MB"
