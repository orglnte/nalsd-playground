from __future__ import annotations

import pytest

from platformd.blocks import DEFAULT_HOST_PORTS, backend_for
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
    assert config.memory_mb == 96
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


PER_BLOCK_MINIMAL_CEILING_MB = 100


def test_each_block_minimal_profile_under_per_block_ceiling():
    """
    Top-priority design goal: every block on its `minimal` profile must
    sustain ~100 rps/qps on the least memory and CPU that can achieve
    that. The concrete guardrail is that NO minimal block may declare a
    `memory_mb` above PER_BLOCK_MINIMAL_CEILING_MB. Going higher is
    reserved for explicit profile=... at acquire time or scale_hint()
    post-drop; anything that lifts a block's minimal limit above 100 MB
    is a design regression, not a tuning choice.
    """
    for block_type in BlockType:
        config = backend_for(_spec(block_type))
        assert config.memory_mb <= PER_BLOCK_MINIMAL_CEILING_MB, (
            f"{block_type.value} minimal profile exceeds per-block ceiling: "
            f"{config.memory_mb} MB > {PER_BLOCK_MINIMAL_CEILING_MB} MB"
        )
