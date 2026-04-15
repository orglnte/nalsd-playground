from __future__ import annotations

import pytest

from platform_api.types import BlockSpec, BlockType, ComputeSpec, Persistence, StorageSpec
from platformd.blocks import (
    DEFAULT_HOST_PORTS,
    backend_for,
    capacity_for,
    catalog_entry,
    memory_for,
)


def _spec(
    block_type: BlockType,
    *,
    name: str = "test",
    compute: ComputeSpec | None = None,
    storage: StorageSpec | None = None,
    rps: int | None = None,
    **params,
) -> BlockSpec:
    return BlockSpec(
        name=name,
        block_type=block_type,
        compute=compute,
        storage=storage,
        rps=rps,
        params=params,
    )


# -- baseline rendering --


def test_transactional_store_default_baseline():
    config = backend_for(_spec(BlockType.TRANSACTIONAL_STORE))
    assert "postgres" in config.image
    assert config.container_port == 5432
    assert config.host_port == DEFAULT_HOST_PORTS[BlockType.TRANSACTIONAL_STORE]
    assert "POSTGRES_PASSWORD" in config.env_vars
    assert "POSTGRES_DB" in config.env_vars
    assert "shared_buffers=16MB" in " ".join(config.command or [])
    assert "max_connections=20" in " ".join(config.command or [])
    assert config.memory_mb == 96
    assert config.storage_mb == 4096
    assert config.persistence is Persistence.EPHEMERAL
    assert config.readiness.kind == "postgres"
    assert config.username is not None
    assert config.password is not None
    assert config.database is not None


def test_object_store_default_baseline():
    config = backend_for(_spec(BlockType.OBJECT_STORE))
    assert "rustfs" in config.image
    assert config.container_port == 9000
    assert config.host_port == DEFAULT_HOST_PORTS[BlockType.OBJECT_STORE]
    assert config.memory_mb == 256
    assert config.storage_mb == 4096
    assert config.readiness.kind == "rustfs"
    assert config.database is None


def test_ephemeral_kv_cache_default_baseline():
    config = backend_for(_spec(BlockType.EPHEMERAL_KV_CACHE))
    assert "redis" in config.image
    assert config.container_port == 6379
    assert config.host_port == DEFAULT_HOST_PORTS[BlockType.EPHEMERAL_KV_CACHE]
    assert config.command is not None
    assert "--maxmemory" in config.command
    assert config.memory_mb == 32
    assert config.storage_mb == 32  # caches: storage == memory
    assert config.readiness.kind == "redis"


def test_transactional_store_custom_credentials():
    config = backend_for(
        _spec(
            BlockType.TRANSACTIONAL_STORE,
            username="alice",
            password="secret",
            database="metadata",
        )
    )
    assert config.env_vars["POSTGRES_USER"] == "alice"
    assert config.env_vars["POSTGRES_PASSWORD"] == "secret"
    assert config.env_vars["POSTGRES_DB"] == "metadata"
    assert config.database == "metadata"


# -- scaling --


def test_postgres_scales_linearly_with_memory():
    config = backend_for(
        _spec(BlockType.TRANSACTIONAL_STORE, compute=ComputeSpec(memory_mb=192))
    )
    cmd = " ".join(config.command or [])
    assert config.memory_mb == 192
    assert "shared_buffers=32MB" in cmd  # 16 * 2
    assert "max_connections=40" in cmd   # 20 * 2
    assert "effective_cache_size=64MB" in cmd  # 32 * 2
    assert config.capacity_hints["max_connections"] == 40
    assert config.capacity_hints["rps"] == 200  # 100 * 2


def test_redis_scales_memory_and_maxmemory():
    config = backend_for(
        _spec(
            BlockType.EPHEMERAL_KV_CACHE,
            compute=ComputeSpec(memory_mb=64),
            storage=StorageSpec(size_mb=64),
        )
    )
    assert config.memory_mb == 64
    assert config.storage_mb == 64
    assert "32mb" in (config.command or [])  # maxmemory = mem / 2


def test_rps_inverse_picks_smallest_sufficient_tier():
    # postgres baseline rps=100; asking for 150 rps should pick scale=2 (200 rps).
    assert memory_for(BlockType.TRANSACTIONAL_STORE, rps=150) == 192
    assert memory_for(BlockType.TRANSACTIONAL_STORE, rps=100) == 96
    assert memory_for(BlockType.TRANSACTIONAL_STORE, rps=500) == 768  # scale=8 -> 800 rps


def test_rps_shortcut_at_render_sizes_compute_only():
    config = backend_for(_spec(BlockType.TRANSACTIONAL_STORE, rps=300))
    # 300 rps → scale=4 → memory=384, storage stays at baseline.
    assert config.memory_mb == 384
    assert config.storage_mb == 4096


# -- validation --


def test_non_base2_memory_is_rejected():
    with pytest.raises(ValueError, match="base-2"):
        backend_for(
            _spec(BlockType.TRANSACTIONAL_STORE, compute=ComputeSpec(memory_mb=100))
        )


def test_non_base2_memory_below_baseline_is_rejected():
    with pytest.raises(ValueError, match="base-2"):
        backend_for(
            _spec(BlockType.TRANSACTIONAL_STORE, compute=ComputeSpec(memory_mb=48))
        )


def test_redis_rejects_storage_not_equal_to_memory():
    with pytest.raises(ValueError, match="must be equal"):
        backend_for(
            _spec(
                BlockType.EPHEMERAL_KV_CACHE,
                compute=ComputeSpec(memory_mb=64),
                storage=StorageSpec(size_mb=32),
            )
        )


def test_spec_rejects_rps_and_compute_together():
    with pytest.raises(ValueError, match="either rps or compute"):
        BlockSpec(
            name="x",
            block_type=BlockType.TRANSACTIONAL_STORE,
            compute=ComputeSpec(memory_mb=192),
            rps=200,
        )


def test_compute_spec_rejects_non_positive_memory():
    with pytest.raises(ValueError, match="positive"):
        ComputeSpec(memory_mb=0)
    with pytest.raises(ValueError, match="positive"):
        ComputeSpec(memory_mb=-16)


def test_storage_spec_rejects_non_positive_size():
    with pytest.raises(ValueError, match="positive"):
        StorageSpec(size_mb=0)


def test_redis_rps_shortcut_scales_storage_with_memory():
    """Regression: rps above baseline on a cache must not force the caller
    to pre-compute matching storage_mb. Cache storage follows memory."""
    config = backend_for(_spec(BlockType.EPHEMERAL_KV_CACHE, rps=150))
    assert config.memory_mb == 64
    assert config.storage_mb == 64


def test_redis_compute_shortcut_scales_storage_with_memory():
    config = backend_for(
        _spec(BlockType.EPHEMERAL_KV_CACHE, compute=ComputeSpec(memory_mb=128))
    )
    assert config.memory_mb == 128
    assert config.storage_mb == 128


def test_persistent_storage_rejected_in_prototype():
    with pytest.raises(NotImplementedError, match="persistent"):
        backend_for(
            _spec(
                BlockType.TRANSACTIONAL_STORE,
                storage=StorageSpec(size_mb=4096, persistence=Persistence.PERSISTENT),
            )
        )


def test_tmpfs_storage_rejected_in_prototype():
    with pytest.raises(NotImplementedError, match="tmpfs"):
        backend_for(
            _spec(
                BlockType.TRANSACTIONAL_STORE,
                storage=StorageSpec(size_mb=4096, persistence=Persistence.TMPFS),
            )
        )


def test_client_rejects_persistent_and_ram_backed_together():
    """Client-side ergonomics guard — `persistent=True, ram_backed=True` is
    mutually exclusive at the kwargs layer."""
    from platform_api.client import Client

    # Use a non-connected client purely to exercise the kwarg guard; the
    # ValueError fires before any socket I/O.
    c = Client("svc", socket_path="/tmp/does-not-exist.sock")
    with pytest.raises(ValueError, match="mutually exclusive"):
        c.acquire(
            BlockType.TRANSACTIONAL_STORE,
            name="db",
            persistent=True,
            ram_backed=True,
        )


# -- catalog --


def test_catalog_exposes_baselines():
    pg = catalog_entry(BlockType.TRANSACTIONAL_STORE)
    assert pg.baseline_memory_mb == 96
    assert pg.baseline_storage_mb == 4096
    assert pg.baseline_rps >= 100


def test_capacity_for_uses_baseline_when_unspecified():
    cap = capacity_for(BlockType.TRANSACTIONAL_STORE)
    assert cap.memory_mb == 96
    assert cap.storage_mb == 4096
    assert cap.scale == 1
    assert cap.rps == 100


PER_BLOCK_MINIMAL_CEILING_MB = 256


def test_each_block_default_baseline_under_per_block_ceiling():
    """
    Top-priority design goal: every block at its default baseline must
    sustain ~100 rps/qps on the least memory and CPU that can achieve
    that. The concrete guardrail is that NO block may declare a baseline
    memory above PER_BLOCK_MINIMAL_CEILING_MB. Going higher is reserved
    for explicit compute=... at acquire time or rps=... shortcut;
    anything that lifts a block's baseline above this ceiling is a
    design regression, not a tuning choice.
    """
    for block_type in BlockType:
        config = backend_for(_spec(block_type))
        assert config.memory_mb <= PER_BLOCK_MINIMAL_CEILING_MB, (
            f"{block_type.value} baseline exceeds per-block ceiling: "
            f"{config.memory_mb} MB > {PER_BLOCK_MINIMAL_CEILING_MB} MB"
        )
