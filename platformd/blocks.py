"""
Building block catalog.

Each building block is an architectural capability (transactional-store,
object-store, ephemeral-kv-cache) that maps to a concrete backend
implementation. Every block declares a capacity baseline (memory, storage,
rps that the baseline sustains). Callers size blocks either by passing an
explicit ComputeSpec/StorageSpec or by passing `rps=` which the daemon
inverts against the block's capacity curve to pick a memory tier.

Scaling rule: legal memory is baseline * 2^n for n in {0, 1, 2, ...}. All
proportional internal knobs (postgres shared_buffers/max_connections/etc,
redis maxmemory) scale linearly with `scale = memory_mb / baseline`.
Storage scales independently by the same base-2 rule; redis requires
storage_mb == memory_mb (cache capacity is one dimension, not two).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from platform_api.types import (
    BlockSpec,
    BlockType,
    ComputeSpec,
    Persistence,
    StorageSpec,
)


@dataclass(frozen=True)
class ReadinessCheck:
    """How the engine should verify the service (not just the container) is ready."""

    kind: str
    timeout_s: float = 30.0
    interval_s: float = 0.5


@dataclass(frozen=True)
class BackendConfig:
    """Concrete backend parameters for a block instance."""

    image: str
    container_port: int
    host_port: int
    env_vars: dict[str, str]
    command: list[str] | None
    memory_mb: int
    memory_swap_mb: int
    storage_mb: int
    persistence: Persistence
    username: str | None
    password: str | None
    database: str | None
    readiness: ReadinessCheck
    tmpfs: dict[str, str] = field(default_factory=dict)
    extras: dict[str, str] = field(default_factory=dict)
    capacity_hints: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class Capacity:
    """Resolved sizing after baseline defaults + scale factor.

    `scale` is the base-2 multiplier (1, 2, 4, 8...) applied to the block's
    baseline. All proportional knobs in the renderer multiply their
    baseline by `scale`. `rps` is baseline_rps * scale — an advertised
    floor, not a hard cap.
    """

    memory_mb: int
    storage_mb: int
    scale: int
    rps: int


@dataclass(frozen=True)
class BlockCatalogEntry:
    block_type: BlockType
    baseline_memory_mb: int
    baseline_storage_mb: int
    baseline_rps: int
    renderer: Callable[[BlockSpec, Capacity], BackendConfig]


DEFAULT_HOST_PORTS: dict[BlockType, int] = {
    BlockType.TRANSACTIONAL_STORE: 15432,
    BlockType.OBJECT_STORE: 19000,
    BlockType.EPHEMERAL_KV_CACHE: 16379,
}


# Baseline storage volume for blocks whose storage is separate from memory.
# Redis (and other in-memory caches) override this: storage == memory.
_DEFAULT_STORAGE_BASELINE_MB = 4096


def _is_base2_multiple(value: int, base: int) -> bool:
    if value < base or value % base != 0:
        return False
    quotient = value // base
    return quotient > 0 and (quotient & (quotient - 1)) == 0


def _resolve_capacity(spec: BlockSpec, entry: BlockCatalogEntry) -> Capacity:
    """Fill in baseline defaults; validate base-2 scaling."""
    if spec.rps is not None:
        memory_mb = _memory_for_rps(entry, spec.rps)
    elif spec.compute is not None:
        memory_mb = spec.compute.memory_mb
    else:
        memory_mb = entry.baseline_memory_mb

    if not _is_base2_multiple(memory_mb, entry.baseline_memory_mb):
        raise ValueError(
            f"{entry.block_type.value}: memory_mb={memory_mb} is not a base-2 "
            f"multiple of baseline {entry.baseline_memory_mb}"
        )
    scale = memory_mb // entry.baseline_memory_mb

    if spec.storage is not None and spec.storage.size_mb is not None:
        storage_mb = spec.storage.size_mb
    elif entry.block_type is BlockType.EPHEMERAL_KV_CACHE:
        # Cache capacity is one dimension — storage follows memory at every
        # tier so `rps=` and `compute=` shortcuts "just work" without
        # forcing the caller to restate storage.
        storage_mb = memory_mb
    else:
        storage_mb = entry.baseline_storage_mb

    if entry.block_type is BlockType.EPHEMERAL_KV_CACHE:
        if storage_mb != memory_mb:
            raise ValueError(
                f"{entry.block_type.value}: memory_mb ({memory_mb}) and "
                f"storage_mb ({storage_mb}) must be equal for in-memory caches"
            )
    else:
        if not _is_base2_multiple(storage_mb, entry.baseline_storage_mb):
            raise ValueError(
                f"{entry.block_type.value}: storage_mb={storage_mb} is not a "
                f"base-2 multiple of baseline {entry.baseline_storage_mb}"
            )

    requested_persistence = (
        spec.storage.persistence if spec.storage is not None else Persistence.EPHEMERAL
    )
    if requested_persistence is not Persistence.EPHEMERAL:
        raise NotImplementedError(
            f"persistence={requested_persistence.value} not yet supported "
            "by the renderer; ephemeral containers only in this prototype"
        )

    return Capacity(
        memory_mb=memory_mb,
        storage_mb=storage_mb,
        scale=scale,
        rps=entry.baseline_rps * scale,
    )


def _memory_for_rps(entry: BlockCatalogEntry, target_rps: int) -> int:
    """Smallest base-2 memory tier whose advertised rps >= target_rps."""
    if target_rps <= entry.baseline_rps:
        return entry.baseline_memory_mb
    scale = 1
    while entry.baseline_rps * scale < target_rps:
        scale *= 2
    return entry.baseline_memory_mb * scale


def _render_transactional_store(spec: BlockSpec, capacity: Capacity) -> BackendConfig:
    username = spec.params.get("username", "platform")
    password = spec.params.get("password", "platform-local-password")
    database = spec.params.get("database", "appdb")
    host_port = spec.params.get("host_port", DEFAULT_HOST_PORTS[BlockType.TRANSACTIONAL_STORE])

    k = capacity.scale
    shared_buffers_mb = 16 * k
    max_connections = 20 * k
    effective_cache_mb = 32 * k
    work_mem_mb = 1 * k
    maintenance_work_mem_mb = 4 * k
    wal_buffers_mb = 1 * k

    storage_spec = spec.storage or StorageSpec(size_mb=capacity.storage_mb)
    persistence = storage_spec.persistence

    return BackendConfig(
        image="postgres:16-alpine",
        container_port=5432,
        host_port=host_port,
        env_vars={
            "POSTGRES_USER": username,
            "POSTGRES_PASSWORD": password,
            "POSTGRES_DB": database,
            "POSTGRES_INITDB_ARGS": "--encoding=UTF8",
        },
        command=[
            "postgres",
            "-c",
            f"shared_buffers={shared_buffers_mb}MB",
            "-c",
            f"max_connections={max_connections}",
            "-c",
            f"effective_cache_size={effective_cache_mb}MB",
            "-c",
            f"work_mem={work_mem_mb}MB",
            "-c",
            f"maintenance_work_mem={maintenance_work_mem_mb}MB",
            "-c",
            f"wal_buffers={wal_buffers_mb}MB",
            "-c",
            "fsync=off",
            "-c",
            "synchronous_commit=off",
            "-c",
            "full_page_writes=off",
        ],
        memory_mb=capacity.memory_mb,
        memory_swap_mb=capacity.memory_mb,
        storage_mb=capacity.storage_mb,
        persistence=persistence,
        username=username,
        password=password,
        database=database,
        readiness=ReadinessCheck(kind="postgres", timeout_s=60.0),
        capacity_hints={
            "max_connections": max_connections,
            "shared_buffers_mb": shared_buffers_mb,
            "rps": capacity.rps,
        },
    )


def _render_object_store(spec: BlockSpec, capacity: Capacity) -> BackendConfig:
    host_port = spec.params.get("host_port", DEFAULT_HOST_PORTS[BlockType.OBJECT_STORE])
    bucket = spec.params.get("bucket", spec.name)
    username = spec.params.get("username", "platform")
    password = spec.params.get("password", "platform-local-password")

    storage_spec = spec.storage or StorageSpec(size_mb=capacity.storage_mb)
    persistence = storage_spec.persistence

    return BackendConfig(
        image="rustfs/rustfs:latest",
        container_port=9000,
        host_port=host_port,
        env_vars={
            "RUSTFS_ROOT_USER": username,
            "RUSTFS_ROOT_PASSWORD": password,
            "RUSTFS_SCANNER_ENABLED": "false",
            "RUSTFS_HEAL_ENABLED": "false",
        },
        command=["server", "/data"],
        memory_mb=capacity.memory_mb,
        memory_swap_mb=capacity.memory_mb,
        storage_mb=capacity.storage_mb,
        persistence=persistence,
        username=username,
        password=password,
        database=None,
        readiness=ReadinessCheck(kind="rustfs", timeout_s=60.0),
        extras={"bucket": bucket},
        capacity_hints={"rps": capacity.rps},
    )


def _render_ephemeral_kv_cache(spec: BlockSpec, capacity: Capacity) -> BackendConfig:
    password = spec.params.get("password")
    host_port = spec.params.get("host_port", DEFAULT_HOST_PORTS[BlockType.EPHEMERAL_KV_CACHE])
    # Redis needs headroom above maxmemory for its own working set; by
    # convention we set maxmemory to half of container memory.
    maxmemory_mb = max(1, capacity.memory_mb // 2)
    command = ["redis-server", "--maxmemory", f"{maxmemory_mb}mb", "--maxmemory-policy", "allkeys-lru"]
    if password:
        command += ["--requirepass", password]

    storage_spec = spec.storage or StorageSpec(size_mb=capacity.storage_mb)
    persistence = storage_spec.persistence

    return BackendConfig(
        image="redis:7-alpine",
        container_port=6379,
        host_port=host_port,
        env_vars={},
        command=command,
        memory_mb=capacity.memory_mb,
        memory_swap_mb=capacity.memory_mb,
        storage_mb=capacity.storage_mb,
        persistence=persistence,
        username=None,
        password=password,
        database=None,
        readiness=ReadinessCheck(kind="redis", timeout_s=30.0),
        capacity_hints={"maxmemory_mb": maxmemory_mb, "rps": capacity.rps},
    )


_CATALOG: dict[BlockType, BlockCatalogEntry] = {
    BlockType.TRANSACTIONAL_STORE: BlockCatalogEntry(
        block_type=BlockType.TRANSACTIONAL_STORE,
        baseline_memory_mb=96,
        baseline_storage_mb=_DEFAULT_STORAGE_BASELINE_MB,
        baseline_rps=100,
        renderer=_render_transactional_store,
    ),
    BlockType.OBJECT_STORE: BlockCatalogEntry(
        block_type=BlockType.OBJECT_STORE,
        baseline_memory_mb=256,
        baseline_storage_mb=_DEFAULT_STORAGE_BASELINE_MB,
        baseline_rps=100,
        renderer=_render_object_store,
    ),
    BlockType.EPHEMERAL_KV_CACHE: BlockCatalogEntry(
        block_type=BlockType.EPHEMERAL_KV_CACHE,
        baseline_memory_mb=32,
        baseline_storage_mb=32,  # caches: storage == memory
        baseline_rps=100,
        renderer=_render_ephemeral_kv_cache,
    ),
}


def catalog_entry(block_type: BlockType) -> BlockCatalogEntry:
    try:
        return _CATALOG[block_type]
    except KeyError as e:
        raise ValueError(f"no catalog entry for block type {block_type}") from e


def memory_for(block_type: BlockType, rps: int) -> int:
    """Smallest base-2 memory tier whose advertised rps >= target rps."""
    return _memory_for_rps(catalog_entry(block_type), rps)


def capacity_for(
    block_type: BlockType,
    *,
    memory_mb: int | None = None,
    storage_mb: int | None = None,
) -> Capacity:
    """Resolve a Capacity for a block given explicit sizing inputs.

    None values fall back to the block's baseline. Validates base-2 scaling
    and (for caches) the memory/storage equality rule.
    """
    entry = catalog_entry(block_type)
    spec = BlockSpec(
        name="__capacity_for__",
        block_type=block_type,
        compute=ComputeSpec(memory_mb=memory_mb) if memory_mb is not None else None,
        storage=StorageSpec(size_mb=storage_mb) if storage_mb is not None else None,
    )
    return _resolve_capacity(spec, entry)


def backend_for(spec: BlockSpec) -> BackendConfig:
    """
    Translate an architectural BlockSpec into a concrete BackendConfig.

    This is the only place in the prototype that knows about product names
    (postgres, rustfs, redis). Service code only ever references
    architectural block types.
    """
    entry = catalog_entry(spec.block_type)
    capacity = _resolve_capacity(spec, entry)
    return entry.renderer(spec, capacity)
