"""
Building block catalog.

Each building block is an architectural capability (transactional-store,
object-store, ephemeral-kv-cache) that maps to a concrete backend
implementation via a minimal-footprint profile.

The block definitions here are declarative. platformd.engine is
responsible for translating them into Pulumi Docker resources.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from platform_api.types import BlockSpec, BlockType


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
    username: str | None
    password: str | None
    database: str | None
    readiness: "ReadinessCheck"
    extras: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ReadinessCheck:
    """How the engine should verify the service (not just the container) is ready."""

    kind: str
    timeout_s: float = 30.0
    interval_s: float = 0.5


DEFAULT_HOST_PORTS: dict[BlockType, int] = {
    BlockType.TRANSACTIONAL_STORE: 15432,
    BlockType.OBJECT_STORE: 19000,
    BlockType.EPHEMERAL_KV_CACHE: 16379,
}


def _transactional_store(spec: BlockSpec) -> BackendConfig:
    if spec.profile != "minimal":
        raise ValueError(
            f"unsupported profile '{spec.profile}' for transactional-store "
            "(prototype supports 'minimal' only)"
        )
    username = spec.params.get("username", "platform")
    password = spec.params.get("password", "platform-local-password")
    database = spec.params.get("database", "appdb")
    host_port = spec.params.get(
        "host_port", DEFAULT_HOST_PORTS[BlockType.TRANSACTIONAL_STORE]
    )
    return BackendConfig(
        image="postgres:16-alpine",
        container_port=5432,
        host_port=host_port,
        env_vars={
            "POSTGRES_USER": username,
            "POSTGRES_PASSWORD": password,
            "POSTGRES_DB": database,
            # Minimal-footprint tuning: postgres defaults assume a big box.
            # Knock shared buffers and connections down to suit a 64 MB limit.
            "POSTGRES_INITDB_ARGS": "--encoding=UTF8",
        },
        command=[
            "postgres",
            "-c", "shared_buffers=16MB",
            "-c", "max_connections=20",
            "-c", "effective_cache_size=32MB",
            "-c", "work_mem=1MB",
            "-c", "maintenance_work_mem=4MB",
            "-c", "wal_buffers=1MB",
            "-c", "fsync=off",
            "-c", "synchronous_commit=off",
            "-c", "full_page_writes=off",
        ],
        memory_mb=96,
        memory_swap_mb=96,
        username=username,
        password=password,
        database=database,
        readiness=ReadinessCheck(kind="postgres", timeout_s=60.0),
    )


def _object_store(spec: BlockSpec) -> BackendConfig:
    if spec.profile != "minimal":
        raise ValueError(
            f"unsupported profile '{spec.profile}' for object-store "
            "(prototype supports 'minimal' only)"
        )
    username = spec.params.get("username", "platform")
    password = spec.params.get("password", "platform-local-password")
    host_port = spec.params.get(
        "host_port", DEFAULT_HOST_PORTS[BlockType.OBJECT_STORE]
    )
    bucket = spec.params.get("bucket", spec.name)
    return BackendConfig(
        image="minio/minio:latest",
        container_port=9000,
        host_port=host_port,
        env_vars={
            "MINIO_ROOT_USER": username,
            "MINIO_ROOT_PASSWORD": password,
        },
        command=["server", "/data", "--quiet"],
        memory_mb=96,
        memory_swap_mb=96,
        username=username,
        password=password,
        database=None,
        readiness=ReadinessCheck(kind="minio", timeout_s=60.0),
        extras={"bucket": bucket},
    )


def _ephemeral_kv_cache(spec: BlockSpec) -> BackendConfig:
    if spec.profile != "minimal":
        raise ValueError(
            f"unsupported profile '{spec.profile}' for ephemeral-kv-cache "
            "(prototype supports 'minimal' only)"
        )
    password = spec.params.get("password")
    host_port = spec.params.get(
        "host_port", DEFAULT_HOST_PORTS[BlockType.EPHEMERAL_KV_CACHE]
    )
    command = ["redis-server", "--maxmemory", "16mb", "--maxmemory-policy", "allkeys-lru"]
    if password:
        command += ["--requirepass", password]
    return BackendConfig(
        image="redis:7-alpine",
        container_port=6379,
        host_port=host_port,
        env_vars={},
        command=command,
        memory_mb=32,
        memory_swap_mb=32,
        username=None,
        password=password,
        database=None,
        readiness=ReadinessCheck(kind="redis", timeout_s=30.0),
    )


_BACKENDS: dict[BlockType, Callable[[BlockSpec], BackendConfig]] = {
    BlockType.TRANSACTIONAL_STORE: _transactional_store,
    BlockType.OBJECT_STORE: _object_store,
    BlockType.EPHEMERAL_KV_CACHE: _ephemeral_kv_cache,
}


def backend_for(spec: BlockSpec) -> BackendConfig:
    """
    Translate an architectural BlockSpec into a concrete BackendConfig.

    This is the only place in the prototype that knows about product names
    (postgres, minio, redis). Service code only ever references architectural
    block types.
    """
    try:
        return _BACKENDS[spec.block_type](spec)
    except KeyError as e:
        raise ValueError(f"no backend for block type {spec.block_type}") from e
