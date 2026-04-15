from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class BlockType(StrEnum):
    TRANSACTIONAL_STORE = "transactional-store"
    OBJECT_STORE = "object-store"
    EPHEMERAL_KV_CACHE = "ephemeral-kv-cache"


class PrivilegeState(StrEnum):
    ACQUIRING = "acquiring"
    OPERATIONAL = "operational"
    SHUTDOWN = "shutdown"


class Persistence(StrEnum):
    EPHEMERAL = "ephemeral"
    PERSISTENT = "persistent"
    TMPFS = "tmpfs"


@dataclass(frozen=True)
class ComputeSpec:
    memory_mb: int

    def __post_init__(self) -> None:
        if self.memory_mb <= 0:
            raise ValueError(f"ComputeSpec.memory_mb must be positive, got {self.memory_mb}")


@dataclass(frozen=True)
class StorageSpec:
    size_mb: int | None = None  # None = let the block use its baseline
    persistence: Persistence = Persistence.EPHEMERAL

    def __post_init__(self) -> None:
        if self.size_mb is not None and self.size_mb <= 0:
            raise ValueError(f"StorageSpec.size_mb must be positive, got {self.size_mb}")


@dataclass(frozen=True)
class Credentials:
    block_type: BlockType
    name: str
    host: str
    port: int
    username: str | None = None
    password: str | None = None
    database: str | None = None
    extras: dict[str, Any] = field(default_factory=dict)

    def as_dsn(self) -> str:
        if self.block_type == BlockType.TRANSACTIONAL_STORE:
            return (
                f"postgresql://{self.username}:{self.password}"
                f"@{self.host}:{self.port}/{self.database}"
            )
        if self.block_type == BlockType.EPHEMERAL_KV_CACHE:
            auth = f":{self.password}@" if self.password else ""
            return f"redis://{auth}{self.host}:{self.port}/0"
        if self.block_type == BlockType.OBJECT_STORE:
            return f"http://{self.host}:{self.port}"
        raise ValueError(f"no DSN form for {self.block_type}")


@dataclass(frozen=True)
class BlockSpec:
    name: str
    block_type: BlockType
    compute: ComputeSpec | None = None
    storage: StorageSpec | None = None
    rps: int | None = None
    params: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.rps is not None and self.compute is not None:
            raise ValueError(
                "BlockSpec: specify either rps or compute, not both "
                "(rps sizes compute via the block's capacity curve)"
            )
        if self.rps is not None and self.rps <= 0:
            raise ValueError(f"BlockSpec.rps must be positive, got {self.rps}")


@dataclass
class ServiceScope:
    service_id: str
    allowed_blocks: set[BlockType]
    max_blocks: int = 16

    def check(self, block_type: BlockType, current_count: int) -> None:
        from platform_api.errors import QuotaExceededError, UnknownBlockError

        if block_type not in self.allowed_blocks:
            raise UnknownBlockError(
                f"service '{self.service_id}' is not permitted to acquire '{block_type.value}'"
            )
        if current_count >= self.max_blocks:
            raise QuotaExceededError(
                f"service '{self.service_id}' has reached max_blocks={self.max_blocks}"
            )
