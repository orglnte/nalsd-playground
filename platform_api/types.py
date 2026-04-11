from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class BlockType(str, Enum):
    TRANSACTIONAL_STORE = "transactional-store"
    OBJECT_STORE = "object-store"
    EPHEMERAL_KV_CACHE = "ephemeral-kv-cache"


class PrivilegeState(str, Enum):
    ACQUIRING = "acquiring"
    OPERATIONAL = "operational"
    SHUTDOWN = "shutdown"


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
    profile: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class CapabilityManifest:
    service_id: str
    allowed_blocks: set[BlockType]
    max_blocks: int = 16

    def check(self, block_type: BlockType, current_count: int) -> None:
        from platform_api.errors import QuotaExceededError, UnknownBlockError

        if block_type not in self.allowed_blocks:
            raise UnknownBlockError(
                f"service '{self.service_id}' is not permitted to "
                f"acquire '{block_type.value}'"
            )
        if current_count >= self.max_blocks:
            raise QuotaExceededError(
                f"service '{self.service_id}' has reached max_blocks={self.max_blocks}"
            )
