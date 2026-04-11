"""
platform_api — the shared wire vocabulary.

Both the service (via platform_client) and the daemon (platformd) import
from here. This package intentionally holds *only* what has to be shared:

- types (BlockType, PrivilegeState, Credentials, BlockSpec, ServiceScope)
- errors (PlatformError hierarchy)
- protocol (bidirectional error-code mapping + credentials encoders)
- manifesto (service-side SQL migration runner — takes Credentials, runs
  SQL; used at bootstrap time after the transactional-store is acquired)

The engine, blocks, and scope loader live in platform_engine/ — only
the daemon imports those.
"""

from __future__ import annotations

from platform_api.errors import (
    InvalidStateError,
    PlatformError,
    PrivilegeDroppedError,
    ProvisioningError,
    QuotaExceededError,
    ReadinessTimeoutError,
    UnknownBlockError,
)
from platform_api.manifesto import apply_manifesto
from platform_api.types import (
    BlockSpec,
    BlockType,
    Credentials,
    PrivilegeState,
    ServiceScope,
)

__all__ = [
    "apply_manifesto",
    "BlockSpec",
    "BlockType",
    "Credentials",
    "InvalidStateError",
    "PlatformError",
    "PrivilegeDroppedError",
    "PrivilegeState",
    "ProvisioningError",
    "QuotaExceededError",
    "ReadinessTimeoutError",
    "ServiceScope",
    "UnknownBlockError",
]
