"""
platform_api — the service-facing SDK + shared wire vocabulary.

Everything a service needs to talk to the Platform lives here:

- types      BlockType, PrivilegeState, Credentials, BlockSpec, ServiceScope
- errors     PlatformError hierarchy
- protocol   bidirectional error-code mapping + credentials encoders
             (imported by BOTH sides: the service via Client, the
             daemon via platformd.server)
- manifesto  service-side SQL migration runner that runs at bootstrap
             once the transactional-store is acquired
- client     the UDS Client services instantiate to call the daemon

The daemon (platformd) imports types, errors, and protocol from here
but NEVER imports Client — that direction would be nonsense (the
daemon is the thing Client talks to). Python does not enforce this;
code review does.
"""

from __future__ import annotations

from platform_api.client import Client
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
    "BlockSpec",
    "BlockType",
    "Client",
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
    "apply_manifesto",
]
