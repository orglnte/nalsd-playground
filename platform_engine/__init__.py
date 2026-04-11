"""
platform_engine — the daemon-only half of the Platform API.

Holds the PulumiDockerEngine, the building-block catalog, the Engine
Protocol seam, and the TOML scope loader. Nothing in this package is
imported by services; only platformd (and tests) reach in here.

The service-facing half is platform_api (types, errors, protocol,
manifesto) — that's the shared wire vocabulary plus small service-side
helpers. Services should never depend on platform_engine.
"""

from __future__ import annotations

from platform_engine.blocks import (
    DEFAULT_HOST_PORTS,
    BackendConfig,
    ReadinessCheck,
    backend_for,
)
from platform_engine.engine import PulumiDockerEngine
from platform_engine.engine_protocol import Engine
from platform_engine.scope import load_scope

__all__ = [
    "BackendConfig",
    "DEFAULT_HOST_PORTS",
    "Engine",
    "PulumiDockerEngine",
    "ReadinessCheck",
    "backend_for",
    "load_scope",
]
