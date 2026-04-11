"""
ServiceScope loader.

Parses a per-service TOML file into a ServiceScope value object. This is
the seam that moves the authorization declaration out of service Python
code and into a file the platform-ops team owns — a prerequisite for the
daemon split (which will own this loader on behalf of services).

Schema:

    service_id     = "<string>"          # required
    allowed_blocks = ["<block-type>", …] # required; values must match BlockType
    max_blocks     = <int>               # optional, default 16
"""

from __future__ import annotations

import tomllib
from pathlib import Path

from platform_api.types import BlockType, ServiceScope


def load_scope(path: Path) -> ServiceScope:
    if not path.is_file():
        raise FileNotFoundError(f"scope file not found: {path}")
    try:
        data = tomllib.loads(path.read_text())
    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"malformed scope file {path}: {e}") from e

    missing = {"service_id", "allowed_blocks"} - data.keys()
    if missing:
        raise ValueError(
            f"scope file {path} missing required keys: {sorted(missing)}"
        )

    try:
        allowed = {BlockType(b) for b in data["allowed_blocks"]}
    except ValueError as e:
        raise ValueError(f"scope file {path}: unknown block type: {e}") from e

    return ServiceScope(
        service_id=data["service_id"],
        allowed_blocks=allowed,
        max_blocks=data.get("max_blocks", 16),
    )
