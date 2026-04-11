"""
Engine Protocol — the narrow seam between PlatformClient and whatever
actually provisions infrastructure.

PlatformClient talks to an Engine through exactly these two methods. Any
concrete engine — the in-process PulumiDockerEngine today, a UDS-backed
daemon client tomorrow — satisfies the Protocol structurally (no nominal
inheritance required). Keeping the Protocol in its own tiny module means
consumers can import it without dragging in Pulumi, Docker, gRPC, or any
other engine-side dependency.
"""

from __future__ import annotations

from typing import Protocol

from platform_api.types import BlockSpec, Credentials


class Engine(Protocol):
    def provision(
        self,
        spec: BlockSpec,
        *,
        existing_leases: dict[str, BlockSpec],
    ) -> Credentials: ...

    def destroy(self) -> None: ...
