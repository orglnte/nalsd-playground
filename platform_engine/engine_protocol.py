"""
Engine Protocol — the narrow seam the daemon uses to reach a provisioner.

platformd's Session holds an Engine and calls exactly these two methods.
The concrete implementation today is PulumiDockerEngine; any other
engine (a future microVM driver, an in-test FakeEngine) satisfies the
Protocol structurally — no nominal inheritance required. Keeping the
Protocol in its own tiny module means consumers can import it without
dragging in Pulumi, Docker, or any other backend-specific dependency.
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
