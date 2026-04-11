from __future__ import annotations

import logging
from typing import Any

from platform_api.errors import InvalidStateError, PrivilegeDroppedError
from platform_api.types import (
    BlockSpec,
    BlockType,
    CapabilityManifest,
    Credentials,
    PrivilegeState,
)

log = logging.getLogger("platform_api.client")


class PlatformClient:
    """
    The service-facing surface of the Platform API.

    Lifecycle:
        1. Construct with a service_id and a CapabilityManifest.
        2. Call acquire() one or more times during startup. Each call blocks
           until the building block is provisioned and ready, then returns
           Credentials.
        3. Call drop_to_scaling_only() exactly once. After this, acquire()
           raises PrivilegeDroppedError.
        4. Call scale_hint() as needed during normal operation.

    The privilege drop is a Linux-capability-style state transition: it is
    irreversible within the process. Restarting the service resets the state
    machine to ACQUIRING.
    """

    def __init__(
        self,
        service_id: str,
        manifest: CapabilityManifest,
        *,
        engine: Any | None = None,
    ) -> None:
        if manifest.service_id != service_id:
            raise ValueError(
                "manifest.service_id does not match PlatformClient service_id"
            )
        self.service_id = service_id
        self._manifest = manifest
        self._state = PrivilegeState.ACQUIRING
        self._leases: dict[str, BlockSpec] = {}
        self._credentials: dict[str, Credentials] = {}

        if engine is None:
            from platform_api.engine import PulumiDockerEngine

            engine = PulumiDockerEngine(service_id=service_id)
        self._engine = engine

    @property
    def state(self) -> PrivilegeState:
        return self._state

    def acquire(
        self,
        block_type: str | BlockType,
        *,
        name: str,
        profile: str = "minimal",
        **params: Any,
    ) -> Credentials:
        if self._state is not PrivilegeState.ACQUIRING:
            raise PrivilegeDroppedError(
                f"acquire() is not permitted in state {self._state.value}; "
                "privileges have been dropped for this service"
            )

        bt = BlockType(block_type) if isinstance(block_type, str) else block_type
        self._manifest.check(bt, current_count=len(self._leases))

        if name in self._leases:
            existing = self._leases[name]
            if existing.block_type is not bt:
                raise ValueError(
                    f"lease '{name}' already exists with block_type="
                    f"{existing.block_type.value}, cannot re-acquire as {bt.value}"
                )
            log.info("acquire: reusing existing lease %s (%s)", name, bt.value)
            return self._credentials[name]

        spec = BlockSpec(name=name, block_type=bt, profile=profile, params=params)
        log.info(
            "acquire: service=%s block=%s name=%s profile=%s",
            self.service_id,
            bt.value,
            name,
            profile,
        )
        # Pass the prospective lease set (existing + this one) to the engine
        # without mutating self._leases. Only on successful provision do we
        # commit the lease and its credentials, so a failed acquire() leaves
        # the client in a clean state that can be retried.
        prospective = {**self._leases, name: spec}
        credentials = self._engine.provision(spec, existing_leases=prospective)
        self._leases[name] = spec
        self._credentials[name] = credentials
        return credentials

    def drop_to_scaling_only(self) -> None:
        if self._state is PrivilegeState.OPERATIONAL:
            log.warning("drop_to_scaling_only called twice; ignoring")
            return
        if self._state is PrivilegeState.SHUTDOWN:
            raise InvalidStateError(
                "cannot drop privileges after shutdown"
            )
        log.info(
            "drop_to_scaling_only: service=%s held_leases=%d",
            self.service_id,
            len(self._leases),
        )
        self._state = PrivilegeState.OPERATIONAL

    def scale_hint(self, name: str, *, load_factor: float) -> None:
        if self._state is PrivilegeState.SHUTDOWN:
            raise InvalidStateError(
                "scale_hint() not permitted after shutdown"
            )
        if self._state is not PrivilegeState.OPERATIONAL:
            raise InvalidStateError(
                f"scale_hint() requires OPERATIONAL state, got {self._state.value}; "
                "call drop_to_scaling_only() first"
            )
        if name not in self._leases:
            raise ValueError(f"unknown lease '{name}'")
        log.info(
            "scale_hint: service=%s lease=%s load_factor=%.2f",
            self.service_id,
            name,
            load_factor,
        )

    def shutdown(self, *, destroy: bool = False) -> None:
        if self._state is PrivilegeState.SHUTDOWN:
            return
        log.info(
            "shutdown: service=%s destroy=%s leases=%d",
            self.service_id,
            destroy,
            len(self._leases),
        )
        if destroy:
            self._engine.destroy()
        self._state = PrivilegeState.SHUTDOWN
