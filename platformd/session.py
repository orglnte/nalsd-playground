from __future__ import annotations

import logging
from typing import Any

from platform_api.engine_protocol import Engine
from platform_api.errors import (
    InvalidStateError,
    PrivilegeDroppedError,
)
from platform_api.types import (
    BlockSpec,
    BlockType,
    Credentials,
    PrivilegeState,
    ServiceScope,
)

log = logging.getLogger("platformd.session")


class Session:
    """
    Per-connection session state inside the daemon.

    Scope is provided by the ScopeStore at connect time based on the
    authenticated service_id — never from the client. The engine is
    shared across reconnects from the same service_id (held by the
    server), but PrivilegeState and leases reset on each new connection.
    """

    def __init__(
        self,
        service_id: str,
        scope: ServiceScope,
        engine: Engine,
    ) -> None:
        if scope.service_id != service_id:
            raise ValueError(
                f"scope.service_id='{scope.service_id}' does not match "
                f"authenticated service_id='{service_id}'"
            )
        self.service_id = service_id
        self._scope = scope
        self._engine = engine
        self._state = PrivilegeState.ACQUIRING
        self._leases: dict[str, BlockSpec] = {}
        self._credentials: dict[str, Credentials] = {}

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
                f"acquire() not permitted in state {self._state.value}; "
                f"privileges have been dropped for service '{self.service_id}'"
            )

        bt = BlockType(block_type) if isinstance(block_type, str) else block_type
        self._scope.check(bt, current_count=len(self._leases))

        if name in self._leases:
            existing = self._leases[name]
            if existing.block_type is not bt:
                raise ValueError(
                    f"lease '{name}' already exists with block_type="
                    f"{existing.block_type.value}, cannot re-acquire as {bt.value}"
                )
            log.info("acquire: reusing lease %s (%s)", name, bt.value)
            return self._credentials[name]

        spec = BlockSpec(name=name, block_type=bt, profile=profile, params=params)
        log.info(
            "acquire: service=%s block=%s name=%s profile=%s",
            self.service_id,
            bt.value,
            name,
            profile,
        )
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
            raise InvalidStateError("cannot drop privileges after shutdown")
        log.info(
            "drop_to_scaling_only: service=%s leases=%d",
            self.service_id,
            len(self._leases),
        )
        self._state = PrivilegeState.OPERATIONAL

    def scale_hint(self, name: str, *, load_factor: float) -> None:
        if self._state is PrivilegeState.SHUTDOWN:
            raise InvalidStateError("scale_hint() not permitted after shutdown")
        if self._state is not PrivilegeState.OPERATIONAL:
            raise InvalidStateError(
                f"scale_hint() requires OPERATIONAL, got {self._state.value}; "
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

    def shutdown(self) -> None:
        if self._state is PrivilegeState.SHUTDOWN:
            return
        log.info(
            "shutdown: service=%s leases=%d",
            self.service_id,
            len(self._leases),
        )
        self._state = PrivilegeState.SHUTDOWN
