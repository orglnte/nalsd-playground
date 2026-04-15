from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from platform_api.errors import (
    InvalidStateError,
    PrivilegeDroppedError,
    QuotaExceededError,
)
from platform_api.types import (
    BlockSpec,
    BlockType,
    ComputeSpec,
    Credentials,
    PrivilegeState,
    ServiceScope,
    StorageSpec,
)
from platformd.engine_protocol import Engine

log = logging.getLogger("platformd.session")

# Hard ceiling for record mode. Even an operator-opted-in recording
# cannot accumulate more than this; the point is to learn the service's
# real shape, not to paper over a runaway bug.
RECORD_MAX_BLOCKS = 32


class Session:
    """
    Per-connection session state inside the daemon.

    Base class owns the state machine (ACQUIRING → OPERATIONAL → SHUTDOWN),
    lease bookkeeping, and provision dispatch. Subclasses implement the
    acquire-time policy: EnforcingSession checks a scope, RecordingSession
    logs and records.
    """

    def __init__(self, service_id: str, engine: Engine) -> None:
        self.service_id = service_id
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
        compute: ComputeSpec | None = None,
        storage: StorageSpec | None = None,
        rps: int | None = None,
        **params: Any,
    ) -> Credentials:
        if self._state is not PrivilegeState.ACQUIRING:
            raise PrivilegeDroppedError(
                f"acquire() not permitted in state {self._state.value}; "
                f"privileges have been dropped for service '{self.service_id}'"
            )

        bt = BlockType(block_type) if isinstance(block_type, str) else block_type
        self._check_acquire(bt)

        if name in self._leases:
            existing = self._leases[name]
            if existing.block_type is not bt:
                raise ValueError(
                    f"lease '{name}' already exists with block_type="
                    f"{existing.block_type.value}, cannot re-acquire as {bt.value}"
                )
            log.info("acquire: reusing lease %s (%s)", name, bt.value)
            return self._credentials[name]

        spec = BlockSpec(
            name=name,
            block_type=bt,
            compute=compute,
            storage=storage,
            rps=rps,
            params=params,
        )
        log.info(
            "acquire: service=%s block=%s name=%s compute=%s storage=%s rps=%s",
            self.service_id,
            bt.value,
            name,
            compute,
            storage,
            rps,
        )
        prospective = {**self._leases, name: spec}
        credentials = self._engine.provision(spec, existing_leases=prospective)
        self._leases[name] = spec
        self._credentials[name] = credentials
        self._post_acquire(name)
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
        self._on_drop()
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
        self._on_shutdown()
        self._state = PrivilegeState.SHUTDOWN

    # -- hooks for subclasses --

    def _check_acquire(self, bt: BlockType) -> None:
        """Called before provisioning. Raise to reject."""

    def _post_acquire(self, name: str) -> None:
        """Called after a successful provision."""

    def _on_drop(self) -> None:
        """Called when transitioning to OPERATIONAL."""

    def _on_shutdown(self) -> None:
        """Called on session shutdown."""


class EnforcingSession(Session):
    """Checks every acquire() against a pre-loaded ServiceScope."""

    def __init__(self, service_id: str, engine: Engine, scope: ServiceScope) -> None:
        if scope.service_id != service_id:
            raise ValueError(
                f"scope.service_id='{scope.service_id}' does not match "
                f"authenticated service_id='{service_id}'"
            )
        super().__init__(service_id, engine)
        self._scope = scope

    def _check_acquire(self, bt: BlockType) -> None:
        self._scope.check(bt, current_count=len(self._leases))


class RecordingSession(Session):
    """Records acquire() calls and writes a .recorded.toml on drop/shutdown."""

    def __init__(self, service_id: str, engine: Engine, recording_output: Path) -> None:
        super().__init__(service_id, engine)
        self._recording_output = recording_output
        self._recorded_order: list[str] = []
        self._recording_written = False
        log.warning(
            "session in RECORD mode: service=%s — scope will be derived "
            "from acquire() calls, output=%s",
            service_id,
            recording_output,
        )

    def _check_acquire(self, bt: BlockType) -> None:
        if len(self._leases) >= RECORD_MAX_BLOCKS:
            raise QuotaExceededError(
                f"service '{self.service_id}' reached record-mode "
                f"ceiling max_blocks={RECORD_MAX_BLOCKS}"
            )
        log.warning(
            "RECORD: service=%s acquire block=%s (no scope enforcement, recording only)",
            self.service_id,
            bt.value,
        )

    def _post_acquire(self, name: str) -> None:
        self._recorded_order.append(name)

    def _on_drop(self) -> None:
        self._write_recording()

    def _on_shutdown(self) -> None:
        if not self._recording_written:
            self._write_recording()

    def _write_recording(self) -> None:
        if not self._recorded_order:
            log.info(
                "RECORD: service=%s — no acquires observed; skipping write",
                self.service_id,
            )
            self._recording_written = True
            return

        ts = datetime.now(UTC).isoformat(timespec="seconds")
        block_types_seen = sorted({self._leases[n].block_type.value for n in self._recorded_order})
        allowed_list = ", ".join(f'"{b}"' for b in block_types_seen)
        body = (
            f"# recorded by platformd at {ts}\n"
            f"# review, then rename to {self.service_id}.toml to promote to enforced mode.\n"
            f"# observed acquires: {len(self._recorded_order)}\n"
            f'service_id = "{self.service_id}"\n'
            f"allowed_blocks = [{allowed_list}]\n"
            f"max_blocks = {len(self._recorded_order)}\n"
        )
        self._recording_output.parent.mkdir(parents=True, exist_ok=True)
        self._recording_output.write_text(body, encoding="utf-8")
        self._recording_written = True
        log.warning(
            "RECORD: service=%s wrote %s (%d acquires captured)",
            self.service_id,
            self._recording_output,
            len(self._recorded_order),
        )
