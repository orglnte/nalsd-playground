from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from platformd.engine_protocol import Engine
from platform_api.errors import (
    InvalidStateError,
    PrivilegeDroppedError,
    QuotaExceededError,
)
from platform_api.types import (
    BlockSpec,
    BlockType,
    Credentials,
    PrivilegeState,
    ServiceScope,
)

log = logging.getLogger("platformd.session")

# Hard ceiling for record mode. Even an operator-opted-in recording
# cannot accumulate more than this; the point is to learn the service's
# real shape, not to paper over a runaway bug.
RECORD_MAX_BLOCKS = 32


class Session:
    """
    Per-connection session state inside the daemon.

    In enforce mode (the default) the session is constructed with a
    ServiceScope from the ScopeStore, looked up by authenticated
    service_id. Every acquire() is checked against that scope.

    In record mode the session has no scope: acquires are recorded and
    gated only by (a) the BlockType enum whitelist, which the wire-level
    decode already enforces, and (b) RECORD_MAX_BLOCKS. On
    drop_to_scaling_only() or shutdown() the accumulated acquires are
    written to recording_output as a .recorded.toml file that the
    operator reviews and renames to promote the service to enforce mode.
    """

    def __init__(
        self,
        service_id: str,
        engine: Engine,
        *,
        scope: ServiceScope | None = None,
        mode: str = "enforce",
        recording_output: Path | None = None,
    ) -> None:
        if mode not in {"enforce", "record"}:
            raise ValueError(
                f"unknown session mode '{mode}' (must be 'enforce' or 'record')"
            )
        if mode == "enforce":
            if scope is None:
                raise ValueError("enforce mode requires a scope")
            if scope.service_id != service_id:
                raise ValueError(
                    f"scope.service_id='{scope.service_id}' does not match "
                    f"authenticated service_id='{service_id}'"
                )
        else:
            if recording_output is None:
                raise ValueError(
                    "record mode requires a recording_output path so the "
                    "observed scope can be persisted for review"
                )
            log.warning(
                "session in RECORD mode: service=%s — scope will be derived "
                "from acquire() calls, output=%s",
                service_id,
                recording_output,
            )

        self.service_id = service_id
        self._scope = scope
        self._engine = engine
        self._mode = mode
        self._recording_output = recording_output
        self._state = PrivilegeState.ACQUIRING
        self._leases: dict[str, BlockSpec] = {}
        self._credentials: dict[str, Credentials] = {}
        self._recorded_order: list[str] = []
        self._recording_written = False

    @property
    def state(self) -> PrivilegeState:
        return self._state

    @property
    def mode(self) -> str:
        return self._mode

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

        if self._mode == "record":
            if len(self._leases) >= RECORD_MAX_BLOCKS:
                raise QuotaExceededError(
                    f"service '{self.service_id}' reached record-mode "
                    f"ceiling max_blocks={RECORD_MAX_BLOCKS}"
                )
            log.warning(
                "RECORD: service=%s acquire block=%s name=%s profile=%s "
                "(no scope enforcement, recording only)",
                self.service_id,
                bt.value,
                name,
                profile,
            )
        else:
            assert self._scope is not None
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
        if self._mode == "record":
            self._recorded_order.append(name)
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
        if self._mode == "record":
            self._write_recording()
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
        # If the service disconnects without dropping, still persist what
        # we learned — a recording run whose service crashed is still a
        # useful starting point for the operator.
        if self._mode == "record" and not self._recording_written:
            self._write_recording()
        self._state = PrivilegeState.SHUTDOWN

    def _write_recording(self) -> None:
        assert self._recording_output is not None
        if not self._recorded_order:
            log.info(
                "RECORD: service=%s — no acquires observed; skipping write",
                self.service_id,
            )
            self._recording_written = True
            return

        ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
        block_types_seen = sorted(
            {self._leases[n].block_type.value for n in self._recorded_order}
        )
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
