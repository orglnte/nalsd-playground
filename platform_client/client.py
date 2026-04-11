from __future__ import annotations

import json
import logging
import socket
from pathlib import Path
from typing import Any

from platform_api.errors import (
    InvalidStateError,
    PlatformError,
    PrivilegeDroppedError,
    ProvisioningError,
    QuotaExceededError,
    ReadinessTimeoutError,
    UnknownBlockError,
)
from platform_api.types import BlockType, Credentials, PrivilegeState

log = logging.getLogger("platform_client")

DEFAULT_SOCKET_PATH = Path("dev-config/run/platformd.sock")

_ERROR_BY_CODE: dict[str, type[PlatformError]] = {
    "privilege_dropped": PrivilegeDroppedError,
    "invalid_state": InvalidStateError,
    "quota_exceeded": QuotaExceededError,
    "unknown_block": UnknownBlockError,
    "provisioning": ProvisioningError,
    "readiness_timeout": ReadinessTimeoutError,
}


class Client:
    """
    Service-side client for platformd.

    Construct, call connect(), then call acquire() / drop_to_scaling_only() /
    scale_hint(). Errors from the daemon are decoded into the existing
    PlatformError hierarchy, so service code does not need to care that
    the engine is remote.

    Client-side PrivilegeState is advisory — the daemon is the authority
    and re-checks on every call. The pre-check here only exists to give
    service code a local fast-fail without a round-trip in the obvious
    wrong-state cases.
    """

    def __init__(
        self,
        service_id: str,
        *,
        socket_path: Path | str = DEFAULT_SOCKET_PATH,
    ) -> None:
        self.service_id = service_id
        self._socket_path = Path(socket_path)
        self._sock: socket.socket | None = None
        self._reader: Any = None  # binary file object
        self._next_id = 0
        self._state = PrivilegeState.ACQUIRING

    @property
    def state(self) -> PrivilegeState:
        return self._state

    def connect(self) -> None:
        if self._sock is not None:
            return
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(str(self._socket_path))
        self._sock = s
        self._reader = s.makefile("rb", buffering=0)
        log.info(
            "platform_client connected: service=%s socket=%s",
            self.service_id,
            self._socket_path,
        )

    def close(self) -> None:
        if self._reader is not None:
            try:
                self._reader.close()
            finally:
                self._reader = None
        if self._sock is not None:
            try:
                self._sock.close()
            finally:
                self._sock = None

    def __enter__(self) -> Client:
        self.connect()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

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
                "privileges have already been dropped for this client"
            )
        bt_value = (
            block_type.value if isinstance(block_type, BlockType) else block_type
        )
        result = self._call(
            "Acquire",
            {
                "block_type": bt_value,
                "name": name,
                "profile": profile,
                "params": params,
            },
        )
        return _decode_credentials(result)

    def drop_to_scaling_only(self) -> None:
        if self._state is PrivilegeState.OPERATIONAL:
            log.warning("drop_to_scaling_only called twice; ignoring")
            return
        if self._state is PrivilegeState.SHUTDOWN:
            raise InvalidStateError("cannot drop privileges after shutdown")
        self._call("DropToScalingOnly", {})
        self._state = PrivilegeState.OPERATIONAL

    def scale_hint(self, name: str, *, load_factor: float) -> None:
        if self._state is PrivilegeState.SHUTDOWN:
            raise InvalidStateError("scale_hint() not permitted after shutdown")
        if self._state is not PrivilegeState.OPERATIONAL:
            raise InvalidStateError(
                f"scale_hint() requires OPERATIONAL, got {self._state.value}; "
                "call drop_to_scaling_only() first"
            )
        self._call(
            "ScaleHint", {"name": name, "load_factor": load_factor}
        )

    def shutdown(self) -> None:
        if self._state is PrivilegeState.SHUTDOWN:
            return
        try:
            if self._sock is not None:
                self._call("Shutdown", {})
        finally:
            self._state = PrivilegeState.SHUTDOWN
            self.close()

    def _call(self, method: str, params: dict[str, Any]) -> Any:
        if self._sock is None:
            raise RuntimeError("client is not connected; call connect() first")
        self._next_id += 1
        request = {"id": self._next_id, "method": method, "params": params}
        payload = (json.dumps(request) + "\n").encode("utf-8")
        self._sock.sendall(payload)

        line = self._reader.readline()
        if not line:
            raise ProvisioningError(
                f"platformd closed the connection during {method}"
            )
        try:
            response = json.loads(line.decode("utf-8"))
        except json.JSONDecodeError as e:
            raise ProvisioningError(
                f"malformed response from platformd: {e}"
            ) from e

        if "error" in response:
            err = response["error"]
            code = err.get("code", "internal_error")
            message = err.get("message", "unknown error")
            exc_type = _ERROR_BY_CODE.get(code, PlatformError)
            raise exc_type(message)
        return response.get("result")


def _decode_credentials(d: dict[str, Any]) -> Credentials:
    return Credentials(
        block_type=BlockType(d["block_type"]),
        name=d["name"],
        host=d["host"],
        port=d["port"],
        username=d.get("username"),
        password=d.get("password"),
        database=d.get("database"),
        extras=d.get("extras") or {},
    )
