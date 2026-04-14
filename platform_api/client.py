"""
platform_api.Client — UDS client for platformd.

This is a thin transport-and-codec layer. It owns a socket, serialises
JSON-RPC calls onto it, and decodes error responses back into the
PlatformError hierarchy via platform_api.protocol.

Crucially, the Client does NOT track PrivilegeState or any other piece
of session state. The daemon is the sole authority on what calls are
legal at any given moment; every method round-trips, and the daemon's
answer is the answer. An earlier version of this class mirrored the
state machine locally as a round-trip optimization; the duplication
grew a second copy of the transition logic, a second error-code table,
and a test whose whole purpose was to prove the local copy was not
load-bearing. Deleting the local copy is simpler, and the
trust-boundary semantics are unchanged.
"""

from __future__ import annotations

import json
import logging
import socket
from pathlib import Path
from typing import Any

from platform_api.protocol import decode_credentials, exception_for
from platform_api.types import BlockType, Credentials

log = logging.getLogger("platform_api.client")

DEFAULT_SOCKET_PATH = Path("dev-config/run/platformd.sock")


class Client:
    def __init__(
        self,
        service_id: str,
        *,
        socket_path: Path | str = DEFAULT_SOCKET_PATH,
    ) -> None:
        self.service_id = service_id
        self._socket_path = Path(socket_path)
        self._sock: socket.socket | None = None
        self._reader: Any = None
        self._next_id = 0

    def connect(self) -> None:
        if self._sock is not None:
            return
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(str(self._socket_path))
        self._sock = s
        self._reader = s.makefile("rb", buffering=0)
        log.info(
            "client connected: service=%s socket=%s",
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
        bt_value = block_type.value if isinstance(block_type, BlockType) else block_type
        result = self._call(
            "Acquire",
            {
                "block_type": bt_value,
                "name": name,
                "profile": profile,
                "params": params,
            },
        )
        return decode_credentials(result)

    def drop_to_scaling_only(self) -> None:
        self._call("DropToScalingOnly", {})

    def scale_hint(self, name: str, *, load_factor: float) -> None:
        self._call("ScaleHint", {"name": name, "load_factor": load_factor})

    def shutdown(self) -> None:
        if self._sock is None:
            return
        try:
            self._call("Shutdown", {})
        except Exception:
            pass
        finally:
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
            from platform_api.errors import ProvisioningError

            raise ProvisioningError(f"platformd closed the connection during {method}")
        try:
            response = json.loads(line.decode("utf-8"))
        except json.JSONDecodeError as e:
            from platform_api.errors import ProvisioningError

            raise ProvisioningError(f"malformed response from platformd: {e}") from e

        if "error" in response:
            err = response["error"]
            raise exception_for(
                err.get("code", "internal_error"),
                err.get("message", "unknown error"),
            )
        return response.get("result")
