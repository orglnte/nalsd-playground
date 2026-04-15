"""
platform_api.Client — HTTP client for platformd.

Thin wrapper over `httpx.Client`. The method surface mirrors the
daemon's OpenAPI spec (`POST /sessions`, `POST /acquire`,
`POST /drop-to-scaling-only`, `POST /scale-hint`, `DELETE /sessions`).

The Client does NOT track PrivilegeState or any other piece of session
state. The daemon is the sole authority on what calls are legal at any
given moment; every method round-trips, and the daemon's answer is the
answer. The Client holds only the bearer token obtained at `connect()`
and uses it as `Authorization: Bearer` on every subsequent request.
"""

from __future__ import annotations

import contextlib
import logging
from typing import Any

import httpx

from platform_api.errors import PlatformError, ProvisioningError
from platform_api.protocol import exception_for
from platform_api.types import (
    BlockSpec,
    BlockType,
    ComputeSpec,
    Credentials,
    Persistence,
    StorageSpec,
)

log = logging.getLogger("platform_api.client")

DEFAULT_BASE_URL = "http://127.0.0.1:8080"


class Client:
    def __init__(
        self,
        service_id: str,
        *,
        base_url: str | None = None,
        timeout: float = 30.0,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.service_id = service_id
        self._base_url = base_url or DEFAULT_BASE_URL
        self._timeout = timeout
        self._http: httpx.Client | None = http_client
        self._owns_http = http_client is None
        self._token: str | None = None
        self._session_id: str | None = None

    def connect(self) -> None:
        if self._token is not None:
            return
        if self._http is None:
            self._http = httpx.Client(base_url=self._base_url, timeout=self._timeout)
        resp = self._http.post("/sessions", json={"service_id": self.service_id})
        body = self._parse_or_raise(resp, "POST /sessions")
        self._session_id = body["session_id"]
        self._token = body["token"]
        log.info(
            "client session started: service=%s session=%s",
            self.service_id,
            self._session_id,
        )

    def close(self) -> None:
        if self._http is None:
            return
        if self._token is not None:
            with contextlib.suppress(Exception):
                self._http.delete("/sessions", headers=self._auth_headers())
        self._token = None
        self._session_id = None
        if self._owns_http:
            try:
                self._http.close()
            finally:
                self._http = None

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
        memory_mb: int | None = None,
        storage_mb: int | None = None,
        rps: int | None = None,
        persistent: bool = False,
        ram_backed: bool = False,
        **params: Any,
    ) -> Credentials:
        bt = BlockType(block_type) if isinstance(block_type, str) else block_type

        if persistent and ram_backed:
            raise ValueError("persistent=True and ram_backed=True are mutually exclusive")
        if ram_backed:
            persistence = Persistence.TMPFS
        elif persistent:
            persistence = Persistence.PERSISTENT
        else:
            persistence = Persistence.EPHEMERAL

        storage_spec: StorageSpec | None
        if storage_mb is not None or persistent or ram_backed:
            storage_spec = StorageSpec(size_mb=storage_mb, persistence=persistence)
        else:
            storage_spec = None

        compute_spec = ComputeSpec(memory_mb=memory_mb) if memory_mb is not None else None

        # Client-side validation: surfaces e.g. the rps+compute XOR rule as
        # a ValueError before we round-trip.
        BlockSpec(
            name=name,
            block_type=bt,
            compute=compute_spec,
            storage=storage_spec,
            rps=rps,
            params=params,
        )

        body: dict[str, Any] = {
            "block_type": bt.value,
            "name": name,
            "params": params,
        }
        if compute_spec is not None:
            body["compute"] = {"memory_mb": compute_spec.memory_mb}
        if storage_spec is not None:
            storage_payload: dict[str, Any] = {"persistence": storage_spec.persistence.value}
            if storage_spec.size_mb is not None:
                storage_payload["size_mb"] = storage_spec.size_mb
            body["storage"] = storage_payload
        if rps is not None:
            body["rps"] = rps

        result = self._post("/acquire", body)
        return Credentials(
            block_type=BlockType(result["block_type"]),
            name=result["name"],
            host=result["host"],
            port=result["port"],
            username=result.get("username"),
            password=result.get("password"),
            database=result.get("database"),
            extras=result.get("extras") or {},
        )

    def drop_to_scaling_only(self) -> None:
        self._post("/drop-to-scaling-only", None, expect_body=False)

    def scale_hint(self, name: str, *, load_factor: float) -> None:
        self._post(
            "/scale-hint",
            {"name": name, "load_factor": load_factor},
            expect_body=False,
        )

    def shutdown(self) -> None:
        self.close()

    # -- internals --

    def _auth_headers(self) -> dict[str, str]:
        assert self._token is not None, "client is not connected"
        return {"Authorization": f"Bearer {self._token}"}

    def _post(
        self, path: str, body: dict[str, Any] | None, *, expect_body: bool = True
    ) -> Any:
        if self._http is None or self._token is None:
            raise RuntimeError("client is not connected; call connect() first")
        resp = self._http.post(path, json=body, headers=self._auth_headers())
        return self._parse_or_raise(resp, f"POST {path}", expect_body=expect_body)

    def _parse_or_raise(
        self, resp: httpx.Response, context: str, *, expect_body: bool = True
    ) -> Any:
        if 200 <= resp.status_code < 300:
            if not expect_body or resp.status_code == 204:
                return None
            try:
                return resp.json()
            except ValueError as e:
                raise ProvisioningError(f"{context}: malformed JSON response: {e}") from e

        detail = self._decode_error_detail(resp)
        if isinstance(detail, dict) and isinstance(detail.get("code"), str):
            exc = exception_for(detail["code"], detail.get("message") or "unknown error")
            if isinstance(exc, PlatformError):
                raise exc
            raise exc
        message = detail if isinstance(detail, str) else resp.text or "<empty>"
        raise ProvisioningError(f"{context}: HTTP {resp.status_code}: {message}")

    def _decode_error_detail(self, resp: httpx.Response) -> Any:
        try:
            payload = resp.json()
        except ValueError:
            return None
        if isinstance(payload, dict) and "detail" in payload:
            return payload["detail"]
        return payload
