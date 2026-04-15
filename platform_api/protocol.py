"""
Wire protocol vocabulary shared by platformd and platform_api.Client.

This is the single authoritative source for how PlatformError subclasses
map to wire-level error codes (and back). Both sides import the SAME
table — adding a new error class is a one-file edit, and neither the
daemon nor the client can drift out of sync with the other because
there is no second mapping to maintain.

`encode_credentials` / `decode_credentials` and `encode_block_spec` /
`decode_block_spec` are the other wire-shaped helpers: they turn the
relevant dataclasses into the dicts the daemon emits and the client
consumes (and vice versa).
"""

from __future__ import annotations

from dataclasses import asdict
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
from platform_api.types import (
    BlockSpec,
    BlockType,
    ComputeSpec,
    Credentials,
    Persistence,
    StorageSpec,
)

# Single authoritative exception ↔ wire-code mapping.
_ERROR_TABLE: tuple[tuple[type[PlatformError], str], ...] = (
    (PrivilegeDroppedError, "privilege_dropped"),
    (InvalidStateError, "invalid_state"),
    (QuotaExceededError, "quota_exceeded"),
    (UnknownBlockError, "unknown_block"),
    (ProvisioningError, "provisioning"),
    (ReadinessTimeoutError, "readiness_timeout"),
)

_CODE_BY_EXC: dict[type[PlatformError], str] = {cls: code for cls, code in _ERROR_TABLE}
_EXC_BY_CODE: dict[str, type[PlatformError]] = {code: cls for cls, code in _ERROR_TABLE}


def code_for(exc: BaseException) -> str:
    """Wire code for an exception instance.

    Unknown PlatformError subclasses fall back to "internal_error".
    Bare ValueError from the daemon's wire decoder becomes
    "invalid_request" so the client can distinguish protocol problems
    from platform failures.
    """
    code = _CODE_BY_EXC.get(type(exc), "internal_error")  # type: ignore[arg-type]
    if isinstance(exc, ValueError) and code == "internal_error":
        return "invalid_request"
    return code


def exception_for(code: str, message: str) -> PlatformError:
    """Reconstruct a PlatformError subclass from a wire code.

    Unknown codes produce a plain PlatformError so the client always
    raises something the caller can catch.
    """
    exc_type = _EXC_BY_CODE.get(code, PlatformError)
    return exc_type(message)


def encode_credentials(c: Credentials) -> dict[str, Any]:
    d = asdict(c)
    d["block_type"] = c.block_type.value
    return d


def decode_credentials(d: dict[str, Any]) -> Credentials:
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


def encode_block_spec(spec: BlockSpec) -> dict[str, Any]:
    """Serialize a BlockSpec for the Acquire RPC payload."""
    payload: dict[str, Any] = {
        "name": spec.name,
        "block_type": spec.block_type.value,
        "params": dict(spec.params),
    }
    if spec.compute is not None:
        payload["compute"] = {"memory_mb": spec.compute.memory_mb}
    if spec.storage is not None:
        storage_payload: dict[str, Any] = {
            "persistence": spec.storage.persistence.value,
        }
        if spec.storage.size_mb is not None:
            storage_payload["size_mb"] = spec.storage.size_mb
        payload["storage"] = storage_payload
    if spec.rps is not None:
        payload["rps"] = spec.rps
    return payload


def decode_block_spec(d: dict[str, Any]) -> BlockSpec:
    """Reconstruct a BlockSpec from an Acquire RPC payload."""
    compute_d = d.get("compute")
    storage_d = d.get("storage")
    compute = ComputeSpec(memory_mb=int(compute_d["memory_mb"])) if compute_d else None
    storage: StorageSpec | None
    if storage_d:
        size_mb = storage_d.get("size_mb")
        storage = StorageSpec(
            size_mb=int(size_mb) if size_mb is not None else None,
            persistence=Persistence(storage_d.get("persistence", Persistence.EPHEMERAL.value)),
        )
    else:
        storage = None
    return BlockSpec(
        name=d["name"],
        block_type=BlockType(d["block_type"]),
        compute=compute,
        storage=storage,
        rps=int(d["rps"]) if d.get("rps") is not None else None,
        params=dict(d.get("params") or {}),
    )


def error_response(request_id: Any, exc: BaseException) -> dict[str, Any]:
    return {
        "id": request_id,
        "error": {"code": code_for(exc), "message": str(exc)},
    }


def result_response(request_id: Any, result: Any) -> dict[str, Any]:
    return {"id": request_id, "result": result}
