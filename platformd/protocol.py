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
from platform_api.types import Credentials

ERROR_CODES: dict[type[PlatformError], str] = {
    PrivilegeDroppedError: "privilege_dropped",
    InvalidStateError: "invalid_state",
    QuotaExceededError: "quota_exceeded",
    UnknownBlockError: "unknown_block",
    ProvisioningError: "provisioning",
    ReadinessTimeoutError: "readiness_timeout",
}


def encode_credentials(c: Credentials) -> dict[str, Any]:
    d = asdict(c)
    d["block_type"] = c.block_type.value
    return d


def decode_credentials(d: dict[str, Any]) -> Credentials:
    from platform_api.types import BlockType

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


def error_response(request_id: Any, exc: BaseException) -> dict[str, Any]:
    code = ERROR_CODES.get(type(exc), "internal_error")
    if isinstance(exc, ValueError) and code == "internal_error":
        code = "invalid_request"
    return {
        "id": request_id,
        "error": {"code": code, "message": str(exc)},
    }


def result_response(request_id: Any, result: Any) -> dict[str, Any]:
    return {"id": request_id, "result": result}
