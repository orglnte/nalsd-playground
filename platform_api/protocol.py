"""
Wire error vocabulary shared by platformd and platform_api.Client.

Both the HTTP server (platformd) and the HTTP client (platform_api.Client)
import the SAME exception ↔ wire-code table, so there is no second
mapping to keep in sync. Adding a new error class is a one-file edit.

The server serializes platform errors as JSON with
`{"code": "...", "message": "..."}` in the HTTPException detail; the
client reconstructs the matching PlatformError subclass via
`exception_for(code, message)`.
"""

from __future__ import annotations

from platform_api.errors import (
    InvalidStateError,
    PlatformError,
    PrivilegeDroppedError,
    ProvisioningError,
    QuotaExceededError,
    ReadinessTimeoutError,
    UnknownBlockError,
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
    Bare ValueError from the daemon's validation layer becomes
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
