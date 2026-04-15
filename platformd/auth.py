"""
Bootstrap-authentication seam.

The platform daemon never reads `service_id` from user-supplied fields and
takes that claim at face value. Every connection goes through a
`BootstrapVerifier` whose `verify(payload)` returns the authenticated
`service_id` (or raises `IdentityRejectedError`). All authorization
decisions downstream — scope lookup, record-mode placement, session
creation — run against that authenticated identity.

Implementations planned behind this seam:

- `TrustingVerifier` (this file) — PROTOTYPE ONLY. Accepts the caller's
  `service_id` claim validated against an allow-list. Not secure; placeholder
  for the real primitives below.
- `HmacTokenVerifier` — verifies short-lived HMAC-signed JWTs. Dev + CI.
- `CloudWorkloadIdentityVerifier` — verifies platform-minted tokens
  (GCP metadata server, AWS IAM + STS, Azure managed identity). Production.
- `MtlsCertVerifier` — verifies client certs issued by platformd's own CA
  via a bootstrap-only `/issue-cert` endpoint (SPIFFE-shaped flow).

The invariant preserved across all of these: the daemon's knowledge of
`service_id` comes from a source the caller does not control.
"""

from __future__ import annotations

from typing import Any, Protocol


class IdentityRejectedError(Exception):
    """Raised when the bootstrap payload cannot be mapped to a known,
    authenticated service_id. Becomes a 401 on the wire."""


class BootstrapVerifier(Protocol):
    def verify(self, payload: dict[str, Any]) -> str: ...


class TrustingVerifier:
    """PROTOTYPE-ONLY verifier.

    Accepts whatever `service_id` the caller claims, validated against a
    pre-configured allow-list of known service_ids. The claim is trusted
    after the name check — there is no cryptographic proof of identity.

    Not secure. Replace with `HmacTokenVerifier`,
    `CloudWorkloadIdentityVerifier`, or `MtlsCertVerifier` before any
    deployment that is not an isolated development environment.
    """

    def __init__(self, known_service_ids: frozenset[str]) -> None:
        self._known = known_service_ids

    def verify(self, payload: dict[str, Any]) -> str:
        service_id = payload.get("service_id")
        if not isinstance(service_id, str) or not service_id:
            raise IdentityRejectedError(
                "bootstrap payload missing or invalid 'service_id'"
            )
        if service_id not in self._known:
            raise IdentityRejectedError(
                f"unknown service_id '{service_id}'; not present in identities allow-list"
            )
        return service_id
