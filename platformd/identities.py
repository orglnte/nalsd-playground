from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass
from pathlib import Path

# service_id is composed into filesystem paths by ScopeStore and the
# recording-output path, so it must be a safe bare filename. Anything
# with a separator, parent-dir traversal, or a leading dot is rejected
# at identity-load time — the daemon never sees an unsafe service_id.
_VALID_SERVICE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")


class UnknownPeerError(Exception):
    """Raised when a connecting UID has no identity mapping."""


def _validate_service_id(service_id: object, *, where: str) -> str:
    if not isinstance(service_id, str):
        raise ValueError(f"{where}: service_id must be a string")
    if not _VALID_SERVICE_ID.fullmatch(service_id):
        raise ValueError(
            f"{where}: service_id '{service_id}' is not a safe identifier "
            "(allowed: [A-Za-z0-9][A-Za-z0-9_-]{0,63})"
        )
    return service_id


@dataclass(frozen=True)
class Identities:
    by_uid: dict[int, str]

    def service_for_uid(self, uid: int) -> str:
        try:
            return self.by_uid[uid]
        except KeyError as e:
            raise UnknownPeerError(f"no identity mapping for peer uid={uid}") from e


def load_identities(path: Path) -> Identities:
    if not path.is_file():
        raise FileNotFoundError(f"identities file not found: {path}")
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"malformed identities file {path}: {e}") from e

    entries = data.get("identities")
    if not isinstance(entries, list) or not entries:
        raise ValueError(f"identities file {path} must contain a non-empty [[identities]] array")

    by_uid: dict[int, str] = {}
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict) or "uid" not in entry or "service_id" not in entry:
            raise ValueError(f"identities file {path}: entry {i} missing 'uid' or 'service_id'")
        uid = entry["uid"]
        service_id = _validate_service_id(
            entry["service_id"], where=f"identities file {path}: entry {i}"
        )
        if not isinstance(uid, int):
            raise ValueError(f"identities file {path}: entry {i} uid must be an integer")
        if uid in by_uid:
            raise ValueError(f"identities file {path}: duplicate uid {uid}")
        by_uid[uid] = service_id
    return Identities(by_uid=by_uid)
