from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class DaemonConfig:
    socket_path: Path
    scope_dir: Path
    identities_path: Path
    service_modes: dict[str, str] = field(default_factory=dict)

    def mode_for(self, service_id: str) -> str:
        return self.service_modes.get(service_id, "enforce")


def load_daemon_config(path: Path) -> DaemonConfig:
    if not path.is_file():
        raise FileNotFoundError(f"daemon config not found: {path}")
    try:
        data = tomllib.loads(path.read_text())
    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"malformed daemon config {path}: {e}") from e

    missing = {"socket_path", "scope_dir", "identities_path"} - data.keys()
    if missing:
        raise ValueError(
            f"daemon config {path} missing required keys: {sorted(missing)}"
        )

    base = path.parent
    modes: dict[str, str] = {}
    for service_id, block in (data.get("service") or {}).items():
        mode = block.get("mode", "enforce")
        if mode not in {"enforce", "record"}:
            raise ValueError(
                f"daemon config {path}: service '{service_id}' has "
                f"invalid mode '{mode}' (must be 'enforce' or 'record')"
            )
        modes[service_id] = mode

    return DaemonConfig(
        socket_path=_resolve(base, data["socket_path"]),
        scope_dir=_resolve(base, data["scope_dir"]),
        identities_path=_resolve(base, data["identities_path"]),
        service_modes=modes,
    )


def _resolve(base: Path, value: str) -> Path:
    p = Path(value)
    return p if p.is_absolute() else (base / p).resolve()
