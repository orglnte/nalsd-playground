from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class DaemonConfig:
    listen_host: str
    listen_port: int
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

    missing = {"listen_address", "scope_dir", "identities_path"} - data.keys()
    if missing:
        raise ValueError(f"daemon config {path} missing required keys: {sorted(missing)}")

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

    host, port = _parse_listen_address(data["listen_address"])
    return DaemonConfig(
        listen_host=host,
        listen_port=port,
        scope_dir=_resolve(base, data["scope_dir"]),
        identities_path=_resolve(base, data["identities_path"]),
        service_modes=modes,
    )


def _parse_listen_address(value: object) -> tuple[str, int]:
    if not isinstance(value, str) or ":" not in value:
        raise ValueError(f"listen_address must be 'host:port', got {value!r}")
    host, _, port_s = value.rpartition(":")
    if not host:
        raise ValueError(f"listen_address must include a host, got {value!r}")
    try:
        port = int(port_s)
    except ValueError as e:
        raise ValueError(f"listen_address port must be an integer, got {port_s!r}") from e
    if not 0 <= port < 65536:
        raise ValueError(f"listen_address port out of range: {port}")
    return host, port


def _resolve(base: Path, value: str) -> Path:
    p = Path(value)
    return p if p.is_absolute() else (base / p).resolve()
