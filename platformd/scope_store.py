from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from platform_api.types import ServiceScope
from platformd.scope_loader import load_scope


class ScopeNotFoundError(Exception):
    """Raised when a service has no scope file in the store."""


@dataclass(frozen=True)
class ScopeStore:
    """
    Reads scope files from a directory. Scope for a service is looked up
    exclusively by authenticated service_id — the client cannot influence
    which scope applies to it.
    """

    scope_dir: Path

    def get(self, service_id: str) -> ServiceScope:
        path = self.scope_dir / f"{service_id}.toml"
        if not path.is_file():
            raise ScopeNotFoundError(
                f"no scope file for service '{service_id}' in {self.scope_dir}"
            )
        scope = load_scope(path)
        if scope.service_id != service_id:
            raise ValueError(
                f"scope file {path} declares service_id='{scope.service_id}', "
                f"expected '{service_id}' (file name must match service_id)"
            )
        return scope
