"""
platformd — the Platform daemon.

Runs as a separate host process. Owns the engine, the scopes, and the
trust boundary. Services connect over a Unix domain socket and call
Acquire / DropToScalingOnly / ScaleHint / Shutdown via line-delimited
JSON-RPC. Identity is established by SO_PEERCRED (Linux) or getpeereid
(macOS), never by anything the client sends.
"""

from platformd.config import DaemonConfig, load_daemon_config
from platformd.identities import Identities, load_identities
from platformd.scope_store import ScopeStore
from platformd.session import Session

__all__ = [
    "DaemonConfig",
    "Identities",
    "ScopeStore",
    "Session",
    "load_daemon_config",
    "load_identities",
]
