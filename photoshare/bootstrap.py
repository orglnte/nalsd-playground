"""
photoshare startup sequence.

This function *is* the infrastructure declaration. Editing it to add an
acquire() call and restarting the service causes platformd to provision
the new resource — no manifest, Terraform, or deploy pipeline involved.

v1  : transactional-store only. Upload endpoint is absent by construction.
v1.1: additionally acquires object-store. Upload endpoint becomes available.

Scope is owned by platformd (dev-config/scopes/photoshare.toml). The
service no longer constructs a scope locally; it connects to the daemon
and the daemon looks up the scope by authenticated service_id.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from platform_api import BlockType, Client, Credentials, apply_manifesto

log = logging.getLogger("photoshare.bootstrap")

MIGRATIONS_DIR = Path(__file__).parent / "migrations"

_ENV_SOCKET = "PLATFORMD_SOCKET"
_DEFAULT_SOCKET = Path("dev-config/run/platformd.sock")


def _socket_path() -> Path:
    override = os.environ.get(_ENV_SOCKET)
    return Path(override) if override else _DEFAULT_SOCKET


def bootstrap() -> tuple[Client, Credentials, Credentials | None]:
    """
    Run the acquire phase against platformd, then drop privileges.

    Returns (client, db_credentials, store_credentials).
    store_credentials is None in v1.
    """
    log.info("photoshare bootstrap: ACQUIRING phase begin")
    platform = Client("photoshare", socket_path=_socket_path())
    platform.connect()

    db = platform.acquire(
        BlockType.TRANSACTIONAL_STORE,
        name="metadata",
        database="metadata",
    )
    log.info(
        "bootstrap: transactional-store 'metadata' acquired at %s:%d",
        db.host,
        db.port,
    )

    applied = apply_manifesto(db, MIGRATIONS_DIR)
    log.info("bootstrap: manifesto applied=%s", applied)

    # --- v1.1 insertion point ---
    # Adding this acquire() call *is* the infrastructure change. No YAML
    # beyond the daemon's scope file (which already permits object-store
    # for photoshare), no Terraform, no deploy pipeline. Restart the
    # service and platformd provisions a new MinIO container mid-lifecycle;
    # main.py then sees `store is not None` and registers the upload
    # endpoints that were absent in v1.
    store = platform.acquire(BlockType.OBJECT_STORE, name="photos")
    log.info(
        "bootstrap: object-store 'photos' acquired at %s:%d",
        store.host,
        store.port,
    )
    # --- end v1.1 insertion point ---

    platform.drop_to_scaling_only()
    log.info("bootstrap: privileges dropped, now OPERATIONAL")
    return platform, db, store
