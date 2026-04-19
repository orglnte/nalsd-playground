"""
photoshare startup sequence.

This function *is* the infrastructure declaration. Editing it to add an
acquire() call and restarting the service causes platformd to provision
the new resource — no manifest, Terraform, or deploy pipeline involved.

The current bootstrap acquires transactional-store and object-store. An
earlier commit acquired only transactional-store; the upload endpoint was
absent by construction in that version.

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

_ENV_BASE_URL = "PLATFORMD_URL"
_DEFAULT_BASE_URL = "http://127.0.0.1:8443"


def _base_url() -> str:
    return os.environ.get(_ENV_BASE_URL, _DEFAULT_BASE_URL)


def bootstrap() -> tuple[Client, Credentials, Credentials | None]:
    """
    Run the acquire phase against platformd, then drop privileges.

    Returns (client, db_credentials, store_credentials).
    store_credentials is None when the bootstrap doesn't acquire object-store.
    """
    log.info("photoshare bootstrap: ACQUIRING phase begin")
    platform = Client("photoshare", base_url=_base_url())
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

    # --- object-store acquire ---
    # Adding this acquire() call *is* the infrastructure change. No YAML
    # beyond the daemon's scope file (which already permits object-store
    # for photoshare), no Terraform, no deploy pipeline. Restart the
    # service and platformd provisions a new rustfs container mid-lifecycle;
    # main.py then sees `store is not None` and registers the upload
    # endpoints that were absent before this acquire was added.
    store = platform.acquire(BlockType.OBJECT_STORE, name="photos")
    log.info(
        "bootstrap: object-store 'photos' acquired at %s:%d",
        store.host,
        store.port,
    )
    # --- end object-store acquire ---

    platform.drop_to_scaling_only()
    log.info("bootstrap: privileges dropped, now OPERATIONAL")
    return platform, db, store
