"""
photoshare startup sequence.

This function *is* the infrastructure declaration. Editing it to add an
acquire() call and restarting the service causes the Platform API to
provision the new resource — no manifest, Terraform, or deploy pipeline
involved. That is the load-bearing demonstration of the runtime IfC model.

v1  : transactional-store only. Upload endpoint is absent by construction.
v1.1: additionally acquires object-store. Upload endpoint becomes available.
"""

from __future__ import annotations

import logging
from pathlib import Path

from platform_api import (
    BlockType,
    CapabilityManifest,
    Credentials,
    PlatformClient,
    apply_manifesto,
)

log = logging.getLogger("photoshare.bootstrap")

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def _build_manifest() -> CapabilityManifest:
    return CapabilityManifest(
        service_id="photoshare",
        allowed_blocks={
            BlockType.TRANSACTIONAL_STORE,
            BlockType.OBJECT_STORE,
        },
        max_blocks=4,
    )


def bootstrap() -> tuple[PlatformClient, Credentials, Credentials | None]:
    """
    Run the acquire phase, then drop privileges.

    Returns (platform_client, db_credentials, store_credentials).
    store_credentials is None in v1.
    """
    log.info("photoshare bootstrap: ACQUIRING phase begin")
    platform = PlatformClient(
        service_id="photoshare", manifest=_build_manifest()
    )

    db = platform.acquire(
        BlockType.TRANSACTIONAL_STORE,
        name="photos",
        database="photos",
    )
    log.info("bootstrap: transactional-store 'photos' acquired at %s:%d", db.host, db.port)

    applied = apply_manifesto(db, MIGRATIONS_DIR)
    log.info("bootstrap: manifesto applied=%s", applied)

    # --- v1.1 insertion point ---
    # This acquire() call *is* the infrastructure change. No YAML, no
    # Terraform, no deploy pipe. Restart the service and Pulumi provisions
    # a new MinIO container mid-lifecycle; main.py then sees `store is not
    # None` and registers the upload endpoints that were absent in v1.
    store: Credentials | None = platform.acquire(
        BlockType.OBJECT_STORE, name="images"
    )
    log.info(
        "bootstrap: object-store 'images' acquired at %s:%d",
        store.host,
        store.port,
    )
    # --- end v1.1 insertion point ---

    platform.drop_to_scaling_only()
    log.info("bootstrap: privileges dropped, now OPERATIONAL")
    return platform, db, store
