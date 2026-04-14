"""
Versioned manifesto runner.

A manifesto is a directory of SQL files named `vNNN_<description>.sql`. The
runner applies them in order against a freshly-provisioned transactional-store
and records applied versions in the __platform_manifesto__ table inside the
database itself. Idempotent — re-running against an already-migrated database
is a no-op.

This is the simplest possible form of the "versioned startup provisioning
manifesto" concept. Production would extend it with rollback semantics,
online vs. offline migration classification, and platform-enforced
compatibility checks.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import psycopg

from platform_api.types import Credentials

log = logging.getLogger("platform_api.manifesto")

_MANIFESTO_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS __platform_manifesto__ (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

_VERSION_RE = re.compile(r"^(v\d+)(?:_.*)?\.sql$")


def apply_manifesto(credentials: Credentials, manifesto_dir: Path) -> list[str]:
    """
    Apply all pending SQL files from manifesto_dir in version order.
    Returns the list of versions applied on this invocation.
    """
    if not manifesto_dir.is_dir():
        raise FileNotFoundError(f"manifesto dir not found: {manifesto_dir}")

    files = sorted(
        (p for p in manifesto_dir.iterdir() if _VERSION_RE.match(p.name)),
        key=lambda p: p.name,
    )
    if not files:
        log.info("manifesto: no version files in %s", manifesto_dir)
        return []

    applied: list[str] = []
    with psycopg.connect(credentials.as_dsn(), autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(_MANIFESTO_TABLE_DDL)
            cur.execute("SELECT version FROM __platform_manifesto__")
            already = {row[0] for row in cur.fetchall()}
        conn.commit()

        for path in files:
            match = _VERSION_RE.match(path.name)
            if match is None:
                raise ValueError(f"manifesto: filename {path.name!r} does not match vNNN_ pattern")
            version = match.group(1)
            if version in already:
                log.info("manifesto: skipping %s (already applied)", version)
                continue
            sql = path.read_text()
            log.info("manifesto: applying %s from %s", version, path.name)
            with conn.cursor() as cur:
                cur.execute(sql)
                cur.execute(
                    "INSERT INTO __platform_manifesto__ (version) VALUES (%s)",
                    (version,),
                )
            conn.commit()
            applied.append(version)

    log.info(
        "manifesto: complete, applied=%d skipped=%d",
        len(applied),
        len(files) - len(applied),
    )
    return applied
