"""
Photo-app service for load testing.

Simulates a photoshare app backed by postgres (metadata) + RustFS (S3 blobs).

Endpoints:
  POST /photos          — upload: insert metadata + put S3 object
  GET  /photos/{id}     — view: read metadata + fetch S3 object
  GET  /photos          — list: paginated recent photos (metadata only)
  GET  /photos/search   — search: LIKE query on title (metadata only)

  Legacy (kept for backwards compat with load-test-e2e):
  POST /db, GET /db, POST /store, GET /store
"""
from __future__ import annotations

import os
import random
import string
from contextlib import asynccontextmanager
from typing import Any

import psycopg
from granian import Granian
from aiobotocore.session import AioSession
from fastapi import FastAPI, Query, Response

HOST = os.environ.get("BACKEND_HOST", "127.0.0.1")

PG_DSN = (
    f"postgresql://platform:platform-local-password"
    f"@{HOST}:15432/metadata"
)
S3_ENDPOINT = f"http://{HOST}:19000"
S3_KEY = "platform"
S3_SECRET = "platform-local-password"
S3_BUCKET = "photos"
S3_REGION = "us-east-1"

IMAGE_PAYLOAD = b"x" * 1000  # 1 KB simulated image

WORDS = [
    "sunset", "beach", "mountain", "city", "forest", "river", "snow",
    "garden", "portrait", "street", "night", "morning", "autumn", "spring",
    "lake", "bridge", "tower", "market", "cafe", "rain", "cloud", "sky",
    "flower", "dog", "cat", "bird", "travel", "food", "party", "family",
]

_pg: psycopg.AsyncConnection | None = None
_s3_session: AioSession | None = None
_s3: Any = None
_s3_ctx: Any = None
_max_id: int = 0


async def _get_s3():
    """Get or recreate S3 client on connection failure."""
    global _s3, _s3_ctx
    if _s3 is not None:
        return _s3
    if _s3_ctx is not None:
        try:
            await _s3_ctx.__aexit__(None, None, None)
        except Exception:
            pass
    _s3_ctx = _s3_session.create_client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
        region_name=S3_REGION,
    )
    _s3 = await _s3_ctx.__aenter__()
    return _s3


async def _s3_call(fn):
    """Execute an S3 operation, recreating the client on connection errors."""
    global _s3
    try:
        return await fn(await _get_s3())
    except Exception:
        _s3 = None  # force reconnect on next call
        raise


def _random_title() -> str:
    return " ".join(random.choices(WORDS, k=random.randint(2, 4)))


def _random_key() -> str:
    return "photo-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=16))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pg, _s3, _s3_ctx

    global _s3_session

    _pg = await psycopg.AsyncConnection.connect(PG_DSN, autocommit=True)
    async with _pg.cursor() as cur:
        await cur.execute("DROP TABLE IF EXISTS bench_photos")
        await cur.execute(
            "CREATE TABLE bench_photos ("
            "  id SERIAL PRIMARY KEY,"
            "  title TEXT NOT NULL,"
            "  description TEXT,"
            "  s3_key TEXT NOT NULL,"
            "  created_at TIMESTAMPTZ DEFAULT now()"
            ")"
        )
        await cur.execute(
            "CREATE INDEX idx_bp_title "
            "ON bench_photos USING gin (to_tsvector('english', title))"
        )
        await cur.execute(
            "CREATE INDEX idx_bp_created "
            "ON bench_photos (created_at DESC)"
        )

    _s3_session = AioSession()
    s3 = await _get_s3()

    try:
        await s3.create_bucket(Bucket=S3_BUCKET)
    except Exception:
        pass

    yield

    if _s3_ctx is not None:
        try:
            await _s3_ctx.__aexit__(None, None, None)
        except Exception:
            pass
    if _pg and not _pg.closed:
        await _pg.close()


app = FastAPI(lifespan=lifespan)


# ── Photo endpoints ──────────────────────────────────────────────────

@app.post("/photos")
async def upload_photo() -> Response:
    """Upload: insert metadata row + put S3 object."""
    global _max_id
    title = _random_title()
    s3_key = _random_key()

    try:
        await _s3_call(lambda s3: s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=IMAGE_PAYLOAD))
    except Exception:
        return Response(content=b"s3 write failed", status_code=502)

    async with _pg.cursor() as cur:
        await cur.execute(
            "INSERT INTO bench_photos (title, description, s3_key) "
            "VALUES (%s, %s, %s) RETURNING id",
            (title, f"A photo of {title}", s3_key),
        )
        row = await cur.fetchone()
        photo_id = row[0]

    if photo_id > _max_id:
        _max_id = photo_id
    return {"id": photo_id, "key": s3_key}


@app.get("/photos/search")
async def search_photos(q: str = Query(default="")) -> dict:
    """Search: full-text search on title."""
    if not q:
        q = random.choice(WORDS)
    async with _pg.cursor() as cur:
        await cur.execute(
            "SELECT id, title, s3_key, created_at FROM bench_photos "
            "WHERE to_tsvector('english', title) @@ plainto_tsquery('english', %s) "
            "ORDER BY created_at DESC LIMIT 20",
            (q,),
        )
        rows = await cur.fetchall()
    return {
        "query": q,
        "count": len(rows),
        "photos": [{"id": r[0], "title": r[1]} for r in rows],
    }


@app.get("/photos/{photo_id}")
async def view_photo(photo_id: int) -> Response:
    """View: read metadata + fetch S3 blob."""
    async with _pg.cursor() as cur:
        await cur.execute(
            "SELECT id, title, s3_key FROM bench_photos WHERE id = %s",
            (photo_id,),
        )
        row = await cur.fetchone()

    if not row:
        return Response(content=b"not found", status_code=404)

    s3_key = row[2]
    try:
        resp = await _s3_call(lambda s3: s3.get_object(Bucket=S3_BUCKET, Key=s3_key))
        await resp["Body"].read()
    except Exception:
        return Response(content=b"s3 read failed", status_code=502)

    return {"id": row[0], "title": row[1], "key": s3_key}


@app.get("/photos")
async def list_photos(page: int = Query(default=0, ge=0)) -> dict:
    """List: paginated recent photos (metadata only)."""
    limit = 20
    offset = page * limit
    async with _pg.cursor() as cur:
        await cur.execute(
            "SELECT id, title, created_at FROM bench_photos "
            "ORDER BY created_at DESC LIMIT %s OFFSET %s",
            (limit, offset),
        )
        rows = await cur.fetchall()
    return {
        "page": page,
        "count": len(rows),
        "photos": [{"id": r[0], "title": r[1]} for r in rows],
    }


# ── Legacy endpoints (load-test-e2e compat) ──────────────────────────

@app.post("/db")
async def db_write() -> dict:
    global _max_id
    async with _pg.cursor() as cur:
        await cur.execute(
            "INSERT INTO bench_photos (title, description, s3_key) "
            "VALUES (%s, %s, %s) RETURNING id",
            (_random_title(), "legacy", "none"),
        )
        row = await cur.fetchone()
        row_id = row[0]
    if row_id > _max_id:
        _max_id = row_id
    return {"id": row_id}


@app.get("/db")
async def db_read() -> dict:
    target = max(_max_id, 1)
    row_id = random.randint(1, target)
    async with _pg.cursor() as cur:
        await cur.execute("SELECT id, title FROM bench_photos WHERE id = %s", (row_id,))
        row = await cur.fetchone()
    return {"id": row[0] if row else None}


@app.post("/store")
async def store_write() -> Response:
    key = _random_key()
    try:
        await _s3_call(lambda s3: s3.put_object(Bucket=S3_BUCKET, Key=key, Body=IMAGE_PAYLOAD))
    except Exception:
        return Response(content=b"s3 write failed", status_code=502)
    return {"key": key}


@app.get("/store")
async def store_read() -> Response:
    if _max_id < 1:
        return Response(content=b"no photos yet", status_code=404)
    row_id = random.randint(1, _max_id)
    async with _pg.cursor() as cur:
        await cur.execute("SELECT s3_key FROM bench_photos WHERE id = %s", (row_id,))
        row = await cur.fetchone()
    if not row or row[0] == "none":
        return Response(content=b"no s3 key", status_code=404)
    try:
        resp = await _s3_call(lambda s3: s3.get_object(Bucket=S3_BUCKET, Key=row[0]))
        data = await resp["Body"].read()
    except Exception:
        return Response(content=b"s3 read failed", status_code=502)
    return Response(content=data, media_type="application/octet-stream")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8090, log_level="error")
