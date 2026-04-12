"""
Minimal FastAPI service with two endpoints for load testing:
  POST /db    — insert a 300-byte row into postgres, return the id
  GET  /db    — read a random row
  POST /store — put a 300-byte object into Garage (S3), return the key
  GET  /store — get a random object

All I/O is async: psycopg AsyncConnection for postgres, aiobotocore for S3.
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
from fastapi import FastAPI, Response

HOST = os.environ.get("BACKEND_HOST", "127.0.0.1")

PG_DSN = (
    f"postgresql://platform:platform-local-password"
    f"@{HOST}:15432/appdb"
)
S3_ENDPOINT = f"http://{HOST}:19000"
S3_KEY = "anonymous"
S3_SECRET = "anonymous"
S3_BUCKET = "store"
S3_REGION = "us-east-1"

PAYLOAD = b"x" * 300

_pg: psycopg.AsyncConnection | None = None
_s3: Any = None
_s3_ctx: Any = None
_obj_keys: list[str] = []
_max_id: int = 0


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pg, _s3, _s3_ctx

    # Async postgres
    _pg = await psycopg.AsyncConnection.connect(PG_DSN, autocommit=True)
    async with _pg.cursor() as cur:
        await cur.execute(
            "CREATE TABLE IF NOT EXISTS bench ("
            "  id SERIAL PRIMARY KEY,"
            "  payload TEXT NOT NULL"
            ")"
        )

    # Async S3
    session = AioSession()
    _s3_ctx = session.create_client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
        region_name=S3_REGION,
    )
    _s3 = await _s3_ctx.__aenter__()

    # Ensure bucket exists
    try:
        await _s3.create_bucket(Bucket=S3_BUCKET)
    except _s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    except Exception:
        pass  # bucket may already exist

    yield

    await _s3_ctx.__aexit__(None, None, None)
    if _pg and not _pg.closed:
        await _pg.close()


app = FastAPI(lifespan=lifespan)


# ── Postgres (async) ─────────────────────────────────────────────────

@app.post("/db")
async def db_write() -> dict:
    global _max_id
    async with _pg.cursor() as cur:
        await cur.execute(
            "INSERT INTO bench (payload) VALUES (%s) RETURNING id",
            (PAYLOAD.decode(),),
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
        await cur.execute("SELECT id, payload FROM bench WHERE id = %s", (row_id,))
        row = await cur.fetchone()
    return {"id": row[0] if row else None}


# ── Garage S3 (async via aiobotocore) ────────────────────────────────

@app.post("/store")
async def store_write() -> dict:
    key = "obj-" + "".join(random.choices(string.ascii_lowercase, k=12))
    await _s3.put_object(Bucket=S3_BUCKET, Key=key, Body=PAYLOAD)
    _obj_keys.append(key)
    return {"key": key}


@app.get("/store")
async def store_read() -> Response:
    if not _obj_keys:
        return Response(content=b"no objects yet", status_code=404)
    key = random.choice(_obj_keys)
    resp = await _s3.get_object(Bucket=S3_BUCKET, Key=key)
    data = await resp["Body"].read()
    return Response(content=data, media_type="application/octet-stream")


if __name__ == "__main__":
    Granian(
        "bench_service:app",
        address="127.0.0.1",
        port=8090,
        interface="asgi",
        log_level="error",
    ).serve()
