"""
photoshare-demo FastAPI app.

The app's feature surface is a strict function of which building blocks the
bootstrap phase acquired. If object-store credentials are absent, the upload
and retrieve endpoints are not registered at all — the code itself reflects
what infrastructure exists.
"""

from __future__ import annotations

import logging
import random
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi import FastAPI, File, HTTPException, Query, Request, Response, UploadFile
from psycopg_pool import ConnectionPool

from photoshare_demo.bootstrap import bootstrap
from platform_api import Client, Credentials

log = logging.getLogger("photoshare_demo.main")


# Reserve a small headroom below postgres max_connections for admin /
# health-check traffic so the app pool cannot starve operator access.
_CONNECTION_HEADROOM = 2


def plan_pool(creds: Credentials) -> tuple[int, int]:
    """Derive (min_size, max_size) for a psycopg pool from platform hints.

    Reads `max_connections` from Credentials.extras and reserves a small
    headroom for admin/health traffic. Single-worker sizing today; a
    multi-worker deployment would divide the result by worker count.
    """
    max_connections = int(creds.extras.get("max_connections", 20))
    pool_max = max(1, max_connections - _CONNECTION_HEADROOM)
    pool_min = min(2, pool_max)
    return pool_min, pool_max


def _make_pool(creds: Credentials) -> ConnectionPool:
    pool_min, pool_max = plan_pool(creds)
    log.info(
        "pool sizing: max_connections=%s pool_min=%d pool_max=%d",
        creds.extras.get("max_connections", "<default>"),
        pool_min,
        pool_max,
    )
    return ConnectionPool(
        creds.as_dsn(),
        min_size=pool_min,
        max_size=pool_max,
        kwargs={"connect_timeout": 5},
    )


async def _ensure_bucket(s3: Any, bucket: str) -> None:
    try:
        await s3.head_bucket(Bucket=bucket)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchBucket"):
            await s3.create_bucket(Bucket=bucket)
            log.info("created object-store bucket %s", bucket)
        else:
            raise


def create_app(
    platform: Client,
    db: Credentials,
    store: Credentials | None,
) -> FastAPI:
    pool = _make_pool(db)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if store is None:
            app.state.s3 = None
            app.state.bucket = None
            yield
            return
        session = aioboto3.Session()
        async with session.client(
            "s3",
            endpoint_url=f"http://{store.host}:{store.port}",
            aws_access_key_id=store.username,
            aws_secret_access_key=store.password,
            region_name="us-east-1",
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
            ),
        ) as s3:
            bucket_name = store.extras.get("bucket", store.name)
            await _ensure_bucket(s3, bucket_name)
            app.state.s3 = s3
            app.state.bucket = bucket_name
            yield

    app = FastAPI(title="photoshare-demo", version="0.1.0", lifespan=lifespan)
    app.state.platform = platform
    app.state.db = db
    app.state.store = store

    @app.get("/health")
    def health() -> dict[str, Any]:
        blocks = {"transactional-store": db.name}
        if store is not None:
            blocks["object-store"] = store.name
        return {
            "service": "photoshare",
            "acquired_blocks": blocks,
        }

    @app.get("/photos/search")
    def search_photos(q: str = Query(default="")) -> dict[str, Any]:
        if not q:
            q = random.choice(["sunset", "beach", "mountain", "city", "forest"])
        with pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT id, title, filename, uploaded_at FROM photos "
                "WHERE to_tsvector('english', title) @@ plainto_tsquery('english', %s) "
                "ORDER BY uploaded_at DESC LIMIT 20",
                (q,),
            )
            rows = cur.fetchall()
        return {
            "query": q,
            "count": len(rows),
            "photos": [{"id": r[0], "title": r[1], "filename": r[2]} for r in rows],
        }

    @app.get("/photos")
    def list_photos(page: int = Query(default=0, ge=0)) -> dict[str, Any]:
        limit = 20
        offset = page * limit
        with pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT id, title, filename, size_bytes, uploaded_at "
                "FROM photos ORDER BY uploaded_at DESC LIMIT %s OFFSET %s",
                (limit, offset),
            )
            rows = cur.fetchall()
        return {
            "page": page,
            "count": len(rows),
            "photos": [
                {
                    "id": row[0],
                    "title": row[1],
                    "filename": row[2],
                    "size_bytes": row[3],
                    "uploaded_at": row[4].isoformat(),
                }
                for row in rows
            ],
        }

    if store is not None:
        _WORDS = [
            "sunset",
            "beach",
            "mountain",
            "city",
            "forest",
            "river",
            "snow",
            "garden",
            "portrait",
            "street",
            "night",
            "morning",
        ]

        def _random_title() -> str:
            return " ".join(random.choices(_WORDS, k=random.randint(2, 4)))

        _UPLOAD_FILE_DEFAULT = File(None)

        @app.post("/photos", status_code=201)
        async def upload_photo(
            request: Request,
            file: UploadFile | None = _UPLOAD_FILE_DEFAULT,
        ) -> dict[str, Any]:
            photo_id = uuid.uuid4().hex
            title = _random_title()

            if file is not None:
                data = await file.read()
                content_type = file.content_type or "application/octet-stream"
                filename = file.filename or photo_id
            else:
                # Simple mode for load testing: 1 KB synthetic payload
                data = b"x" * 1000
                content_type = "application/octet-stream"
                filename = f"{photo_id}.bin"

            size = len(data)
            await request.app.state.s3.put_object(
                Bucket=request.app.state.bucket,
                Key=photo_id,
                Body=data,
                ContentType=content_type,
            )
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO photos (id, filename, content_type, "
                        "size_bytes, title) VALUES (%s, %s, %s, %s, %s)",
                        (photo_id, filename, content_type, size, title),
                    )
                conn.commit()
            return {
                "id": photo_id,
                "title": title,
                "filename": filename,
                "size_bytes": size,
            }

        @app.get("/photos/{photo_id}")
        async def fetch_photo(request: Request, photo_id: str) -> Response:
            with pool.connection() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT content_type FROM photos WHERE id = %s",
                    (photo_id,),
                )
                row = cur.fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="photo not found")
            content_type = row[0]
            try:
                resp = await request.app.state.s3.get_object(
                    Bucket=request.app.state.bucket,
                    Key=photo_id,
                )
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "unknown")
                raise HTTPException(
                    status_code=500, detail=f"object-store error: {code}"
                ) from e
            async with resp["Body"] as stream:
                data = await stream.read()
            return Response(content=data, media_type=content_type)

    else:

        @app.post("/photos", status_code=503)
        def upload_disabled() -> dict[str, Any]:
            raise HTTPException(
                status_code=503,
                detail=(
                    "upload unavailable: object-store not acquired in this "
                    "version of the service — edit photoshare_demo/bootstrap.py to "
                    "enable"
                ),
            )

    return app


def build() -> FastAPI:
    """Entry point for uvicorn factory mode."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    platform, db, store = bootstrap()
    return create_app(platform, db, store)
