"""
photoshare FastAPI app.

The app's feature surface is a strict function of which building blocks the
bootstrap phase acquired. If object-store credentials are absent, the upload
and retrieve endpoints are not registered at all — the code itself reflects
what infrastructure exists.
"""

from __future__ import annotations

import io
import logging
import random
import uuid
from typing import Any

from fastapi import FastAPI, File, HTTPException, Query, Response, UploadFile
from minio import Minio
from minio.error import S3Error
from psycopg_pool import ConnectionPool

from photoshare.bootstrap import bootstrap
from platform_api import Client, Credentials

log = logging.getLogger("photoshare.main")


def _make_pool(creds: Credentials) -> ConnectionPool:
    return ConnectionPool(
        creds.as_dsn(),
        min_size=2,
        max_size=10,
        kwargs={"connect_timeout": 5},
    )


def _ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        log.info("created object-store bucket %s", bucket)


def create_app(
    platform: Client,
    db: Credentials,
    store: Credentials | None,
) -> FastAPI:
    app = FastAPI(title="photoshare", version="0.1.0")
    app.state.platform = platform
    app.state.db = db
    app.state.store = store

    pool = _make_pool(db)

    minio_client: Minio | None = None
    bucket_name: str | None = None
    if store is not None:
        minio_client = Minio(
            f"{store.host}:{store.port}",
            access_key=store.username,
            secret_key=store.password,
            secure=False,
        )
        bucket_name = store.extras.get("bucket", store.name)
        _ensure_bucket(minio_client, bucket_name)

    app.state.minio = minio_client
    app.state.bucket = bucket_name

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
        assert minio_client is not None
        assert bucket_name is not None

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
        async def upload_photo(file: UploadFile | None = _UPLOAD_FILE_DEFAULT) -> dict[str, Any]:
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
            minio_client.put_object(
                bucket_name,
                photo_id,
                io.BytesIO(data),
                length=size,
                content_type=content_type,
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
        def fetch_photo(photo_id: str) -> Response:
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
                obj = minio_client.get_object(bucket_name, photo_id)
            except S3Error as e:
                raise HTTPException(status_code=500, detail=f"object-store error: {e.code}") from e
            try:
                data = obj.read()
            finally:
                obj.close()
                obj.release_conn()
            return Response(content=data, media_type=content_type)

    else:

        @app.post("/photos", status_code=503)
        def upload_disabled() -> dict[str, Any]:
            raise HTTPException(
                status_code=503,
                detail=(
                    "upload unavailable: object-store not acquired in this "
                    "version of the service — edit photoshare/bootstrap.py to "
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
