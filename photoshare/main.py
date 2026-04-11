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
import uuid
from contextlib import contextmanager
from typing import Iterator

import psycopg
from fastapi import FastAPI, File, HTTPException, Response, UploadFile
from minio import Minio
from minio.error import S3Error

from photoshare.bootstrap import bootstrap
from platform_api import Credentials, PlatformClient

log = logging.getLogger("photoshare.main")


@contextmanager
def _pg(creds: Credentials) -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(creds.as_dsn(), connect_timeout=5)
    try:
        yield conn
    finally:
        conn.close()


def _ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        log.info("created object-store bucket %s", bucket)


def create_app(
    platform: PlatformClient,
    db: Credentials,
    store: Credentials | None,
) -> FastAPI:
    app = FastAPI(title="photoshare", version="0.1.0")
    app.state.platform = platform
    app.state.db = db
    app.state.store = store

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
    def health() -> dict:
        blocks = {"transactional-store": db.name}
        if store is not None:
            blocks["object-store"] = store.name
        return {
            "service": "photoshare",
            "privilege_state": platform.state.value,
            "acquired_blocks": blocks,
        }

    @app.get("/photos")
    def list_photos() -> list[dict]:
        with _pg(db) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, filename, content_type, size_bytes, "
                    "uploaded_at FROM photos ORDER BY uploaded_at DESC"
                )
                rows = cur.fetchall()
        return [
            {
                "id": row[0],
                "filename": row[1],
                "content_type": row[2],
                "size_bytes": row[3],
                "uploaded_at": row[4].isoformat(),
            }
            for row in rows
        ]

    if store is not None:
        assert minio_client is not None and bucket_name is not None

        @app.post("/photos", status_code=201)
        async def upload_photo(file: UploadFile = File(...)) -> dict:
            data = await file.read()
            photo_id = uuid.uuid4().hex
            content_type = file.content_type or "application/octet-stream"
            size = len(data)
            minio_client.put_object(
                bucket_name,
                photo_id,
                io.BytesIO(data),
                length=size,
                content_type=content_type,
            )
            with _pg(db) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO photos (id, filename, content_type, "
                        "size_bytes) VALUES (%s, %s, %s, %s)",
                        (photo_id, file.filename or photo_id, content_type, size),
                    )
                conn.commit()
            return {
                "id": photo_id,
                "filename": file.filename,
                "size_bytes": size,
                "content_type": content_type,
            }

        @app.get("/photos/{photo_id}")
        def fetch_photo(photo_id: str) -> Response:
            with _pg(db) as conn:
                with conn.cursor() as cur:
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
                data = obj.read()
                obj.close()
                obj.release_conn()
            except S3Error as e:
                raise HTTPException(
                    status_code=500, detail=f"object-store error: {e.code}"
                ) from e
            return Response(content=data, media_type=content_type)

    else:

        @app.post("/photos", status_code=503)
        def upload_disabled() -> dict:
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
