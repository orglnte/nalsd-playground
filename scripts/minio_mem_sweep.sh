#!/usr/bin/env bash
# Sweep MinIO memory limits to find the threshold for 1000 rps @ 30s.
# Reuses existing postgres/redis containers, only restarts minio.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
LOAD_BIN="$REPO/scripts/load-test/target/release/load-test"

for MEM in 96 146 196 246; do
    echo ""
    echo "================================================================"
    echo "  MinIO memory limit: ${MEM} MB"
    echo "================================================================"

    # Stop and recreate just the minio container with new memory limit
    docker rm -f minio-sweep 2>/dev/null || true
    docker run -d --name minio-sweep \
        -m "${MEM}m" --memory-swap "${MEM}m" \
        --tmpfs "/data:rw,noexec,nosuid,size=${MEM}m" \
        -e MINIO_ROOT_USER=platform \
        -e MINIO_ROOT_PASSWORD=platform-local-password \
        -p 19000:9000 \
        minio/minio:latest server /data --quiet >/dev/null

    # Wait for health
    for i in $(seq 1 20); do
        STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:19000/minio/health/live 2>/dev/null || echo "000")
        [ "$STATUS" = "200" ] && break
        sleep 0.5
    done

    # Run load test (only minio matters, but pg/redis need to be up too)
    # We'll just test minio standalone with a small wrapper
    # Actually the load binary hits all three — let's just skip pg/redis
    # and run minio-only. But the binary always runs all three.
    # Simplest: just look at the minio line in output.

    echo "  Running 1000 rps × 30s ..."
    docker rm -f nalsd-measure-db nalsd-measure-store nalsd-measure-cache 2>/dev/null || true

    # Create pg and redis for the load binary (reuse same names)
    docker run -d --name nalsd-measure-cache \
        -m 32m --memory-swap 32m \
        -p 16379:6379 \
        redis:7-alpine redis-server --maxmemory 16mb --maxmemory-policy allkeys-lru >/dev/null 2>&1 || true

    docker run -d --name nalsd-measure-db \
        -m 96m --memory-swap 96m \
        -e POSTGRES_USER=platform \
        -e POSTGRES_PASSWORD=platform-local-password \
        -e POSTGRES_DB=appdb \
        -p 15432:5432 \
        postgres:16-alpine \
        postgres -c shared_buffers=16MB -c max_connections=20 -c fsync=off -c synchronous_commit=off -c full_page_writes=off >/dev/null 2>&1 || true

    # Rename minio container to what the load binary expects
    docker rm -f nalsd-measure-store 2>/dev/null || true
    docker rename minio-sweep nalsd-measure-store

    # Wait for pg readiness
    for i in $(seq 1 30); do
        pg_isready -h 127.0.0.1 -p 15432 -U platform -q 2>/dev/null && break
        sleep 0.5
    done
    sleep 1

    "$LOAD_BIN" --rps 1000 --duration 30 --concurrency 8 2>&1 | grep -E '(minio|TOTAL|Load results)'

    # Cleanup
    docker rm -f nalsd-measure-db nalsd-measure-store nalsd-measure-cache 2>/dev/null || true
done

echo ""
echo "Done."
