#!/usr/bin/env python3
"""
Load-test provisioned containers at 100 rps per block, measure RSS before/after.

Expects the three containers from measure_rss.py to already be running,
or provisions them via a fresh daemon session.

Usage:
    .venv/bin/python scripts/load_test.py [--duration 30] [--rps 100]
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
VENV_PYTHON = REPO / ".venv" / "bin" / "python"
HOST = "127.0.0.1"

# Default ports from platformd/blocks.py
POSTGRES_PORT = 15432
MINIO_PORT = 19000
REDIS_PORT = 16379

PG_USER = "platform"
PG_PASS = "platform-local-password"
PG_DB = "appdb"
MINIO_USER = "platform"
MINIO_PASS = "platform-local-password"


def mb(bytes_val: int) -> str:
    return f"{bytes_val / 1024 / 1024:.1f} MB"


def get_container_mem(container_name: str) -> int | None:
    try:
        out = subprocess.check_output(
            ["docker", "stats", "--no-stream", "--format",
             "{{.MemUsage}}", container_name],
            stderr=subprocess.DEVNULL, timeout=10,
        )
        usage = out.decode().strip().split("/")[0].strip()
        if "GiB" in usage:
            return int(float(usage.replace("GiB", "")) * 1024 ** 3)
        if "MiB" in usage:
            return int(float(usage.replace("MiB", "")) * 1024 ** 2)
        if "KiB" in usage:
            return int(float(usage.replace("KiB", "")) * 1024)
        return None
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None


def get_process_rss(pid: int) -> int:
    if sys.platform == "darwin":
        out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)])
        return int(out.strip()) * 1024
    else:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    raise RuntimeError(f"cannot read RSS for pid {pid}")


def find_daemon_pid() -> int | None:
    """Find running platformd process."""
    try:
        out = subprocess.check_output(
            ["pgrep", "-f", "python.*-m platformd"],
            stderr=subprocess.DEVNULL,
        )
        pids = out.decode().strip().splitlines()
        return int(pids[0]) if pids else None
    except subprocess.CalledProcessError:
        return None


# ── Load generators ──────────────────────────────────────────────────

class RateLimiter:
    """Token-bucket rate limiter for target rps."""

    def __init__(self, rps: int):
        self._interval = 1.0 / rps
        self._next = time.monotonic()
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            if now < self._next:
                time.sleep(self._next - now)
            self._next = max(time.monotonic(), self._next) + self._interval


class LoadStats:
    def __init__(self):
        self.ok = 0
        self.err = 0
        self.latencies: list[float] = []
        self._lock = threading.Lock()

    def record(self, latency: float, error: bool = False) -> None:
        with self._lock:
            if error:
                self.err += 1
            else:
                self.ok += 1
                self.latencies.append(latency)

    def summary(self) -> dict:
        with self._lock:
            lats = sorted(self.latencies)
            n = len(lats)
            return {
                "ok": self.ok,
                "err": self.err,
                "p50_ms": lats[n // 2] * 1000 if n else 0,
                "p99_ms": lats[int(n * 0.99)] * 1000 if n else 0,
                "mean_ms": (sum(lats) / n) * 1000 if n else 0,
            }


def load_postgres(duration: float, rps: int) -> LoadStats:
    """Mix: 50% INSERT, 50% SELECT by PK."""
    import psycopg

    dsn = f"postgresql://{PG_USER}:{PG_PASS}@{HOST}:{POSTGRES_PORT}/{PG_DB}"
    conn = psycopg.connect(dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS loadtest ("
            "  id SERIAL PRIMARY KEY,"
            "  payload TEXT NOT NULL,"
            "  created_at TIMESTAMPTZ DEFAULT now()"
            ")"
        )

    stats = LoadStats()
    limiter = RateLimiter(rps)
    stop = time.monotonic() + duration
    row_id = 0

    while time.monotonic() < stop:
        limiter.wait()
        t0 = time.monotonic()
        try:
            with conn.cursor() as cur:
                if row_id % 2 == 0:
                    cur.execute(
                        "INSERT INTO loadtest (payload) VALUES (%s) RETURNING id",
                        (f"row-{row_id}",),
                    )
                    cur.fetchone()
                else:
                    cur.execute(
                        "SELECT * FROM loadtest WHERE id = %s",
                        (max(1, row_id // 2),),
                    )
                    cur.fetchone()
            stats.record(time.monotonic() - t0)
        except Exception:
            stats.record(time.monotonic() - t0, error=True)
        row_id += 1

    conn.close()
    return stats


def load_redis(duration: float, rps: int) -> LoadStats:
    """Mix: 50% SET, 50% GET."""
    import redis as redis_lib

    client = redis_lib.Redis(host=HOST, port=REDIS_PORT, socket_timeout=2)
    stats = LoadStats()
    limiter = RateLimiter(rps)
    stop = time.monotonic() + duration
    key_id = 0

    while time.monotonic() < stop:
        limiter.wait()
        t0 = time.monotonic()
        try:
            if key_id % 2 == 0:
                client.set(f"k:{key_id}", f"value-{key_id}" * 10)
            else:
                client.get(f"k:{key_id - 1}")
            stats.record(time.monotonic() - t0)
        except Exception:
            stats.record(time.monotonic() - t0, error=True)
        key_id += 1

    client.close()
    return stats


def load_minio(duration: float, rps: int) -> LoadStats:
    """Mix: 50% PUT, 50% GET on small objects (~1 KB)."""
    from minio import Minio
    import io

    client = Minio(
        f"{HOST}:{MINIO_PORT}",
        access_key=MINIO_USER,
        secret_key=MINIO_PASS,
        secure=False,
    )
    bucket = "loadtest"
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    stats = LoadStats()
    limiter = RateLimiter(rps)
    stop = time.monotonic() + duration
    obj_id = 0
    payload = b"x" * 1024  # 1 KB

    while time.monotonic() < stop:
        limiter.wait()
        t0 = time.monotonic()
        try:
            if obj_id % 2 == 0:
                client.put_object(
                    bucket, f"obj-{obj_id}", io.BytesIO(payload), len(payload),
                )
            else:
                resp = client.get_object(bucket, f"obj-{obj_id - 1}")
                resp.read()
                resp.close()
                resp.release_conn()
            stats.record(time.monotonic() - t0)
        except Exception:
            stats.record(time.monotonic() - t0, error=True)
        obj_id += 1

    return stats


# ── Orchestration ────────────────────────────────────────────────────

def write_config(tmpdir: Path) -> Path:
    scope_dir = tmpdir / "scopes"
    scope_dir.mkdir()
    (scope_dir / "measure.toml").write_text(
        'service_id = "measure"\n'
        'allowed_blocks = ["transactional-store", "object-store", "ephemeral-kv-cache"]\n'
        "max_blocks = 8\n"
    )
    uid = os.getuid()
    ident_path = tmpdir / "identities.toml"
    ident_path.write_text(
        f"[[identities]]\nuid = {uid}\n"
        f'service_id = "measure"\n'
    )
    sock_path = tmpdir / "platformd.sock"
    config_path = tmpdir / "platformd.toml"
    config_path.write_text(
        f'socket_path = "{sock_path}"\n'
        f'scope_dir = "{scope_dir}"\n'
        f'identities_path = "{ident_path}"\n'
        "\n[service.measure]\n"
        'mode = "enforce"\n'
    )
    return config_path


def provision_blocks(sock_path: Path) -> None:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(str(sock_path))
    f = s.makefile("rwb", buffering=0)
    blocks = [
        ("transactional-store", "db"),
        ("object-store", "store"),
        ("ephemeral-kv-cache", "cache"),
    ]
    for i, (bt, name) in enumerate(blocks, 1):
        req = json.dumps({"id": i, "method": "Acquire", "params": {"block_type": bt, "name": name}}) + "\n"
        print(f"  Acquiring {bt}/{name} ...", end=" ", flush=True)
        t0 = time.monotonic()
        s.sendall(req.encode())
        resp = json.loads(f.readline().decode())
        if "error" in resp:
            print(f"FAILED: {resp['error']}")
        else:
            print(f"OK ({time.monotonic() - t0:.1f}s)")
    # Drop
    s.sendall(json.dumps({"id": len(blocks) + 1, "method": "DropToScalingOnly", "params": {}}).encode() + b"\n")
    f.readline()
    f.close()
    s.close()


def snapshot_mem(daemon_pid: int | None) -> dict[str, int | None]:
    """Capture RSS for daemon + all measure containers."""
    snap = {}
    if daemon_pid:
        try:
            snap["platformd"] = get_process_rss(daemon_pid)
        except Exception:
            snap["platformd"] = None

    out = subprocess.run(
        ["docker", "ps", "--filter", "name=nalsd-measure-",
         "--format", "{{.Names}}"],
        capture_output=True, text=True,
    )
    for cname in sorted(out.stdout.strip().splitlines()):
        if cname:
            snap[cname] = get_container_mem(cname)
    return snap


def print_comparison(label_before: str, before: dict, label_after: str, after: dict) -> None:
    all_keys = list(dict.fromkeys(list(before) + list(after)))
    print(f"\n  {'Component':<42s} {label_before:>12s} {label_after:>12s} {'Delta':>12s}")
    print(f"  {'─' * 78}")
    total_before = 0
    total_after = 0
    for k in all_keys:
        b = before.get(k)
        a = after.get(k)
        b_str = mb(b) if b is not None else "n/a"
        a_str = mb(a) if a is not None else "n/a"
        if b is not None and a is not None:
            d = a - b
            d_str = f"+{mb(d)}" if d >= 0 else f"-{mb(-d)}"
            total_before += b
            total_after += a
        else:
            d_str = ""
            total_before += b or 0
            total_after += a or 0
        print(f"  {k:<42s} {b_str:>12s} {a_str:>12s} {d_str:>12s}")
    print(f"  {'─' * 78}")
    d = total_after - total_before
    d_str = f"+{mb(d)}" if d >= 0 else f"-{mb(-d)}"
    print(f"  {'TOTAL':<42s} {mb(total_before):>12s} {mb(total_after):>12s} {d_str:>12s}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=30, help="seconds of load per backend")
    parser.add_argument("--rps", type=int, default=100, help="requests per second per backend")
    args = parser.parse_args()

    print("=" * 60)
    print(f"nalsd — Load Test ({args.rps} rps × {args.duration}s per backend)")
    print("=" * 60)

    with tempfile.TemporaryDirectory(prefix="nalsd-load-") as tmpdir:
        tmpdir = Path(tmpdir)
        config_path = write_config(tmpdir)
        sock_path = tmpdir / "platformd.sock"

        # Clean up previous containers
        for name in ["nalsd-measure-db", "nalsd-measure-store", "nalsd-measure-cache"]:
            subprocess.run(["docker", "rm", "-f", name],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        print("\n[1/5] Starting platformd and provisioning blocks ...")
        env = os.environ.copy()
        daemon = subprocess.Popen(
            [str(VENV_PYTHON), "-m", "platformd", "--config", str(config_path)],
            cwd=str(REPO), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        try:
            # Wait for socket
            deadline = time.monotonic() + 15.0
            while time.monotonic() < deadline:
                if sock_path.exists():
                    try:
                        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                        s.connect(str(sock_path))
                        s.close()
                        break
                    except OSError:
                        pass
                time.sleep(0.3)
            else:
                raise TimeoutError("daemon not ready")

            provision_blocks(sock_path)
            time.sleep(2)  # settle

            print("\n[2/5] Baseline RSS (idle after provisioning) ...")
            before = snapshot_mem(daemon.pid)
            for k, v in before.items():
                print(f"  {k:<42s} {mb(v) if v else 'n/a':>12s}")

            print(f"\n[3/5] Running load: {args.rps} rps × {args.duration}s (all 3 backends in parallel) ...")

            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                fut_pg = pool.submit(load_postgres, args.duration, args.rps)
                fut_rd = pool.submit(load_redis, args.duration, args.rps)
                fut_s3 = pool.submit(load_minio, args.duration, args.rps)

                # Print progress dots
                t_end = time.monotonic() + args.duration
                while time.monotonic() < t_end:
                    elapsed = args.duration - (t_end - time.monotonic())
                    print(f"\r  [{int(elapsed)}/{args.duration}s]", end="", flush=True)
                    time.sleep(1)
                print(f"\r  [{args.duration}/{args.duration}s]")

                pg_stats = fut_pg.result()
                rd_stats = fut_rd.result()
                s3_stats = fut_s3.result()

            print("\n[4/5] RSS under load (sampled at end of load window) ...")
            after = snapshot_mem(daemon.pid)

            print_comparison("Idle", before, "Under load", after)

            # Wait for settle, measure again
            print(f"\n[5/5] RSS after load settles (5s cool-down) ...")
            time.sleep(5)
            settled = snapshot_mem(daemon.pid)
            print_comparison("Under load", after, "Settled", settled)

            # Print load stats
            print(f"\n{'─' * 60}")
            print("  Load results:")
            for label, s in [("postgres", pg_stats), ("redis", rd_stats), ("minio", s3_stats)]:
                sm = s.summary()
                print(
                    f"  {label:<12s}  ok={sm['ok']:>5d}  err={sm['err']:>3d}  "
                    f"p50={sm['p50_ms']:>6.1f}ms  p99={sm['p99_ms']:>6.1f}ms  "
                    f"mean={sm['mean_ms']:>6.1f}ms"
                )
            print()

        finally:
            daemon.send_signal(signal.SIGTERM)
            daemon.wait(timeout=10)
            for name in ["nalsd-measure-db", "nalsd-measure-store", "nalsd-measure-cache"]:
                subprocess.run(["docker", "rm", "-f", name],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
