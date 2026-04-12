#!/usr/bin/env python3
"""
Measure RSS footprint of platformd + provisioned containers.

Starts the daemon, connects as a client, acquires all three block types,
then reports memory usage for:
  - platformd Python process (RSS)
  - each provisioned Docker container (RSS from docker stats)

Usage:
    .venv/bin/python scripts/measure_rss.py
"""
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
VENV_PYTHON = REPO / ".venv" / "bin" / "python"


def mb(bytes_val: int) -> str:
    return f"{bytes_val / 1024 / 1024:.1f} MB"


def get_process_rss(pid: int) -> int:
    """Return RSS in bytes for a given PID (macOS + Linux)."""
    if sys.platform == "darwin":
        out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)])
        return int(out.strip()) * 1024  # ps reports KB on macOS
    else:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    raise RuntimeError(f"cannot read RSS for pid {pid}")


def get_container_mem(container_name: str) -> int | None:
    """Return container memory usage in bytes via docker stats."""
    try:
        out = subprocess.check_output(
            ["docker", "stats", "--no-stream", "--format",
             "{{.MemUsage}}", container_name],
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
        # Format: "12.34MiB / 96MiB" or "1.234GiB / 96MiB"
        usage = out.decode().strip().split("/")[0].strip()
        if "GiB" in usage:
            return int(float(usage.replace("GiB", "")) * 1024 * 1024 * 1024)
        if "MiB" in usage:
            return int(float(usage.replace("MiB", "")) * 1024 * 1024)
        if "KiB" in usage:
            return int(float(usage.replace("KiB", "")) * 1024)
        return None
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None


def write_config(tmpdir: Path) -> Path:
    """Write a temporary daemon config that allows all three block types."""
    scope_dir = tmpdir / "scopes"
    scope_dir.mkdir()

    # Scope: allow all blocks
    (scope_dir / "measure.toml").write_text(
        'service_id = "measure"\n'
        'allowed_blocks = ["transactional-store", "object-store", "ephemeral-kv-cache"]\n'
        "max_blocks = 8\n"
    )

    # Identities: map current UID to "measure"
    uid = os.getuid()
    ident_path = tmpdir / "identities.toml"
    ident_path.write_text(
        f"[[identities]]\nuid = {uid}\n"
        f'service_id = "measure"\n'
    )

    # Daemon config
    sock_path = tmpdir / "platformd.sock"
    config_path = tmpdir / "platformd.toml"
    config_path.write_text(
        f'socket_path = "{sock_path}"\n'
        f'scope_dir = "{scope_dir}"\n'
        f'identities_path = "{ident_path}"\n'
        "\n"
        "[service.measure]\n"
        'mode = "enforce"\n'
    )
    return config_path


def start_daemon(config_path: Path) -> subprocess.Popen:
    env = os.environ.copy()
    return subprocess.Popen(
        [str(VENV_PYTHON), "-m", "platformd", "--config", str(config_path)],
        cwd=str(REPO),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def wait_for_socket(sock_path: Path, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if sock_path.exists():
            try:
                s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                s.connect(str(sock_path))
                s.close()
                return
            except OSError:
                pass
        time.sleep(0.3)
    raise TimeoutError(f"daemon socket {sock_path} not ready after {timeout}s")


def rpc_call(sock_path: Path, method: str, params: dict) -> dict:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(str(sock_path))
    f = s.makefile("rwb", buffering=0)
    req = json.dumps({"id": 1, "method": method, "params": params}) + "\n"
    s.sendall(req.encode())
    resp = json.loads(f.readline().decode())
    # Keep connection open for subsequent calls — but for simplicity
    # we open a fresh connection per acquire (single-connection daemon).
    f.close()
    s.close()
    if "error" in resp:
        raise RuntimeError(f"RPC error: {resp['error']}")
    return resp.get("result")


def main() -> None:
    print("=" * 60)
    print("nalsd — RSS Footprint Measurement")
    print("=" * 60)

    with tempfile.TemporaryDirectory(prefix="nalsd-measure-") as tmpdir:
        tmpdir = Path(tmpdir)
        config_path = write_config(tmpdir)
        sock_path = tmpdir / "platformd.sock"

        # Clean up any leftover containers from a previous run
        for name in [
            "nalsd-measure-db", "nalsd-measure-store", "nalsd-measure-cache",
        ]:
            subprocess.run(
                ["docker", "rm", "-f", name],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )

        print("\n[1/4] Starting platformd ...")
        daemon = start_daemon(config_path)
        try:
            wait_for_socket(sock_path)
            daemon_pid = daemon.pid
            rss_idle = get_process_rss(daemon_pid)
            print(f"  platformd PID: {daemon_pid}")
            print(f"  RSS (idle, no blocks): {mb(rss_idle)}")

            # Acquire blocks one at a time (single-connection daemon).
            # Each acquire opens a new connection.
            blocks = [
                ("transactional-store", "db"),
                ("object-store", "store"),
                ("ephemeral-kv-cache", "cache"),
            ]

            for i, (block_type, name) in enumerate(blocks, 1):
                print(f"\n[{i+1}/4] Acquiring {block_type}/{name} ...")
                t0 = time.monotonic()
                # Each acquire needs its own session (connection).
                # But we need all blocks in one session for a real test.
                # Actually the single-connection daemon means we need one
                # connection that sends multiple acquires.
                # Let's do it properly with a raw multi-call session.
                pass  # handled below

            # Do a single session with all acquires
            print("\n[2/4] Acquiring all blocks in one session ...")
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.connect(str(sock_path))
            f = s.makefile("rwb", buffering=0)
            req_id = 0
            for block_type, name in blocks:
                req_id += 1
                req = json.dumps({
                    "id": req_id,
                    "method": "Acquire",
                    "params": {"block_type": block_type, "name": name},
                }) + "\n"
                print(f"  -> Acquire {block_type}/{name} ...", end=" ", flush=True)
                t0 = time.monotonic()
                s.sendall(req.encode())
                resp = json.loads(f.readline().decode())
                elapsed = time.monotonic() - t0
                if "error" in resp:
                    print(f"FAILED ({resp['error']})")
                    continue
                print(f"OK ({elapsed:.1f}s)")

            # Drop to scaling only
            req_id += 1
            s.sendall(json.dumps({
                "id": req_id,
                "method": "DropToScalingOnly",
                "params": {},
            }).encode() + b"\n")
            f.readline()
            f.close()
            s.close()

            # Let things settle
            print("\n[3/4] Letting processes settle (3s) ...")
            time.sleep(3)

            # Measure daemon RSS after provisioning
            rss_loaded = get_process_rss(daemon_pid)
            print(f"\n[4/4] Measurements")
            print(f"{'─' * 60}")
            print(f"  platformd RSS (idle):      {mb(rss_idle)}")
            print(f"  platformd RSS (3 blocks):  {mb(rss_loaded)}")
            print()

            # Measure containers
            container_prefix = "nalsd-measure-"
            out = subprocess.check_output(
                ["docker", "ps", "--filter", f"name={container_prefix}",
                 "--format", "{{.Names}}"],
            ).decode().strip()
            if out:
                total_container_mem = 0
                for cname in sorted(out.splitlines()):
                    mem = get_container_mem(cname)
                    if mem is not None:
                        total_container_mem += mem
                        print(f"  {cname:40s} {mb(mem)}")
                    else:
                        print(f"  {cname:40s} (unavailable)")
                print(f"{'─' * 60}")
                print(f"  Container total:           {mb(total_container_mem)}")
                print(f"  System total (daemon+ctr): {mb(rss_loaded + total_container_mem)}")
            else:
                print("  (no containers found)")

            print()

        finally:
            daemon.send_signal(signal.SIGTERM)
            daemon.wait(timeout=10)
            # Clean up containers
            out = subprocess.run(
                ["docker", "ps", "-a", "--filter", "name=nalsd-measure-",
                 "--format", "{{.Names}}"],
                capture_output=True, text=True,
            )
            for cname in out.stdout.strip().splitlines():
                if cname:
                    subprocess.run(
                        ["docker", "rm", "-f", cname],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    )


if __name__ == "__main__":
    main()
