#!/usr/bin/env python3
"""
nalsd E2E load test — subclass of PEM's LoadTestOrchestrator.

Usage:
    python scripts/run_e2e_load.py [--rps 1000] [--duration 10m] [--rustfs-mem 1g]
"""
from __future__ import annotations

import importlib.util
import os
import signal
import subprocess
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# Import the orchestrator from PEM
spec = importlib.util.spec_from_file_location(
    "load_test_lib",
    Path.home() / ".claude/skills/pem/universal/load-test-orchestrator.py",
)
lib = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lib)

VENV_PYTHON = str(REPO / ".venv/bin/python")


class NalsdLoadTest(lib.LoadTestOrchestrator):
    containers = ["nalsd-photoshare-images", "nalsd-photoshare-photos"]
    k6_script = "scripts/k6/photo-app.ts"
    repo = REPO
    results_base = Path("tmp/load_test_results")

    def add_arguments(self, parser):
        parser.add_argument("--rustfs-mem", default="1g")

    def setup(self) -> int | None:
        # Clean old state
        for pattern in ["bench_service", "python -m photoshare", "python -m platformd"]:
            subprocess.run(f"pkill -9 -f '{pattern}'", shell=True, capture_output=True)
        for port in [8080, 8090]:
            subprocess.run(f"kill -9 $(lsof -ti :{port}) 2>/dev/null", shell=True, capture_output=True)
        for name in self.containers:
            subprocess.run(f"docker rm -f {name}", shell=True, capture_output=True)
        time.sleep(0.5)

        # Start platformd
        self._platformd_log = open(self.results_dir / "platformd.log", "w")
        self._platformd = subprocess.Popen(
            f"{VENV_PYTHON} -m platformd --config dev-config/platformd.toml",
            shell=True, stdout=self._platformd_log, stderr=subprocess.STDOUT,
            cwd=str(REPO), preexec_fn=os.setsid,
        )
        time.sleep(1.5)

        # Provision via photoshare
        print("  provisioning containers...")
        prov_log = open(self.results_dir / "photoshare_provision.log", "w")
        prov = subprocess.Popen(
            f"{VENV_PYTHON} -m photoshare",
            shell=True, stdout=prov_log, stderr=subprocess.STDOUT,
            cwd=str(REPO), preexec_fn=os.setsid,
        )

        # Wait for containers
        deadline = time.time() + 120
        while time.time() < deadline:
            r = lib.shell("docker ps --format '{{.Names}}'", check=False)
            if all(c in r.stdout for c in self.containers):
                break
            time.sleep(2)
        else:
            print("  ERROR: containers did not start")
            return None

        if not lib.wait_for_port(15432, timeout=60):
            print("  ERROR: postgres not ready")
            return None
        if not lib.wait_for_port(19000, timeout=60):
            print("  ERROR: RustFS not ready")
            return None
        print("  containers up.")

        # Kill photoshare (only needed for provisioning)
        os.killpg(os.getpgid(prov.pid), signal.SIGTERM)
        prov.wait(timeout=5)
        prov_log.close()

        # Raise RustFS memory
        mem = self.args.rustfs_mem
        lib.shell(f"docker update --memory {mem} --memory-swap {mem} nalsd-photoshare-images")
        print(f"  RustFS memory set to {mem}")

        # Start bench_service
        self._bench_log = open(self.results_dir / "bench_service.log", "w")
        self._bench = subprocess.Popen(
            f"{VENV_PYTHON} scripts/bench_service.py",
            shell=True, stdout=self._bench_log, stderr=subprocess.STDOUT,
            cwd=str(REPO), preexec_fn=os.setsid,
        )
        if not lib.wait_for_port(8090, timeout=15):
            print("  ERROR: bench_service not ready")
            print("  log:", (self.results_dir / "bench_service.log").read_text()[-500:])
            return None
        print(f"  bench_service up (PID {self._bench.pid})")

        return self._bench.pid

    def smoke(self) -> bool:
        r = lib.shell("curl -s --max-time 5 -X POST http://127.0.0.1:8090/photos", check=False)
        if r.returncode != 0 or "id" not in r.stdout:
            print(f"  failed: {r.stdout}")
            return False
        print(f"  {r.stdout.strip()}")
        return True

    def teardown(self):
        self._bench.terminate()
        self._bench.wait(timeout=5)
        self._bench_log.close()
        self._platformd.terminate()
        self._platformd.wait(timeout=5)
        self._platformd_log.close()

        # Collect RustFS internal log
        r = lib.shell("docker exec nalsd-photoshare-images cat /logs/rustfs.log", check=False, timeout=10)
        (self.results_dir / "rustfs_internal.log").write_text(r.stdout)


if __name__ == "__main__":
    NalsdLoadTest().run()
