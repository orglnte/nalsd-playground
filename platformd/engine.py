"""
Platform engine: the Pulumi Automation API driver.

Owns the Pulumi stack lifecycle for a single service. Translates BlockSpecs
into Docker resources via the inline-program pattern, runs stack.up() to
converge, polls the newly-provisioned resource for true readiness, and
returns Credentials.
"""

from __future__ import annotations

import logging
import os
import socket
import time
from pathlib import Path

from platform_api.errors import ProvisioningError, ReadinessTimeoutError
from platform_api.types import BlockSpec, BlockType, Credentials
from platformd.blocks import BackendConfig, backend_for

log = logging.getLogger("platformd.engine")

REPO_ROOT = Path(__file__).resolve().parent.parent
PULUMI_BIN = REPO_ROOT / ".pulumi" / "bin"
STATE_DIR = REPO_ROOT / ".pulumi_state"


def _setup_pulumi_env() -> None:
    path = os.environ.get("PATH", "")
    if str(PULUMI_BIN) not in path.split(os.pathsep):
        os.environ["PATH"] = f"{PULUMI_BIN}{os.pathsep}{path}"
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    os.environ["PULUMI_BACKEND_URL"] = f"file://{STATE_DIR}"
    os.environ.setdefault("PULUMI_CONFIG_PASSPHRASE", "")
    os.environ.setdefault("PULUMI_SKIP_UPDATE_CHECK", "true")


class PulumiDockerEngine:
    """
    Drives Pulumi Automation API with the Docker provider.

    One engine instance per service. Holds the authoritative view of active
    leases and rebuilds the inline program on each provision() call so that
    Pulumi diffs and provisions only the delta.
    """

    def __init__(
        self,
        service_id: str,
        *,
        project_name: str = "nalsd-platform",
    ) -> None:
        _setup_pulumi_env()
        self.service_id = service_id
        self.project_name = project_name
        self._host = "127.0.0.1"

    def provision(
        self,
        spec: BlockSpec,
        *,
        existing_leases: dict[str, BlockSpec],
    ) -> Credentials:
        backend = backend_for(spec)
        self._run_stack_up(existing_leases)
        self._wait_ready(backend)
        creds = self._build_credentials(spec, backend)
        log.info(
            "provisioned %s/%s at %s:%d",
            spec.block_type.value,
            spec.name,
            creds.host,
            creds.port,
        )
        return creds

    def destroy(self) -> None:
        from pulumi import automation as auto

        try:
            stack = auto.select_stack(
                stack_name=self.service_id,
                project_name=self.project_name,
                program=lambda: None,
            )
        except auto.StackNotFoundError:
            return
        log.info("destroying stack %s", self.service_id)
        stack.destroy(on_output=self._log_pulumi)
        stack.workspace.remove_stack(self.service_id)

    def _run_stack_up(self, leases: dict[str, BlockSpec]) -> None:
        from pulumi import automation as auto

        # Snapshot the lease set so the closure is immune to any later
        # mutation of the caller's dict. The inline program may be invoked
        # by Pulumi asynchronously relative to this call.
        snapshot = dict(leases)

        def program() -> None:
            self._render_program(snapshot)

        try:
            stack = auto.create_or_select_stack(
                stack_name=self.service_id,
                project_name=self.project_name,
                program=program,
            )
        except Exception as e:
            raise ProvisioningError(f"failed to create/select stack: {e}") from e

        # refresh=True reconciles Pulumi state against the real Docker
        # daemon before planning, so containers deleted out-of-band
        # (docker rm, Docker Desktop restart, machine reboot that wiped
        # non-persistent state) are detected as missing and re-created.
        # Without this, the engine would plan against a stale view and the
        # subsequent readiness poll would time out against a non-existent
        # container.
        try:
            stack.up(on_output=self._log_pulumi, refresh=True)
        except Exception as e:
            raise ProvisioningError(f"stack.up failed: {e}") from e

    def _render_program(self, leases: dict[str, BlockSpec]) -> None:
        """Inline Pulumi program: declare a container per active lease."""
        import pulumi_docker as docker

        for lease_name, spec in leases.items():
            backend = backend_for(spec)
            envs = [f"{k}={v}" for k, v in backend.env_vars.items()]
            image = docker.RemoteImage(
                f"img-{lease_name}",
                name=backend.image,
                keep_locally=True,
            )
            ports = [
                docker.ContainerPortArgs(
                    internal=backend.container_port,
                    external=backend.host_port,
                    ip=self._host,
                    protocol="tcp",
                )
            ]

            docker.Container(
                f"ctr-{lease_name}",
                image=image.image_id,
                name=f"nalsd-{self.service_id}-{lease_name}",
                envs=envs,
                command=backend.command,
                ports=ports,
                tmpfs=backend.tmpfs or None,
                memory=backend.memory_mb,
                memory_swap=backend.memory_swap_mb,
                restart="unless-stopped",
                must_run=True,
                rm=False,
            )

    def _wait_ready(self, backend: BackendConfig) -> None:
        deadline = time.monotonic() + backend.readiness.timeout_s
        last_error: Exception | None = None
        log.info(
            "waiting for %s readiness on %s:%d (timeout=%.0fs)",
            backend.readiness.kind,
            self._host,
            backend.host_port,
            backend.readiness.timeout_s,
        )
        while time.monotonic() < deadline:
            try:
                if self._check_ready(backend):
                    return
            except Exception as e:
                last_error = e
            time.sleep(backend.readiness.interval_s)
        raise ReadinessTimeoutError(
            f"{backend.readiness.kind} at {self._host}:{backend.host_port} "
            f"not ready after {backend.readiness.timeout_s}s: {last_error}"
        )

    def _check_ready(self, backend: BackendConfig) -> bool:
        kind = backend.readiness.kind
        if kind == "postgres":
            return self._check_postgres(backend)
        if kind == "redis":
            return self._check_redis(backend)
        if kind == "rustfs":
            return self._check_rustfs(backend)
        raise ValueError(f"unknown readiness kind {kind}")

    def _tcp_open(self, port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.5)
            try:
                s.connect((self._host, port))
                return True
            except OSError:
                return False

    def _check_postgres(self, backend: BackendConfig) -> bool:
        if not self._tcp_open(backend.host_port):
            return False
        import psycopg

        dsn = (
            f"postgresql://{backend.username}:{backend.password}"
            f"@{self._host}:{backend.host_port}/{backend.database}"
        )
        with psycopg.connect(dsn, connect_timeout=2) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return True

    def _check_redis(self, backend: BackendConfig) -> bool:
        if not self._tcp_open(backend.host_port):
            return False
        import redis

        client = redis.Redis(
            host=self._host,
            port=backend.host_port,
            password=backend.password,
            socket_timeout=2,
        )
        try:
            return bool(client.ping())
        finally:
            client.close()

    def _check_rustfs(self, backend: BackendConfig) -> bool:
        # RustFS requires auth on all HTTP endpoints including health.
        # TCP connectivity is sufficient — if the port accepts connections
        # the server is ready to handle S3 requests.
        return self._tcp_open(backend.host_port)

    def _build_credentials(self, spec: BlockSpec, backend: BackendConfig) -> Credentials:
        extras: dict[str, object] = dict(backend.extras)
        if spec.block_type is BlockType.OBJECT_STORE:
            extras.setdefault("bucket", spec.name)
        # Surface capacity hints so the service can size its own clients
        # (pools, connection limits) to match the provisioned
        # infrastructure. Keys are block-specific; see blocks.py renderers.
        extras.update(backend.capacity_hints)
        extras["memory_mb"] = backend.memory_mb
        extras["storage_mb"] = backend.storage_mb
        return Credentials(
            block_type=spec.block_type,
            name=spec.name,
            host=self._host,
            port=backend.host_port,
            username=backend.username,
            password=backend.password,
            database=backend.database,
            extras=extras,
        )

    def _log_pulumi(self, line: str) -> None:
        line = line.rstrip()
        if line:
            log.debug("pulumi: %s", line)
