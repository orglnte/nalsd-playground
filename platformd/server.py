from __future__ import annotations

import json
import logging
import os
import socket
from collections.abc import Callable
from pathlib import Path
from typing import Any

from platform_api.errors import PlatformError
from platform_api.protocol import (
    decode_block_spec,
    encode_credentials,
    error_response,
    result_response,
)
from platformd.auth import peer_uid
from platformd.config import DaemonConfig
from platformd.engine_protocol import Engine
from platformd.identities import Identities, UnknownPeerError
from platformd.scope_store import ScopeNotFoundError, ScopeStore
from platformd.session import EnforcingSession, RecordingSession, Session

log = logging.getLogger("platformd.server")

EngineFactory = Callable[[str], Engine]


class Server:
    """
    Line-delimited JSON-RPC over a Unix domain socket.

    One connection at a time (prototype). The server authenticates the
    peer by OS-provided UID, maps to service_id, loads the scope from
    the ScopeStore, and hands an accept()ed socket to a fresh Session.
    """

    def __init__(
        self,
        config: DaemonConfig,
        identities: Identities,
        scope_store: ScopeStore,
        engine_factory: EngineFactory,
    ) -> None:
        self._config = config
        self._identities = identities
        self._scope_store = scope_store
        self._engine_factory = engine_factory
        self._engines: dict[str, Engine] = {}
        self._listen_sock: socket.socket | None = None
        self._stopped = False

    def _engine_for(self, service_id: str) -> Engine:
        if service_id not in self._engines:
            self._engines[service_id] = self._engine_factory(service_id)
        return self._engines[service_id]

    def start(self) -> None:
        sock_path = self._config.socket_path
        sock_path.parent.mkdir(parents=True, exist_ok=True)
        if sock_path.exists():
            sock_path.unlink()
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.bind(str(sock_path))
        os.chmod(sock_path, 0o600)
        s.listen(1)
        self._listen_sock = s
        log.info("platformd listening on %s", sock_path)

    def serve_forever(self) -> None:
        assert self._listen_sock is not None, "start() must be called first"
        while not self._stopped:
            try:
                conn, _ = self._listen_sock.accept()
            except OSError:
                if self._stopped:
                    return
                raise
            try:
                self._handle_connection(conn)
            finally:
                conn.close()

    def stop(self) -> None:
        self._stopped = True
        if self._listen_sock is not None:
            try:
                self._listen_sock.close()
            finally:
                if self._config.socket_path.exists():
                    self._config.socket_path.unlink()

    def _handle_connection(self, conn: socket.socket) -> None:
        try:
            uid = peer_uid(conn)
            service_id = self._identities.service_for_uid(uid)
        except (UnknownPeerError, ValueError) as e:
            log.warning("rejecting connection: %s", e)
            _write(conn, error_response(None, e))
            return

        mode = self._config.mode_for(service_id)
        engine = self._engine_for(service_id)
        try:
            if mode == "record":
                recording_output = self._config.scope_dir / f"{service_id}.recorded.toml"
                session: Session = RecordingSession(
                    service_id,
                    engine,
                    recording_output,
                )
            else:
                scope = self._scope_store.get(service_id)
                session = EnforcingSession(service_id, engine, scope)
        except (ScopeNotFoundError, ValueError) as e:
            log.warning("rejecting connection: %s", e)
            _write(conn, error_response(None, e))
            return
        log.info("session start: service=%s uid=%d mode=%s", service_id, uid, mode)

        f = conn.makefile("rwb", buffering=0)
        try:
            for raw in f:
                line = raw.decode("utf-8").strip()
                if not line:
                    continue
                _write(conn, self._dispatch(session, line))
        finally:
            session.shutdown()
            f.close()
            log.info("session end: service=%s", service_id)

    def _dispatch(self, session: Session, line: str) -> dict[str, Any]:
        try:
            request = json.loads(line)
        except json.JSONDecodeError as e:
            return error_response(None, ValueError(f"malformed JSON: {e}"))

        request_id = request.get("id")
        method = request.get("method")
        params = request.get("params") or {}
        try:
            result = self._run_method(session, method, params)
        except PlatformError as e:
            return error_response(request_id, e)
        except ValueError as e:
            return error_response(request_id, e)
        except Exception as e:
            log.exception("unhandled error in %s", method)
            return error_response(request_id, e)
        return result_response(request_id, result)

    def _run_method(self, session: Session, method: str, params: dict[str, Any]) -> Any:
        if method == "Acquire":
            spec = decode_block_spec(params)
            creds = session.acquire(
                spec.block_type,
                name=spec.name,
                compute=spec.compute,
                storage=spec.storage,
                rps=spec.rps,
                **spec.params,
            )
            return encode_credentials(creds)
        if method == "DropToScalingOnly":
            session.drop_to_scaling_only()
            return None
        if method == "ScaleHint":
            session.scale_hint(params["name"], load_factor=float(params["load_factor"]))
            return None
        if method == "Shutdown":
            session.shutdown()
            return None
        raise ValueError(f"unknown method '{method}'")


def _write(conn: socket.socket, obj: dict[str, Any]) -> None:
    conn.sendall((json.dumps(obj) + "\n").encode("utf-8"))


def default_engine_factory(service_id: str) -> Engine:
    from platformd.engine import PulumiDockerEngine

    return PulumiDockerEngine(service_id=service_id)


def build_server(config_path: Path) -> Server:
    from platformd.config import load_daemon_config
    from platformd.identities import load_identities

    config = load_daemon_config(config_path)
    identities = load_identities(config.identities_path)
    scope_store = ScopeStore(scope_dir=config.scope_dir)
    return Server(
        config=config,
        identities=identities,
        scope_store=scope_store,
        engine_factory=default_engine_factory,
    )
