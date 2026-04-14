from __future__ import annotations

import argparse
import logging
import signal
import sys
from pathlib import Path

from platformd.server import build_server

log = logging.getLogger("platformd")

_SUBCOMMANDS = ("serve", "destroy")


def main(argv: list[str] | None = None) -> int:
    raw = sys.argv[1:] if argv is None else list(argv)

    # Backward compat: `python -m platformd --config x.toml` (no subcommand)
    # still means "serve". Preserves pre-existing muscle memory and systemd
    # units. Subcommands become opt-in for new workflows.
    if not raw or raw[0] not in _SUBCOMMANDS:
        raw = ["serve", *raw]

    parser = argparse.ArgumentParser(prog="platformd")
    sub = parser.add_subparsers(dest="command", required=True)

    # `serve` — the control plane. Runs until SIGTERM/SIGINT.
    serve = sub.add_parser("serve", help="run the platform daemon")
    serve.add_argument(
        "--config",
        type=Path,
        default=Path("dev-config/platformd.toml"),
        help="path to platformd.toml",
    )

    # `destroy` — offline decommission of a service's infrastructure.
    # Deliberately NOT wired to daemon shutdown: containers outliving the
    # daemon is a core architectural invariant (services reconnect after
    # daemon restart with state intact). Destroy is an explicit operator
    # action for service retirement.
    destroy = sub.add_parser(
        "destroy",
        help="tear down all infrastructure for a given service (operator action)",
    )
    destroy.add_argument("--service-id", required=True, help="service_id whose stack to destroy")
    destroy.add_argument(
        "--yes",
        action="store_true",
        help="skip the interactive confirmation (for automation)",
    )

    args = parser.parse_args(raw)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if args.command == "serve":
        return _cmd_serve(args.config)
    if args.command == "destroy":
        return _cmd_destroy(args.service_id, yes=args.yes)
    parser.error(f"unknown command: {args.command}")
    return 2  # unreachable; parser.error exits


def _cmd_serve(config_path: Path) -> int:
    server = build_server(config_path)

    def _shutdown(signum: int, frame: object) -> None:
        log.info("signal %d, shutting down", signum)
        server.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    server.start()
    server.serve_forever()
    return 0


def _cmd_destroy(service_id: str, *, yes: bool) -> int:
    # Import lazily so `serve` doesn't pay the Pulumi import cost at startup.
    from platformd.engine import PulumiDockerEngine

    if not yes:
        print(
            f"This will destroy all infrastructure provisioned for service "
            f"'{service_id}' (containers, Pulumi stack, state).\n"
            f"Type the service_id to confirm:"
        )
        try:
            typed = input("> ").strip()
        except EOFError:
            typed = ""
        if typed != service_id:
            print("Cancelled: confirmation did not match.")
            return 1

    log.info("destroying stack for service_id=%s", service_id)
    engine = PulumiDockerEngine(service_id=service_id)
    engine.destroy()
    log.info("stack destroyed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
