from __future__ import annotations

import argparse
import logging
import signal
import sys
from pathlib import Path

from platformd.server import build_server


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="platformd")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("dev-config/platformd.toml"),
        help="path to platformd.toml",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    server = build_server(args.config)

    def _shutdown(signum, frame):  # noqa: ARG001
        logging.getLogger("platformd").info("signal %d, shutting down", signum)
        server.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    server.start()
    server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
