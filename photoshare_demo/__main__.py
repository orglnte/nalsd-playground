"""Run the photoshare-demo service with `python -m photoshare_demo`."""

from __future__ import annotations

import logging

import uvicorn

from photoshare_demo.main import build


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    app = build()
    # Single worker, no reload, minimal footprint.
    uvicorn.run(app, host="127.0.0.1", port=8080, log_level="info")


if __name__ == "__main__":
    main()
