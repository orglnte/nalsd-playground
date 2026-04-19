# SARP

Prototype. Documentation pending.

## Run the demo

Requires Docker and Python 3.10+.

```sh
# install
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'

# run the control plane (one terminal)
.venv/bin/python -m platformd --config dev-config/platformd.toml

# run the demo service (another terminal)
.venv/bin/python -m photoshare_demo
```

The service exposes a small HTTP API; open `http://127.0.0.1:8000` to use it.

## Tear down

```sh
.venv/bin/python -m platformd destroy --service-id photoshare --yes
```

## Tests

```sh
.venv/bin/python -m pytest
```

E2E tests that need Docker auto-skip when Docker is unavailable.

## License

See `LICENSE`.
