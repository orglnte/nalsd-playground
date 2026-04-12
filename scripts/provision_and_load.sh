#!/usr/bin/env bash
# Provision containers via platformd, then run Rust load test.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
VENV="$REPO/.venv/bin/python"
LOAD_BIN="$REPO/scripts/load-test/target/release/load-test"
TMPDIR=$(mktemp -d /tmp/nalsd-load-XXXXXX)

trap 'kill $DAEMON_PID 2>/dev/null; docker rm -f nalsd-measure-db nalsd-measure-store nalsd-measure-store-lite nalsd-measure-cache 2>/dev/null; rm -rf "$TMPDIR"' EXIT

UID_VAL=$(id -u)

# Write temp config
mkdir -p "$TMPDIR/scopes"
cat > "$TMPDIR/scopes/measure.toml" <<EOF
service_id = "measure"
allowed_blocks = ["transactional-store", "object-store", "lightweight-object-store", "ephemeral-kv-cache"]
max_blocks = 8
EOF

cat > "$TMPDIR/identities.toml" <<EOF
[[identities]]
uid = $UID_VAL
service_id = "measure"
EOF

cat > "$TMPDIR/platformd.toml" <<EOF
socket_path = "$TMPDIR/platformd.sock"
scope_dir = "$TMPDIR/scopes"
identities_path = "$TMPDIR/identities.toml"

[service.measure]
mode = "enforce"
EOF

# Clean up previous containers
docker rm -f nalsd-measure-db nalsd-measure-store nalsd-measure-store-lite nalsd-measure-cache 2>/dev/null || true

# Start daemon
echo "[provision] Starting platformd ..."
"$VENV" -m platformd --config "$TMPDIR/platformd.toml" &
DAEMON_PID=$!

# Wait for socket
for i in $(seq 1 30); do
    [ -S "$TMPDIR/platformd.sock" ] && break
    sleep 0.5
done

# Acquire blocks via raw JSON over UDS
echo "[provision] Acquiring blocks ..."
python3 -c "
import socket, json, time

sock_path = '$TMPDIR/platformd.sock'
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.connect(sock_path)
f = s.makefile('rwb', buffering=0)

blocks = [
    ('transactional-store', 'db'),
    ('object-store', 'store'),
    ('lightweight-object-store', 'store-lite'),
    ('ephemeral-kv-cache', 'cache'),
]
for i, (bt, name) in enumerate(blocks, 1):
    req = json.dumps({'id': i, 'method': 'Acquire', 'params': {'block_type': bt, 'name': name}}) + '\n'
    print(f'  Acquiring {bt}/{name} ...', end=' ', flush=True)
    t0 = time.monotonic()
    s.sendall(req.encode())
    resp = json.loads(f.readline().decode())
    if 'error' in resp:
        print(f'FAILED: {resp[\"error\"]}')
    else:
        print(f'OK ({time.monotonic()-t0:.1f}s)')

# Drop
s.sendall(json.dumps({'id': len(blocks)+1, 'method': 'DropToScalingOnly', 'params': {}}).encode() + b'\n')
f.readline()
f.close()
s.close()
"

echo "[provision] Containers ready. Starting Rust load test ..."
echo ""

# Run load test — pass through all args
"$LOAD_BIN" "$@"

echo "[provision] Done. Cleaning up ..."
