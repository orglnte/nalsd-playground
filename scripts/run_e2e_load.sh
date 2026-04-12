#!/usr/bin/env bash
# Provision postgres + garage via platformd, start bench_service, run E2E load test.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
VENV="$REPO/.venv/bin/python"
LOAD_BIN="$REPO/scripts/load-test-e2e/target/release/load-test-e2e"
TMPDIR=$(mktemp -d /tmp/nalsd-e2e-XXXXXX)

cleanup() {
    kill $SVC_PID 2>/dev/null || true
    kill $DAEMON_PID 2>/dev/null || true
    docker rm -f nalsd-measure-db nalsd-measure-store 2>/dev/null || true
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

UID_VAL=$(id -u)

mkdir -p "$TMPDIR/scopes"
cat > "$TMPDIR/scopes/measure.toml" <<EOF
service_id = "measure"
allowed_blocks = ["transactional-store", "object-store"]
max_blocks = 4
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

docker rm -f nalsd-measure-db nalsd-measure-store 2>/dev/null || true

echo "[e2e] Starting platformd ..."
"$VENV" -m platformd --config "$TMPDIR/platformd.toml" &
DAEMON_PID=$!

for i in $(seq 1 30); do
    [ -S "$TMPDIR/platformd.sock" ] && break
    sleep 0.5
done

echo "[e2e] Acquiring blocks ..."
python3 -c "
import socket, json, time

sock_path = '$TMPDIR/platformd.sock'
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.connect(sock_path)
f = s.makefile('rwb', buffering=0)

blocks = [
    ('transactional-store', 'db'),
    ('object-store', 'store'),
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

s.sendall(json.dumps({'id': len(blocks)+1, 'method': 'DropToScalingOnly', 'params': {}}).encode() + b'\n')
f.readline()
f.close()
s.close()
"

echo "[e2e] Starting bench service on :8090 ..."
cd "$REPO/scripts"
"$VENV" bench_service.py &
SVC_PID=$!
cd "$REPO"

# Wait for service
for i in $(seq 1 20); do
    curl -s -o /dev/null http://127.0.0.1:8090/db && break
    sleep 0.5
done

# Seed some writes so reads don't 404
echo "[e2e] Seeding writes ..."
for i in $(seq 1 50); do
    curl -s -X POST http://127.0.0.1:8090/db > /dev/null
    curl -s -X POST http://127.0.0.1:8090/store > /dev/null
done

echo "[e2e] Starting load test ..."
echo ""

caffeinate -i "$LOAD_BIN" "$@"

echo "[e2e] Done."
