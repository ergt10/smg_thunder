#!/usr/bin/env bash
# Phase 5 e2e: Thunder program state tracking and /programs endpoint.
#
# Boots mock_vllm.py on :8001, smg in thunder mode on :30000, then sends chat
# completions with program_id at both supported locations and asserts /programs
# reflects the per-program step counts.
#
# Run from repo root:  bash e2e_test/thunder/test_phase5.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

MOCK_PORT=${MOCK_PORT:-8001}
SMG_PORT=${SMG_PORT:-30000}
CANNED_CONTENT='hello-from-mock-phase5'
MOCK_LOG=/tmp/thunder_phase5_mock.log
SMG_LOG=/tmp/thunder_phase5_smg.log

cleanup() {
    set +e
    if [[ -n "${SMG_PID:-}" ]]; then kill "$SMG_PID" 2>/dev/null; wait "$SMG_PID" 2>/dev/null; fi
    if [[ -n "${MOCK_PID:-}" ]]; then kill "$MOCK_PID" 2>/dev/null; wait "$MOCK_PID" 2>/dev/null; fi
}
trap cleanup EXIT

echo "== boot mock_vllm on :$MOCK_PORT =="
python3 e2e_test/thunder/mock_vllm.py --port "$MOCK_PORT" --canned-content "$CANNED_CONTENT" \
    > "$MOCK_LOG" 2>&1 &
MOCK_PID=$!
sleep 1
curl -sS -m 3 "http://localhost:$MOCK_PORT/health" >/dev/null || { echo "mock did not start"; cat "$MOCK_LOG"; exit 1; }

echo "== boot smg in thunder mode on :$SMG_PORT =="
cargo run --quiet --bin smg -- --backend thunder \
    --worker-urls "http://localhost:$MOCK_PORT" \
    --port "$SMG_PORT" > "$SMG_LOG" 2>&1 &
SMG_PID=$!
for i in $(seq 1 30); do
    if curl -sS -m 1 "http://localhost:$SMG_PORT/health" >/dev/null 2>&1; then break; fi
    sleep 1
done
curl -sS -m 3 "http://localhost:$SMG_PORT/health" >/dev/null || {
    echo "smg did not become healthy"; tail -20 "$SMG_LOG"; exit 1;
}

post_chat() {
    local payload="$1"
    local outfile="$2"
    local code
    code=$(curl -sS -m 10 -o "$outfile" -w '%{http_code}' \
        -X POST "http://localhost:$SMG_PORT/v1/chat/completions" \
        -H 'content-type: application/json' \
        -d "$payload")
    echo "HTTP $code for $outfile"
    cat "$outfile"
    echo
    if [[ "$code" != "200" ]]; then
        echo "FAIL: expected HTTP 200, got $code"
        exit 1
    fi
}

echo "== send two distinct program_ids =="
post_chat '{"model":"mock","program_id":"program-A","messages":[{"role":"user","content":"ping A"}]}' \
    /tmp/thunder_phase5_a1.json
post_chat '{"model":"mock","extra_body":{"program_id":"program-B"},"messages":[{"role":"user","content":"ping B"}]}' \
    /tmp/thunder_phase5_b1.json

PROGRAMS_FILE=/tmp/thunder_phase5_programs_1.json
curl -sS -m 3 "http://localhost:$SMG_PORT/programs" -o "$PROGRAMS_FILE"
echo "programs after first two requests:"
cat "$PROGRAMS_FILE"
echo

python3 - "$PROGRAMS_FILE" <<'PY'
import json
import sys

programs = json.load(open(sys.argv[1], encoding="utf-8"))
for pid in ("program-A", "program-B"):
    assert pid in programs, f"missing {pid}: {programs}"
    item = programs[pid]
    assert item["step_count"] == 1, f"{pid} step_count={item['step_count']}"
    assert item["status"] == "acting", f"{pid} status={item['status']}"
    assert item["state"] == "active", f"{pid} state={item['state']}"
    assert item["total_tokens"] > 0, f"{pid} total_tokens={item['total_tokens']}"
    assert item["context_len"] > 0, f"{pid} context_len={item['context_len']}"
PY

echo "== resend program-A =="
post_chat '{"model":"mock","program_id":"program-A","messages":[{"role":"user","content":"ping A again"}]}' \
    /tmp/thunder_phase5_a2.json

PROGRAMS_FILE=/tmp/thunder_phase5_programs_2.json
curl -sS -m 3 "http://localhost:$SMG_PORT/programs" -o "$PROGRAMS_FILE"
echo "programs after resending program-A:"
cat "$PROGRAMS_FILE"
echo

python3 - "$PROGRAMS_FILE" <<'PY'
import json
import sys

programs = json.load(open(sys.argv[1], encoding="utf-8"))
assert programs["program-A"]["step_count"] == 2, programs
assert programs["program-B"]["step_count"] == 1, programs
assert programs["program-A"]["status"] == "acting", programs
assert programs["program-B"]["status"] == "acting", programs
PY

MOCK_STATE=$(curl -sS -m 3 "http://localhost:$MOCK_PORT/control/state")
echo "mock state: $MOCK_STATE"
if ! echo "$MOCK_STATE" | grep -q '"request_count": 3'; then
    echo "FAIL: mock did not see exactly 3 requests"
    exit 1
fi

echo
echo "PASS: Phase 5 e2e complete"
