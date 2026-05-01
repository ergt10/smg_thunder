#!/usr/bin/env bash
# Phase 6 e2e: vLLM metrics polling + BackendState capacity reporting via /thunder/metrics.
#
# Boots mock_vllm.py on :8001, smg in thunder mode on :30000, then
#   1) hits /thunder/metrics on a cold registry — asserts cache_config + zero counters
#   2) sends a chat completion with program_id, asserts metrics now show
#      reasoning/acting tokens + reduced remaining_capacity
#   3) shrinks the mock's capacity via /control/capacity, sleeps past the poll interval,
#      and asserts smg picked up the smaller total_tokens_capacity.
#
# Run from repo root:  bash e2e_test/thunder/test_phase6.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

MOCK_PORT=${MOCK_PORT:-8001}
SMG_PORT=${SMG_PORT:-30000}
CANNED_CONTENT='hello-from-mock-phase6'
INITIAL_BLOCKS=2048
INITIAL_BLOCK_TOKENS=16
SHRUNK_BLOCKS=64
MOCK_LOG=/tmp/thunder_phase6_mock.log
SMG_LOG=/tmp/thunder_phase6_smg.log

cleanup() {
    set +e
    if [[ -n "${SMG_PID:-}" ]]; then kill "$SMG_PID" 2>/dev/null; wait "$SMG_PID" 2>/dev/null; fi
    if [[ -n "${MOCK_PID:-}" ]]; then kill "$MOCK_PID" 2>/dev/null; wait "$MOCK_PID" 2>/dev/null; fi
}
trap cleanup EXIT

echo "== boot mock_vllm on :$MOCK_PORT =="
python3 e2e_test/thunder/mock_vllm.py --port "$MOCK_PORT" --canned-content "$CANNED_CONTENT" \
    --kv-cache-block-tokens "$INITIAL_BLOCK_TOKENS" \
    --num-kv-cache-blocks "$INITIAL_BLOCKS" \
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

# Wait an extra second to ensure the metrics poller has run at least once after startup.
sleep 1

METRICS_FILE=/tmp/thunder_phase6_metrics_cold.json
curl -sS -m 3 "http://localhost:$SMG_PORT/thunder/metrics" -o "$METRICS_FILE"
echo "cold /thunder/metrics:"
cat "$METRICS_FILE"
echo

EXPECTED_INITIAL_CAPACITY=$((INITIAL_BLOCKS * INITIAL_BLOCK_TOKENS))
python3 - "$METRICS_FILE" "$EXPECTED_INITIAL_CAPACITY" <<'PY'
import json
import sys

doc = json.load(open(sys.argv[1], encoding="utf-8"))
expected_capacity = int(sys.argv[2])

assert doc["program_count"] == 0, doc
assert len(doc["backends"]) == 1, doc
backend = doc["backends"][0]

assert backend["active_program_tokens"] == 0, backend
assert backend["reasoning_program_tokens"] == 0, backend
assert backend["acting_program_tokens"] == 0, backend
assert backend["active_program_count"] == 0, backend
assert backend["remaining_capacity"] == expected_capacity, backend
assert backend["buffer_per_program"] == 100, backend

metrics = backend["metrics"]
assert metrics["healthy"] is True, metrics
cache = metrics["cache_config"]
assert cache["total_tokens_capacity"] == expected_capacity, cache
PY

echo "== send a chat completion (program-A) =="
RESPONSE_FILE=/tmp/thunder_phase6_response.json
HTTP_CODE=$(curl -sS -m 10 -o "$RESPONSE_FILE" -w '%{http_code}' \
    -X POST "http://localhost:$SMG_PORT/v1/chat/completions" \
    -H 'content-type: application/json' \
    -d '{"model":"mock","program_id":"program-A","messages":[{"role":"user","content":"ping"}]}')
echo "HTTP $HTTP_CODE"
cat "$RESPONSE_FILE"
echo
[[ "$HTTP_CODE" == "200" ]] || { echo "FAIL: expected 200"; exit 1; }

METRICS_FILE=/tmp/thunder_phase6_metrics_after.json
curl -sS -m 3 "http://localhost:$SMG_PORT/thunder/metrics" -o "$METRICS_FILE"
echo "/thunder/metrics after one request:"
cat "$METRICS_FILE"
echo

python3 - "$METRICS_FILE" "$EXPECTED_INITIAL_CAPACITY" <<'PY'
import json
import sys

doc = json.load(open(sys.argv[1], encoding="utf-8"))
expected_capacity = int(sys.argv[2])

assert doc["program_count"] == 1, doc
backend = doc["backends"][0]

# program-A finished its request → status=ACTING, contributes via tool_coefficient=1.0
assert backend["acting_program_tokens"] > 0, backend
assert backend["active_program_count"] == 1, backend
assert backend["active_program_tokens"] == backend["acting_program_tokens"], backend

# Remaining capacity should drop by exactly active_tokens + 1 * BUFFER_PER_PROGRAM (100)
expected_remaining = expected_capacity - backend["active_program_tokens"] - 100
assert backend["remaining_capacity"] == expected_remaining, (backend, expected_remaining)
PY

echo "== shrink mock capacity via /control/capacity =="
curl -sS -m 3 -X POST "http://localhost:$MOCK_PORT/control/capacity" \
    -H 'content-type: application/json' \
    -d "{\"num_kv_cache_blocks\": $SHRUNK_BLOCKS}" >/dev/null

# Poll every 0.5s for up to 5s waiting for smg's 1s poller to refresh.
EXPECTED_SHRUNK_CAPACITY=$((SHRUNK_BLOCKS * INITIAL_BLOCK_TOKENS))
for i in $(seq 1 10); do
    METRICS_FILE=/tmp/thunder_phase6_metrics_shrunk.json
    curl -sS -m 3 "http://localhost:$SMG_PORT/thunder/metrics" -o "$METRICS_FILE"
    OBSERVED=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['backends'][0]['metrics']['cache_config']['total_tokens_capacity'])" "$METRICS_FILE")
    if [[ "$OBSERVED" == "$EXPECTED_SHRUNK_CAPACITY" ]]; then
        echo "smg picked up shrunk capacity ($OBSERVED) after ${i}*0.5s"
        break
    fi
    sleep 0.5
done

echo "/thunder/metrics after capacity shrink:"
cat "$METRICS_FILE"
echo

python3 - "$METRICS_FILE" "$EXPECTED_SHRUNK_CAPACITY" <<'PY'
import json
import sys

doc = json.load(open(sys.argv[1], encoding="utf-8"))
expected_capacity = int(sys.argv[2])
backend = doc["backends"][0]
cache = backend["metrics"]["cache_config"]
assert cache["total_tokens_capacity"] == expected_capacity, (cache, expected_capacity)
# remaining_capacity recomputes off the new capacity; should equal capacity - tokens - buffer.
expected_remaining = expected_capacity - backend["active_program_tokens"] - (backend["active_program_count"] * 100)
assert backend["remaining_capacity"] == expected_remaining, (backend, expected_remaining)
PY

echo
echo "PASS: Phase 6 e2e complete"
