#!/usr/bin/env bash
# Phase 3 e2e: non-streaming chat passthrough through ThunderRouter.
#
# Boots mock_vllm.py on :8001, smg in thunder mode on :30000, then POSTs a chat
# completion and asserts the response body matches the mock's canned content.
#
# Run from repo root:  bash e2e_test/thunder/test_phase3.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

MOCK_PORT=${MOCK_PORT:-8001}
SMG_PORT=${SMG_PORT:-30000}
CANNED_CONTENT='hello-from-mock-phase3'
MOCK_LOG=/tmp/thunder_phase3_mock.log
SMG_LOG=/tmp/thunder_phase3_smg.log

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
# smg startup needs a moment (cargo may rebuild even with --quiet)
for i in $(seq 1 30); do
    if curl -sS -m 1 "http://localhost:$SMG_PORT/health" >/dev/null 2>&1; then break; fi
    sleep 1
done
curl -sS -m 3 "http://localhost:$SMG_PORT/health" >/dev/null || {
    echo "smg did not become healthy"; tail -20 "$SMG_LOG"; exit 1;
}

echo "== POST /v1/chat/completions through smg =="
RESPONSE_FILE=/tmp/thunder_phase3_response.json
HTTP_CODE=$(curl -sS -m 10 -o "$RESPONSE_FILE" -w '%{http_code}' \
    -X POST "http://localhost:$SMG_PORT/v1/chat/completions" \
    -H 'content-type: application/json' \
    -d '{"model":"mock","messages":[{"role":"user","content":"ping"}]}')

echo "HTTP $HTTP_CODE"
echo "response:"
cat "$RESPONSE_FILE"
echo

if [[ "$HTTP_CODE" != "200" ]]; then
    echo "FAIL: expected HTTP 200, got $HTTP_CODE"
    exit 1
fi

# Assert canned content survived the round-trip.
if ! grep -q "$CANNED_CONTENT" "$RESPONSE_FILE"; then
    echo "FAIL: response body does not contain canned content '$CANNED_CONTENT'"
    exit 1
fi

# Assert OpenAI shape: id, object, choices, usage all present.
for field in '"id"' '"object"' '"choices"' '"usage"'; do
    if ! grep -q "$field" "$RESPONSE_FILE"; then
        echo "FAIL: response missing $field"
        exit 1
    fi
done

# Assert mock saw exactly 1 request (proves smg actually forwarded).
MOCK_STATE=$(curl -sS -m 3 "http://localhost:$MOCK_PORT/control/state")
echo "mock state: $MOCK_STATE"
if ! echo "$MOCK_STATE" | grep -q '"request_count": 1'; then
    echo "FAIL: mock did not see exactly 1 request"
    exit 1
fi

echo
echo "PASS: Phase 3 e2e complete"
