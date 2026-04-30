#!/usr/bin/env bash
# Phase 4 e2e: streaming SSE passthrough through ThunderRouter.
#
# Boots mock_vllm.py on :8001, smg in thunder mode on :30000, then POSTs a
# streaming chat completion and asserts the SSE frames arrive unchanged enough
# for clients to consume them.
#
# Run from repo root:  bash e2e_test/thunder/test_phase4.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

MOCK_PORT=${MOCK_PORT:-8001}
SMG_PORT=${SMG_PORT:-30000}
CANNED_CONTENT='hello-from-mock-phase4-streaming'
MOCK_LOG=/tmp/thunder_phase4_mock.log
SMG_LOG=/tmp/thunder_phase4_smg.log

cleanup() {
    set +e
    if [[ -n "${SMG_PID:-}" ]]; then kill "$SMG_PID" 2>/dev/null; wait "$SMG_PID" 2>/dev/null; fi
    if [[ -n "${MOCK_PID:-}" ]]; then kill "$MOCK_PID" 2>/dev/null; wait "$MOCK_PID" 2>/dev/null; fi
}
trap cleanup EXIT

echo "== boot mock_vllm on :$MOCK_PORT =="
python3 e2e_test/thunder/mock_vllm.py --port "$MOCK_PORT" \
    --canned-content "$CANNED_CONTENT" \
    --stream-chunk-count 4 \
    --stream-delay-ms 10 \
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

echo "== POST streaming /v1/chat/completions through smg =="
RESPONSE_FILE=/tmp/thunder_phase4_stream.sse
HEADER_FILE=/tmp/thunder_phase4_headers.txt
HTTP_CODE=$(curl -sS -N -m 10 -D "$HEADER_FILE" -o "$RESPONSE_FILE" -w '%{http_code}' \
    -X POST "http://localhost:$SMG_PORT/v1/chat/completions" \
    -H 'content-type: application/json' \
    -d '{"model":"mock","stream":true,"messages":[{"role":"user","content":"ping"}]}')

echo "HTTP $HTTP_CODE"
echo "headers:"
cat "$HEADER_FILE"
echo "stream:"
cat "$RESPONSE_FILE"
echo

if [[ "$HTTP_CODE" != "200" ]]; then
    echo "FAIL: expected HTTP 200, got $HTTP_CODE"
    exit 1
fi

if ! grep -qi '^content-type: text/event-stream' "$HEADER_FILE"; then
    echo "FAIL: response is not text/event-stream"
    exit 1
fi

DATA_CHUNKS=$(grep -c '^data:' "$RESPONSE_FILE" || true)
if (( DATA_CHUNKS < 2 )); then
    echo "FAIL: expected at least 2 SSE data chunks, got $DATA_CHUNKS"
    exit 1
fi

STREAMED_CONTENT=$(python3 - "$RESPONSE_FILE" <<'PY'
import json
import sys

content = []
for raw in open(sys.argv[1], encoding="utf-8"):
    raw = raw.strip()
    if not raw.startswith("data: "):
        continue
    data = raw[len("data: "):]
    if data == "[DONE]":
        continue
    payload = json.loads(data)
    for choice in payload.get("choices", []):
        delta_content = choice.get("delta", {}).get("content")
        if isinstance(delta_content, str):
            content.append(delta_content)
print("".join(content))
PY
)
if [[ "$STREAMED_CONTENT" != "$CANNED_CONTENT" ]]; then
    echo "FAIL: streamed content mismatch"
    echo "expected: $CANNED_CONTENT"
    echo "actual:   $STREAMED_CONTENT"
    exit 1
fi

if ! grep -q 'data: \[DONE\]' "$RESPONSE_FILE"; then
    echo "FAIL: streamed response missing [DONE]"
    exit 1
fi

MOCK_STATE=$(curl -sS -m 3 "http://localhost:$MOCK_PORT/control/state")
echo "mock state: $MOCK_STATE"
if ! echo "$MOCK_STATE" | grep -q '"request_count": 1'; then
    echo "FAIL: mock did not see exactly 1 request"
    exit 1
fi

echo
echo "PASS: Phase 4 e2e complete"
