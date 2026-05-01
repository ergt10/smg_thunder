#!/usr/bin/env bash
# Phase 11 e2e: Thunder profiling endpoint records streaming timings.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

MOCK_PORT=${MOCK_PORT:-8001}
SMG_PORT=${SMG_PORT:-30000}
PROM_PORT=${SMG_PROM_PORT:-29000}
MOCK_LOG=/tmp/thunder_phase11_mock.log
SMG_LOG=/tmp/thunder_phase11_smg.log
STREAM_FILE=/tmp/thunder_phase11_stream.sse

cleanup() {
    set +e
    if [[ -n "${SMG_PID:-}" ]]; then kill "$SMG_PID" 2>/dev/null; wait "$SMG_PID" 2>/dev/null; fi
    if [[ -n "${MOCK_PID:-}" ]]; then kill "$MOCK_PID" 2>/dev/null; wait "$MOCK_PID" 2>/dev/null; fi
}
trap cleanup EXIT

wait_http() {
    local url=$1
    for _ in $(seq 1 40); do
        if curl -sS -m 1 "$url" >/dev/null 2>&1; then return 0; fi
        sleep 0.25
    done
    return 1
}

python3 e2e_test/thunder/mock_vllm.py --port "$MOCK_PORT" \
    --canned-content "profile-token-stream profile-token-stream profile-token-stream profile-token-stream" \
    --stream-chunk-count 8 \
    --stream-delay-ms 40 \
    > "$MOCK_LOG" 2>&1 &
MOCK_PID=$!
wait_http "http://localhost:$MOCK_PORT/health" || { cat "$MOCK_LOG"; exit 1; }

SMG_BIN=target/debug/smg
if [[ -x "$SMG_BIN" ]]; then
    "$SMG_BIN" --backend thunder --worker-urls "http://localhost:$MOCK_PORT" \
        --port "$SMG_PORT" --prometheus-port "$PROM_PORT" --profile \
        > "$SMG_LOG" 2>&1 &
else
    cargo run --quiet --bin smg -- --backend thunder --worker-urls "http://localhost:$MOCK_PORT" \
        --port "$SMG_PORT" --prometheus-port "$PROM_PORT" --profile \
        > "$SMG_LOG" 2>&1 &
fi
SMG_PID=$!
wait_http "http://localhost:$SMG_PORT/health" || { tail -40 "$SMG_LOG"; exit 1; }

curl -sS -N -m 10 -o "$STREAM_FILE" \
    -X POST "http://localhost:$SMG_PORT/v1/chat/completions" \
    -H 'content-type: application/json' \
    -d '{"model":"mock","stream":true,"program_id":"phase11-profile","messages":[{"role":"user","content":"profile this"}]}'

curl -sS -m 3 "http://localhost:$SMG_PORT/profiles/phase11-profile" | python3 -c 'import json,sys
p=json.load(sys.stdin)
assert p["request_arrive_ms"], p
assert p["request_start_ms"], p
assert p["first_token_ms"], p
assert p["request_end_ms"], p
assert p["first_token_time_ms"] is not None, p
assert p["decode_time_ms"] is not None and p["decode_time_ms"] > 0, p
assert p["prompt_tokens"] == 1, p
assert p["completion_tokens"] > 0, p
assert p["token_count"] == p["prompt_tokens"] + p["completion_tokens"], p
'

echo "PASS: Phase 11 e2e complete"
