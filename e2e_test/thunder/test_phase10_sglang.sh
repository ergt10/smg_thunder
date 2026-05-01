#!/usr/bin/env bash
# Phase 10 e2e: Thunder SGLang backend metrics dialect.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

MOCK_PORT=${MOCK_PORT:-8001}
SMG_PORT=${SMG_PORT:-30000}
PROM_PORT=${SMG_PROM_PORT:-29000}
MOCK_LOG=/tmp/thunder_phase10_sglang_mock.log
SMG_LOG=/tmp/thunder_phase10_sglang_smg.log

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
    --kv-cache-block-tokens 16 --num-kv-cache-blocks 32 \
    > "$MOCK_LOG" 2>&1 &
MOCK_PID=$!
wait_http "http://localhost:$MOCK_PORT/health" || { cat "$MOCK_LOG"; exit 1; }

SMG_BIN=target/debug/smg
if [[ -x "$SMG_BIN" ]]; then
    "$SMG_BIN" --backend thunder --worker-urls "http://localhost:$MOCK_PORT" \
        --port "$SMG_PORT" --prometheus-port "$PROM_PORT" \
        --thunder-backend-type sglang > "$SMG_LOG" 2>&1 &
else
    cargo run --quiet --bin smg -- --backend thunder --worker-urls "http://localhost:$MOCK_PORT" \
        --port "$SMG_PORT" --prometheus-port "$PROM_PORT" \
        --thunder-backend-type sglang > "$SMG_LOG" 2>&1 &
fi
SMG_PID=$!
wait_http "http://localhost:$SMG_PORT/health" || { tail -40 "$SMG_LOG"; exit 1; }

curl -sS -m 3 -X POST "http://localhost:$SMG_PORT/v1/chat/completions" \
    -H 'content-type: application/json' \
    -d '{"model":"mock","program_id":"phase10-sglang","messages":[{"role":"user","content":"hi"}]}' >/dev/null

curl -sS -m 3 "http://localhost:$SMG_PORT/thunder/metrics" | python3 -c 'import json,sys
m=json.load(sys.stdin)
backend=m["backends"][0]
assert backend["metrics"]["backend_type"] == "sglang", backend
assert backend["metrics"]["healthy"] is True, backend
assert backend["metrics"]["cache_config"]["total_tokens_capacity"] == 512, backend
'

echo "PASS: Phase 10 SGLang e2e complete"
