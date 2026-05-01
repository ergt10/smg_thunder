#!/usr/bin/env bash
# Phase 12 e2e: char/token ratio and acting-token weight/decay metrics.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

MOCK_PORT=${MOCK_PORT:-8001}
SMG_PORT=${SMG_PORT:-30000}
PROM_PORT=${SMG_PROM_PORT:-29000}
MOCK_LOG=/tmp/thunder_phase12_mock.log
SMG_LOG=/tmp/thunder_phase12_smg.log
METRICS_FILE=/tmp/thunder_phase12_metrics.json

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
    --kv-cache-block-tokens 16 --num-kv-cache-blocks 64 \
    > "$MOCK_LOG" 2>&1 &
MOCK_PID=$!
wait_http "http://localhost:$MOCK_PORT/health" || { cat "$MOCK_LOG"; exit 1; }

SMG_BIN=target/debug/smg
if [[ -x "$SMG_BIN" ]]; then
    "$SMG_BIN" --backend thunder --worker-urls "http://localhost:$MOCK_PORT" \
        --port "$SMG_PORT" --prometheus-port "$PROM_PORT" \
        --acting-token-weight 0.5 --use-acting-token-decay \
        > "$SMG_LOG" 2>&1 &
else
    cargo run --quiet --bin smg -- --backend thunder --worker-urls "http://localhost:$MOCK_PORT" \
        --port "$SMG_PORT" --prometheus-port "$PROM_PORT" \
        --acting-token-weight 0.5 --use-acting-token-decay \
        > "$SMG_LOG" 2>&1 &
fi
SMG_PID=$!
wait_http "http://localhost:$SMG_PORT/health" || { tail -40 "$SMG_LOG"; exit 1; }

BEFORE_RATIO="$(curl -sS -m 2 "http://localhost:$SMG_PORT/thunder/metrics" | python3 -c 'import json,sys; print(json.load(sys.stdin)["char_to_token_ratio"])')"

curl -sS -m 5 -X POST "http://localhost:$SMG_PORT/v1/chat/completions" \
    -H 'content-type: application/json' \
    -d '{"model":"mock","program_id":"phase12-polish","messages":[{"role":"user","content":"phase twelve prompt with enough characters to update ratio"}]}' >/dev/null

curl -sS -m 2 "http://localhost:$SMG_PORT/thunder/metrics" > "$METRICS_FILE"
python3 - "$BEFORE_RATIO" "$METRICS_FILE" <<'PY'
import json, sys
before = float(sys.argv[1])
m = json.load(open(sys.argv[2], encoding="utf-8"))
backend = m["backends"][0]
assert m["char_to_token_ratio"] != before, m
assert backend["tool_coefficient"] == 0.5, backend
assert backend["use_acting_token_decay"] is True, backend
assert "active_program_tokens_with_decay" in backend, backend
assert "remaining_capacity_with_decay" in backend, backend
assert backend["remaining_capacity_with_decay"] is not None, backend
PY

echo "PASS: Phase 12 e2e complete"
