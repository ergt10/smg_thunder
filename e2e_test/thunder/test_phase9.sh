#!/usr/bin/env bash
# Phase 9 e2e: streaming token progress updates Program.total_tokens mid-stream.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

MOCK_PORT=${MOCK_PORT:-8001}
SMG_PORT=${SMG_PORT:-30000}
PROM_PORT=${SMG_PROM_PORT:-29000}
MOCK_LOG=/tmp/thunder_phase9_mock.log
SMG_LOG=/tmp/thunder_phase9_smg.log
STREAM_FILE=/tmp/thunder_phase9_stream.sse
CANNED_CONTENT="$(python3 - <<'PY'
print("stream-progress-token " * 80)
PY
)"

cleanup() {
    set +e
    if [[ -n "${CURL_PID:-}" ]]; then kill "$CURL_PID" 2>/dev/null; wait "$CURL_PID" 2>/dev/null; fi
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

echo "== boot mock_vllm on :$MOCK_PORT =="
python3 e2e_test/thunder/mock_vllm.py --port "$MOCK_PORT" \
    --canned-content "$CANNED_CONTENT" \
    --stream-chunk-count 80 \
    --stream-delay-ms 30 \
    > "$MOCK_LOG" 2>&1 &
MOCK_PID=$!
wait_http "http://localhost:$MOCK_PORT/health" || { echo "mock did not start"; cat "$MOCK_LOG"; exit 1; }

echo "== boot smg in thunder mode on :$SMG_PORT =="
SMG_BIN=target/debug/smg
if [[ -x "$SMG_BIN" ]]; then
    "$SMG_BIN" --backend thunder \
        --worker-urls "http://localhost:$MOCK_PORT" \
        --port "$SMG_PORT" \
        --prometheus-port "$PROM_PORT" > "$SMG_LOG" 2>&1 &
else
    cargo run --quiet --bin smg -- --backend thunder \
        --worker-urls "http://localhost:$MOCK_PORT" \
        --port "$SMG_PORT" \
        --prometheus-port "$PROM_PORT" > "$SMG_LOG" 2>&1 &
fi
SMG_PID=$!
wait_http "http://localhost:$SMG_PORT/health" || { echo "smg did not start"; tail -40 "$SMG_LOG"; exit 1; }

echo "== start long streaming request =="
curl -sS -N -m 15 -o "$STREAM_FILE" \
    -X POST "http://localhost:$SMG_PORT/v1/chat/completions" \
    -H 'content-type: application/json' \
    -d '{"model":"mock","stream":true,"program_id":"phase9-stream","messages":[{"role":"user","content":"stream slowly"}]}' &
CURL_PID=$!

FIRST_TOKENS=0
SECOND_TOKENS=0
for _ in $(seq 1 80); do
    TOKENS="$(curl -sS -m 1 "http://localhost:$SMG_PORT/programs" | python3 -c 'import json,sys
try:
    programs=json.load(sys.stdin)
    print(programs.get("phase9-stream",{}).get("total_tokens",0))
except Exception:
    print(0)
')"
    if (( TOKENS >= 20 )); then
        FIRST_TOKENS=$TOKENS
        break
    fi
    sleep 0.1
done

if (( FIRST_TOKENS < 20 )); then
    echo "FAIL: total_tokens did not grow mid-stream"
    curl -sS -m 2 "http://localhost:$SMG_PORT/programs" || true
    exit 1
fi

for _ in $(seq 1 80); do
    TOKENS="$(curl -sS -m 1 "http://localhost:$SMG_PORT/programs" | python3 -c 'import json,sys
try:
    programs=json.load(sys.stdin)
    print(programs.get("phase9-stream",{}).get("total_tokens",0))
except Exception:
    print(0)
')"
    if (( TOKENS > FIRST_TOKENS )); then
        SECOND_TOKENS=$TOKENS
        break
    fi
    sleep 0.1
done

if (( SECOND_TOKENS <= FIRST_TOKENS )); then
    echo "FAIL: total_tokens did not continue growing"
    exit 1
fi

wait "$CURL_PID"
unset CURL_PID

FINAL_TOKENS="$(curl -sS -m 2 "http://localhost:$SMG_PORT/programs" | python3 -c 'import json,sys
programs=json.load(sys.stdin)
print(programs["phase9-stream"]["total_tokens"])
')"

if (( FINAL_TOKENS < SECOND_TOKENS )); then
    echo "FAIL: final total_tokens regressed from $SECOND_TOKENS to $FINAL_TOKENS"
    exit 1
fi

echo "PASS: Phase 9 e2e complete (tokens: $FIRST_TOKENS -> $SECOND_TOKENS -> $FINAL_TOKENS)"
