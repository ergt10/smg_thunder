#!/usr/bin/env bash
# Phase 7 e2e: TR sub-mode capacity admission.
#
# Sets up a mock with very small total capacity, boots smg with --thunder-sub-mode tr,
# then:
#   1) Sends requests to fill the capacity with distinct program_ids.
#   2) Asserts a *new* program_id gets 503 once capacity is exhausted.
#   3) Asserts an *existing* program_id can still send a follow-up request (200).
#   4) Verifies that default mode (no --thunder-sub-mode tr) never 503s regardless of capacity.
#
# Run from repo root:  bash e2e_test/thunder/test_phase7.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

MOCK_PORT=${MOCK_PORT:-8001}
SMG_TR_PORT=${SMG_TR_PORT:-30001}
SMG_DEFAULT_PORT=${SMG_DEFAULT_PORT:-30002}
SMG_TR_PROM_PORT=${SMG_TR_PROM_PORT:-29001}
SMG_DEFAULT_PROM_PORT=${SMG_DEFAULT_PROM_PORT:-29002}
MOCK_LOG=/tmp/thunder_phase7_mock.log
SMG_TR_LOG=/tmp/thunder_phase7_smg_tr.log
SMG_DEFAULT_LOG=/tmp/thunder_phase7_smg_default.log

# Very small KV cache: 2 blocks × 16 tokens = 32 tokens total.
# BUFFER_PER_PROGRAM = 100 per program, so 1 program with >0 tokens saturates remaining_capacity.
# But we first need capacity > 100 for a program to be admitted, then fill it via token counts.
# Strategy: use 200 blocks × 16 = 3200 tokens, then fill with programs whose actual usage
# totals > 3200 - 100 = 3100, triggering exhaustion.
#
# Simpler approach: start with 8 blocks × 16 = 128 tokens.
# remaining_capacity = 128 - active_tokens - count*100
# After admitting 1 program:  remaining = 128 - (total_tokens) - 100
# mock returns total_tokens = 6 per request.  128 - 6 - 100 = 22 < 100 → next NEW program → 503.
BLOCK_TOKENS=16
NUM_BLOCKS=8   # total = 128 tokens

cleanup() {
    set +e
    if [[ -n "${SMG_TR_PID:-}" ]];      then kill "$SMG_TR_PID"      2>/dev/null; wait "$SMG_TR_PID"      2>/dev/null; fi
    if [[ -n "${SMG_DEFAULT_PID:-}" ]]; then kill "$SMG_DEFAULT_PID" 2>/dev/null; wait "$SMG_DEFAULT_PID" 2>/dev/null; fi
    if [[ -n "${MOCK_PID:-}" ]];        then kill "$MOCK_PID"         2>/dev/null; wait "$MOCK_PID"         2>/dev/null; fi
}
trap cleanup EXIT

echo "== boot mock_vllm on :$MOCK_PORT =="
python3 e2e_test/thunder/mock_vllm.py --port "$MOCK_PORT" \
    --kv-cache-block-tokens "$BLOCK_TOKENS" \
    --num-kv-cache-blocks "$NUM_BLOCKS" \
    > "$MOCK_LOG" 2>&1 &
MOCK_PID=$!
sleep 1
curl -sS -m 3 "http://localhost:$MOCK_PORT/health" >/dev/null || { echo "mock did not start"; cat "$MOCK_LOG"; exit 1; }

# ── TR mode smg ──────────────────────────────────────────────────────────────
echo "== boot smg TR mode on :$SMG_TR_PORT =="
cargo run --quiet --bin smg -- \
    --backend thunder \
    --worker-urls "http://localhost:$MOCK_PORT" \
    --port "$SMG_TR_PORT" \
    --prometheus-port "$SMG_TR_PROM_PORT" \
    --thunder-sub-mode tr \
    > "$SMG_TR_LOG" 2>&1 &
SMG_TR_PID=$!
for i in $(seq 1 30); do
    if curl -sS -m 1 "http://localhost:$SMG_TR_PORT/health" >/dev/null 2>&1; then break; fi
    sleep 1
done
curl -sS -m 3 "http://localhost:$SMG_TR_PORT/health" >/dev/null || {
    echo "smg TR did not become healthy"; tail -20 "$SMG_TR_LOG"; exit 1;
}

# ── default mode smg ─────────────────────────────────────────────────────────
echo "== boot smg default mode on :$SMG_DEFAULT_PORT =="
cargo run --quiet --bin smg -- \
    --backend thunder \
    --worker-urls "http://localhost:$MOCK_PORT" \
    --port "$SMG_DEFAULT_PORT" \
    --prometheus-port "$SMG_DEFAULT_PROM_PORT" \
    > "$SMG_DEFAULT_LOG" 2>&1 &
SMG_DEFAULT_PID=$!
for i in $(seq 1 30); do
    if curl -sS -m 1 "http://localhost:$SMG_DEFAULT_PORT/health" >/dev/null 2>&1; then break; fi
    sleep 1
done
curl -sS -m 3 "http://localhost:$SMG_DEFAULT_PORT/health" >/dev/null || {
    echo "smg default did not become healthy"; tail -20 "$SMG_DEFAULT_LOG"; exit 1;
}
sleep 1   # let metrics poller fetch initial cache config

echo ""
echo "=== TR mode tests ==="

# ── Phase 7 assertion 1: first program is admitted ───────────────────────────
echo "-- send program-alpha (should be 200) --"
CODE=$(curl -sS -m 10 -o /tmp/ph7_alpha.json -w '%{http_code}' \
    -X POST "http://localhost:$SMG_TR_PORT/v1/chat/completions" \
    -H 'content-type: application/json' \
    -d '{"model":"mock","program_id":"program-alpha","messages":[{"role":"user","content":"hi"}]}')
echo "HTTP $CODE"
cat /tmp/ph7_alpha.json; echo
[[ "$CODE" == "200" ]] || { echo "FAIL: expected 200 for first program, got $CODE"; exit 1; }

# ── Phase 7 assertion 2: second NEW program gets 503 (capacity exhausted) ────
# After program-alpha: active_tokens ≈ 6, count=1, remaining = 128 - 6 - 100 = 22 < 100.
echo "-- send program-beta (NEW program, should be 503 — capacity exhausted) --"
CODE=$(curl -sS -m 10 -o /tmp/ph7_beta.json -w '%{http_code}' \
    -X POST "http://localhost:$SMG_TR_PORT/v1/chat/completions" \
    -H 'content-type: application/json' \
    -d '{"model":"mock","program_id":"program-beta","messages":[{"role":"user","content":"hi"}]}')
echo "HTTP $CODE"
cat /tmp/ph7_beta.json; echo
[[ "$CODE" == "503" ]] || { echo "FAIL: expected 503 for new program when capacity full, got $CODE"; exit 1; }

# Verify error body contains our capacity_full code.
if ! grep -q "capacity_full" /tmp/ph7_beta.json; then
    echo "FAIL: 503 response does not contain 'capacity_full'"
    exit 1
fi

# ── Phase 7 assertion 3: existing program-alpha can still continue ────────────
echo "-- re-send program-alpha step 2 (existing program, should still be 200) --"
CODE=$(curl -sS -m 10 -o /tmp/ph7_alpha2.json -w '%{http_code}' \
    -X POST "http://localhost:$SMG_TR_PORT/v1/chat/completions" \
    -H 'content-type: application/json' \
    -d '{"model":"mock","program_id":"program-alpha","messages":[{"role":"user","content":"step2"}]}')
echo "HTTP $CODE"
cat /tmp/ph7_alpha2.json; echo
[[ "$CODE" == "200" ]] || { echo "FAIL: existing program should still get 200, got $CODE"; exit 1; }

# ── Check /programs state ─────────────────────────────────────────────────────
PROGRAMS=$(curl -sS -m 3 "http://localhost:$SMG_TR_PORT/programs")
echo "programs: $PROGRAMS"
python3 - "$PROGRAMS" <<'PY'
import json
import sys
programs = json.loads(sys.argv[1])
assert "program-alpha" in programs, programs
assert "program-beta" not in programs, "program-beta should not exist (was 503'd before create)"
assert programs["program-alpha"]["step_count"] == 2, programs
PY

echo ""
echo "=== Default mode tests ==="

# ── Phase 7 assertion 4: default mode never 503s regardless of capacity ───────
echo "-- default mode: program-gamma (should be 200 even when capacity would be full) --"
CODE=$(curl -sS -m 10 -o /tmp/ph7_gamma.json -w '%{http_code}' \
    -X POST "http://localhost:$SMG_DEFAULT_PORT/v1/chat/completions" \
    -H 'content-type: application/json' \
    -d '{"model":"mock","program_id":"program-gamma","messages":[{"role":"user","content":"hi"}]}')
echo "HTTP $CODE"
[[ "$CODE" == "200" ]] || { echo "FAIL: default mode should not 503, got $CODE"; exit 1; }

CODE=$(curl -sS -m 10 -o /tmp/ph7_delta.json -w '%{http_code}' \
    -X POST "http://localhost:$SMG_DEFAULT_PORT/v1/chat/completions" \
    -H 'content-type: application/json' \
    -d '{"model":"mock","program_id":"program-delta","messages":[{"role":"user","content":"hi"}]}')
echo "HTTP $CODE"
[[ "$CODE" == "200" ]] || { echo "FAIL: default mode should not 503 on second new program, got $CODE"; exit 1; }

echo ""
echo "PASS: Phase 7 e2e complete"
