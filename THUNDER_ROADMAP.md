# ThunderAgent on smg — Implementation Roadmap

Porting the Python [ThunderAgent](https://github.com/HaoKang-Timmy/ThunderAgent) (~2.8k LOC, program-aware OpenAI proxy with capacity-based pause/resume scheduling) into smg as a new `RoutingMode::Thunder` variant.

**Branch**: `feat/thunder`
**Python reference impl**: `/home/ergt/thunder_reconstruct/ThunderAgent/ThunderAgent/` — read this for behavior; do not modify.

---

## Architecture decision

`RoutingMode::Thunder` enum variant + `ThunderRouter` impl `RouterTrait`, all under `model_gateway/src/routers/thunder/`. Touches a small surface of existing files (enum arm in `config/types.rs`, match arm in `routers/factory.rs`, `pub mod thunder` in `routers/mod.rs`, CLI flag) but the bulk of the code is a new self-contained module — no existing logic is rewritten.

Considered alternatives:
- **Custom `LoadBalancingPolicy` + axum middleware**: rejected — streaming token progress callbacks would force middleware to wrap response body and reparse SSE, defeating the reuse benefit.
- **Standalone Cargo project depending on `openai-protocol`**: rejected — user prefers integrated smg mode over separate binary.

---

## Status

| # | Phase | Status | Commit |
|---|---|---|---|
| 0 | Env baseline (`cargo build` + `cargo test` clean) | ✅ Done | _no commit_ (env only; 3365 tests pass, 0 failed) |
| 1 | Empty `RoutingMode::Thunder` wired into factory (501 stub) | ✅ Done | `734dbbec` |
| 2 | Mock vLLM backend (`e2e_test/mock_vllm.py`) | ⬜ Not started | — |
| 3 | Non-streaming chat passthrough | ⬜ Not started | — |
| 4 | Streaming (SSE) chat passthrough | ⬜ Not started | — |
| 5 | Program state + `/programs` endpoint (default mode) | ⬜ Not started | — |
| 6 | vLLM metrics client + `BackendState` capacity + `/thunder/metrics` | ⬜ Not started | — |
| 7 | TR sub-mode capacity admission (503 on full, no pause yet) | ⬜ Not started | — |
| 8 | Pause/Resume scheduler + BFD greedy resume + 30-min timeout | ⬜ Not started | — |
| 9 | Streaming token progress callback (every 20 tokens) | ⬜ Not started | — |
| 10 | SGLang + SkyRL backend support | ⬜ Not started | — |
| 11 | Profiling (`--profile`, `/profiles` endpoint) | ⬜ Not started | — |
| 12 | char/token ratio + acting-token decay/weight polish | ⬜ Not started | — |

Mark a row ✅ and fill the commit hash after each phase commit lands on `feat/thunder`.

---

## Per-phase contract

Every phase commit must satisfy:

1. `cargo build --workspace` passes
2. `cargo test --workspace` passes (no regression in existing smg tests)
3. A runnable e2e validation under `e2e_test/` — **not just unit tests**. Reproducible with copy-paste shell/python commands.
4. Exactly one git commit on `feat/thunder` with message `feat(thunder): <phase summary>` (or `test(thunder): ...` for Phase 2).

---

## Phase details

### Phase 0 — Env baseline
Establish reproducible baseline so any later breakage is attributable. No code changes, no commit.

**Validation**:
```bash
cd smg/
cargo build --workspace
cargo test --workspace
```
Both must succeed before moving on.

### Phase 1 — Empty `RoutingMode::Thunder`
Add the routing mode enum variant; create `routers/thunder/{mod,router}.rs` with empty `ThunderRouter` impl `RouterTrait` returning `NOT_IMPLEMENTED` for every endpoint. Wire into `RouterFactory::create_router`. Add CLI flag.

**Files touched**:
- `model_gateway/src/config/types.rs` — `RoutingMode::Thunder { worker_urls }` arm
- `model_gateway/src/routers/factory.rs` — match arm
- `model_gateway/src/routers/mod.rs` — `pub mod thunder;`
- `model_gateway/src/routers/thunder/mod.rs` — new
- `model_gateway/src/routers/thunder/router.rs` — new (empty `ThunderRouter`)
- CLI parsing in `main.rs` / config builder

**Validation**:
```bash
smg --routing-mode thunder --worker-urls http://localhost:8001 &
curl http://localhost:30000/health           # → 200 ok
curl -X POST http://localhost:30000/v1/chat/completions -d '{}'  # → 501
```

### Phase 2 — Mock vLLM backend
Self-contained Python aiohttp server mocking vLLM's chat completions (streaming + non-streaming) and `/get_server_info`. No GPU required.

**Files**:
- `e2e_test/mock_vllm.py` — new

**Validation**:
```bash
python3 e2e_test/mock_vllm.py --port 8001 &
curl -X POST http://localhost:8001/v1/chat/completions \
     -H 'content-type: application/json' \
     -d '{"messages":[{"role":"user","content":"hi"}]}'
# → returns canned chat completion JSON
```

### Phase 3 — Non-streaming chat passthrough
`ThunderRouter::route_chat` non-streaming path forwards request body to first worker URL via reqwest, returns response unchanged. No state tracking.

**Validation**:
```bash
bash e2e_test/test_phase3.sh
# Starts mock + smg, posts chat completion, asserts response matches mock canned reply
```

### Phase 4 — Streaming (SSE) passthrough
Streaming branch forwards SSE chunks unchanged.

**Validation**:
```bash
bash e2e_test/test_phase4.sh
# curl --no-buffer asserts ≥2 `data:` chunks received
```

### Phase 5 — Program state
`Program` struct (`program_id`, `status: REASONING|ACTING`, `state: ACTIVE|PAUSED|TERMINATED`, `total_tokens`, `step_count`) in `routers/thunder/program.rs`. DashMap registry on `ThunderRouter`. Extract `program_id` from request body (top-level or `extra_body`). Transition `REASONING → ACTING` after response. Expose `/programs` via new `RouterTrait::extra_routes()` default method (returns empty axum Router by default; `ThunderRouter` overrides).

**Validation**:
```bash
bash e2e_test/test_phase5.sh
# Sends 2 distinct program_ids → /programs lists both, step_count=1 each
# Resends program_id=A → /programs shows A.step_count=2
```

### Phase 6 — vLLM metrics + capacity
`BackendState` in `routers/thunder/backend.rs` tracks per-backend tokens. `VLLMMetricsClient` in `routers/thunder/metrics/vllm.rs` polls `/get_server_info`. Compute `active_program_tokens`, `remaining_capacity`. New `/thunder/metrics` endpoint.

**Validation**:
```bash
bash e2e_test/test_phase6.sh
# Mock returns vLLM-shaped cache config (total_kv_cache_tokens=N)
# After sending requests, /thunder/metrics shows correct accumulation
```

### Phase 7 — TR sub-mode admission
Add `--thunder-sub-mode tr|default`. In `tr` mode: `_select_backend_for_new_program` picks least-loaded with capacity; if none has capacity → return **503** (real pause/resume comes in Phase 8).

**Validation**:
```bash
bash e2e_test/test_phase7.sh
# Mock with low capacity → fill → next request returns 503
```

### Phase 8 — Pause/Resume scheduler
Replace 503 with `tokio::sync::Notify`-based wait. Periodic scheduler loop runs **BFD (Best Fit Decreasing)** greedy resume from Python's `_greedy_resume`. Priority: REASONING > new programs > ACTING. 30-min force-resume timeout via `tokio::time::timeout`.

**Files**:
- `routers/thunder/scheduler.rs` — periodic task + BFD logic

**Validation**:
```bash
python3 e2e_test/test_phase8.py
# Mock exposes /control/capacity to dynamically adjust reported capacity
# 1) Fill capacity
# 2) Send extra request → pauses (asserts via /programs status)
# 3) Free capacity via /control/capacity
# 4) Assert paused request resumes within scheduler_interval seconds
```

### Phase 9 — Streaming token progress
During SSE streaming, count tokens, update `Program.total_tokens` every 20 tokens.

**Validation**:
```bash
bash e2e_test/test_phase9.sh
# Mock returns long stream slowly; poll /programs mid-stream; assert total_tokens grows
```

### Phase 10 — SGLang + SkyRL backends
`MetricsClient` trait in `routers/thunder/metrics/mod.rs`; `sglang.rs` + `skyrl.rs` impls. CLI flag `--thunder-backend-type vllm|sglang|skyrl`.

**Validation**:
```bash
bash e2e_test/test_phase10_sglang.sh
bash e2e_test/test_phase10_skyrl.sh
```

### Phase 11 — Profiling
`ProfileState` in `routers/thunder/profile.rs`. `--profile` flag. `/profiles` and `/profiles/{id}` endpoints. Track `on_request_arrive`, `on_request_start` (after pause), `on_first_token`, `on_token`, `on_request_end` with `prompt_tokens`/`completion_tokens`/`cached_tokens`.

**Validation**:
```bash
bash e2e_test/test_phase11.sh
# Enable --profile, send streaming request, /profiles returns non-zero first_token_time and decode_time
```

### Phase 12 — Polish
- Global `char_to_token_ratio` with momentum update (0.2 new + 0.8 old)
- `--acting-token-weight` (default 1.0)
- `--use-acting-token-decay` (2^-t weighting in `remaining_capacity_with_decay`)

**Validation**: parametrized capacity calc unit tests + e2e sample with non-default weights.

---

## How to pick up mid-roadmap

1. `git -C smg log --oneline feat/thunder` — commits = phases completed
2. Update the status table above with the latest done phase
3. Re-read the next phase's "Files touched" and "Validation" sections
4. The Python source under `ThunderAgent/ThunderAgent/<module>/` is the canonical behavior reference

## Quick links to Python source (behavior reference)

| Concept | Python file |
|---|---|
| HTTP app + endpoints | `app.py` |
| Config + CLI | `config.py`, `__main__.py` |
| Router + scheduler + BFD | `scheduler/router.py` |
| HTTP forwarding (streaming/non) | `scheduler/vllm_request_processor.py` |
| Program state machine | `program/state.py` |
| Per-backend capacity tracking | `backend/state.py` |
| vLLM/SGLang/SkyRL metrics | `backend/{vllm,sglang,skyrl}_metrics.py` |
| Profiling | `profile/state.py` |
