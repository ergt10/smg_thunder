//! `ThunderRouter` — program-aware proxy. Phase 7: TR sub-mode capacity admission.
//!
//! Phase 6 tracked per-backend capacity via `BackendState` + `VllmMetricsClient`. Phase 7 adds
//! a `--thunder-sub-mode tr` flag that enforces capacity admission on *new* programs:
//! if no backend has room, the request gets a 503 (pause/resume replaces that in Phase 8).
//!
//! Existing programs (already pinned to a backend) are never blocked — they continue on their
//! assigned backend regardless of the remaining_capacity snapshot (matches Python's behaviour).

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use axum::http::HeaderMap;
use axum::response::Response;
use axum::routing::get;
use axum::Json;
use axum::Router;
use openai_protocol::chat::ChatCompletionRequest;
use serde_json::{json, Value};

use crate::app_context::AppContext;
use crate::config::ThunderSubMode;
use crate::middleware::TenantRequestMeta;
use crate::routers::error;

use super::backend::{BackendState, BUFFER_PER_PROGRAM};
use super::metrics::{MetricsClient, VllmMetricsClient};
use super::program::{snapshot_programs, Program, ProgramRegistry};
use super::proxy::{forward_non_streaming_chat, forward_streaming_chat, StreamingFinishCallback};

/// Interval for the per-backend metrics polling loop. Kept tight enough for e2e tests to observe
/// dynamic capacity changes within a few seconds.
const METRICS_POLL_INTERVAL: Duration = Duration::from_millis(1000);

pub struct ThunderRouter {
    backends: Vec<Arc<BackendState>>,
    client: reqwest::Client,
    programs: ProgramRegistry,
    sub_mode: ThunderSubMode,
}

impl std::fmt::Debug for ThunderRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThunderRouter")
            .field(
                "backends",
                &self.backends.iter().map(|b| b.url()).collect::<Vec<_>>(),
            )
            .field("programs", &self.programs.len())
            .field("sub_mode", &self.sub_mode)
            .finish()
    }
}

impl ThunderRouter {
    pub async fn new(ctx: &Arc<AppContext>) -> Result<Self, String> {
        let (worker_urls, sub_mode) = match &ctx.router_config.mode {
            crate::config::RoutingMode::Thunder {
                worker_urls,
                sub_mode,
            } => (worker_urls.clone(), *sub_mode),
            other => {
                return Err(format!(
                    "ThunderRouter::new called with non-Thunder mode: {:?}",
                    other
                ))
            }
        };

        let client = ctx.client.clone();
        let mut backends = Vec::with_capacity(worker_urls.len());
        for url in &worker_urls {
            let metrics = Arc::new(VllmMetricsClient::new(url.clone(), client.clone()));
            let _ = metrics.fetch_cache_config().await;
            let backend = Arc::new(BackendState::new(url.clone(), metrics.clone()));
            spawn_metrics_poller(metrics);
            backends.push(backend);
        }

        Ok(Self {
            backends,
            client,
            programs: Arc::new(dashmap::DashMap::new()),
            sub_mode,
        })
    }

    pub fn worker_urls(&self) -> Vec<String> {
        self.backends.iter().map(|b| b.url().to_string()).collect()
    }

    /// Resolve the backend for this request according to the current sub-mode.
    ///
    /// - `Default`: always returns the first configured backend (Phase 6 behaviour).
    /// - `Tr` + existing program: returns the program's pinned backend (or first as fallback).
    /// - `Tr` + new program: runs capacity admission — returns `None` when no backend has room.
    fn resolve_backend(&self, program_id: &str) -> Option<Arc<BackendState>> {
        match self.sub_mode {
            ThunderSubMode::Default => self.backends.first().cloned(),
            ThunderSubMode::Tr => {
                if self.programs.contains_key(program_id) {
                    // Existing program: honour its pinned backend. Fall back to first on stale URL.
                    let backend_url: Option<String> = self
                        .programs
                        .get(program_id)
                        .and_then(|p| p.backend_url.clone());
                    if let Some(url) = backend_url {
                        if let Some(b) = self.backends.iter().find(|b| b.url() == url) {
                            return Some(Arc::clone(b));
                        }
                    }
                    self.backends.first().cloned()
                } else {
                    self.select_backend_for_new_program()
                }
            }
        }
    }

    /// TR-mode admission: pick the least-loaded backend that still has enough capacity for one
    /// new program (at least `BUFFER_PER_PROGRAM` remaining tokens after accounting for existing
    /// programs). Mirrors Python's `_select_backend_for_new_program` with `estimated_tokens = 0`
    /// (char/token ratio and pre-admission token estimation land in Phase 12).
    fn select_backend_for_new_program(&self) -> Option<Arc<BackendState>> {
        let mut best: Option<Arc<BackendState>> = None;
        let mut min_active: i64 = i64::MAX;

        for backend in &self.backends {
            // If cache config isn't available yet, treat as having capacity (Python parity).
            let remaining = match backend.remaining_capacity(&self.programs) {
                Some(r) => r,
                None => {
                    if best.is_none() {
                        best = Some(Arc::clone(backend));
                    }
                    continue;
                }
            };
            if remaining < BUFFER_PER_PROGRAM as i64 {
                continue; // Not enough headroom for this program's decode buffer
            }
            let active = backend.active_program_tokens(&self.programs) as i64;
            if active < min_active {
                min_active = active;
                best = Some(Arc::clone(backend));
            }
        }
        best
    }

    fn prepare_program(&self, program_id: &str, body: &ChatCompletionRequest, backend_url: &str) {
        let context_len = serde_json::to_vec(body).map_or(0, |bytes| bytes.len());
        let mut program = self
            .programs
            .entry(program_id.to_owned())
            .or_insert_with(|| Program::new(program_id));
        program.before_request(context_len, backend_url);
    }

    fn complete_program(programs: &ProgramRegistry, program_id: &str, total_tokens: Option<u64>) {
        if let Some(mut program) = programs.get_mut(program_id) {
            program.after_request(total_tokens);
        }
    }
}

fn spawn_metrics_poller(metrics: Arc<VllmMetricsClient>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(METRICS_POLL_INTERVAL);
        interval.tick().await; // skip first tick (initial fetch already done above)
        loop {
            interval.tick().await;
            let _ = metrics.fetch_cache_config().await;
        }
    });
}

fn program_id_from_request(body: &ChatCompletionRequest) -> String {
    body.other
        .get("program_id")
        .or_else(|| {
            body.other
                .get("extra_body")
                .and_then(Value::as_object)
                .and_then(|extra_body| extra_body.get("program_id"))
        })
        .map(value_to_program_id)
        .unwrap_or_else(|| "default".to_string())
}

fn value_to_program_id(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[async_trait::async_trait]
impl crate::routers::RouterTrait for ThunderRouter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn router_type(&self) -> &'static str {
        "thunder"
    }

    fn extra_routes(&self) -> Router<Arc<crate::server::AppState>> {
        let programs = Arc::clone(&self.programs);
        let backends = self.backends.clone();
        Router::new()
            .route(
                "/programs",
                get({
                    let programs = Arc::clone(&programs);
                    move || {
                        let programs = Arc::clone(&programs);
                        async move { Json(snapshot_programs(&programs)) }
                    }
                }),
            )
            .route(
                "/thunder/metrics",
                get({
                    let programs = Arc::clone(&programs);
                    let backends = backends.clone();
                    move || {
                        let programs = Arc::clone(&programs);
                        let backends = backends.clone();
                        async move {
                            let snapshot: Vec<_> =
                                backends.iter().map(|b| b.snapshot(&programs)).collect();
                            Json(json!({
                                "program_count": programs.len(),
                                "backends": snapshot,
                            }))
                        }
                    }
                }),
            )
    }

    async fn route_chat(
        &self,
        _headers: Option<&HeaderMap>,
        _tenant_meta: &TenantRequestMeta,
        body: &ChatCompletionRequest,
        _model_id: &str,
    ) -> Response {
        let program_id = program_id_from_request(body);

        if self.backends.is_empty() {
            return error::service_unavailable(
                "no_workers",
                "Thunder mode has no worker URLs configured",
            );
        }

        let backend = match self.resolve_backend(&program_id) {
            Some(b) => b,
            None => {
                return error::service_unavailable(
                    "capacity_full",
                    "Thunder TR mode: all backends are at capacity; retry later (pause/resume arrives in Phase 8)",
                )
            }
        };


        let backend_url = backend.url().to_string();
        self.prepare_program(&program_id, body, &backend_url);

        if body.stream {
            let programs = Arc::clone(&self.programs);
            let on_finish: StreamingFinishCallback = Box::new(move |total_tokens| {
                Self::complete_program(&programs, &program_id, total_tokens);
            });
            return forward_streaming_chat(&self.client, &backend_url, body, Some(on_finish))
                .await;
        }

        let forwarded = forward_non_streaming_chat(&self.client, &backend_url, body).await;
        Self::complete_program(&self.programs, &program_id, forwarded.total_tokens);
        forwarded.response
    }
}
