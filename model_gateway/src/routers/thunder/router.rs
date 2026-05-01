//! `ThunderRouter` — program-aware proxy. Phase 6: per-backend metrics + capacity tracking.
//!
//! Phase 3-5 only used a flat list of worker URLs. Phase 6 introduces `BackendState` per worker
//! and spawns one background task per backend that polls vLLM's `/get_server_info` to refresh
//! the cache config. Programs are now pinned to a backend on first dispatch (Python parity).

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
use crate::middleware::TenantRequestMeta;
use crate::routers::error;

use super::backend::BackendState;
use super::metrics::{MetricsClient, VllmMetricsClient};
use super::program::{snapshot_programs, Program, ProgramRegistry};
use super::proxy::{forward_non_streaming_chat, forward_streaming_chat, StreamingFinishCallback};

/// Interval for the per-backend metrics polling loop. Kept tight enough for e2e tests to observe
/// dynamic capacity changes within a few seconds.
const METRICS_POLL_INTERVAL: Duration = Duration::from_millis(1000);

pub struct ThunderRouter {
    /// Per-backend state (one entry per `--worker-urls`). Phase 6+ replaces ad-hoc worker URL
    /// lookups with capacity-aware backend selection.
    backends: Vec<Arc<BackendState>>,
    /// Shared HTTP client (cloned from AppContext); reqwest::Client is internally Arc'd, so
    /// cloning is cheap and connection pooling is shared with the rest of smg.
    client: reqwest::Client,
    programs: ProgramRegistry,
}

impl std::fmt::Debug for ThunderRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThunderRouter")
            .field(
                "backends",
                &self
                    .backends
                    .iter()
                    .map(|b| b.url())
                    .collect::<Vec<_>>(),
            )
            .field("programs", &self.programs.len())
            .finish()
    }
}

impl ThunderRouter {
    pub async fn new(ctx: &Arc<AppContext>) -> Result<Self, String> {
        let worker_urls = match &ctx.router_config.mode {
            crate::config::RoutingMode::Thunder { worker_urls } => worker_urls.clone(),
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
            // Best-effort: try to fetch the cache config once before serving. If the upstream
            // is not yet up the periodic poller will retry every second.
            let _ = metrics.fetch_cache_config().await;
            let backend = Arc::new(BackendState::new(url.clone(), metrics.clone()));
            spawn_metrics_poller(metrics);
            backends.push(backend);
        }

        Ok(Self {
            backends,
            client,
            programs: Arc::new(dashmap::DashMap::new()),
        })
    }

    pub fn worker_urls(&self) -> Vec<String> {
        self.backends.iter().map(|b| b.url().to_string()).collect()
    }

    /// Pick the upstream backend for this request.
    ///
    /// Phase 6: still always the first configured backend. Capacity-aware admission lands in
    /// Phase 7; pause/resume scheduling in Phase 8.
    fn select_backend(&self) -> Option<&Arc<BackendState>> {
        self.backends.first()
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
        // First tick fires immediately; we already did one fetch above, so skip it to avoid
        // double-polling at startup.
        interval.tick().await;
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
        let Some(backend) = self.select_backend() else {
            return error::service_unavailable(
                "no_workers",
                "Thunder mode has no worker URLs configured",
            );
        };
        let backend_url = backend.url().to_string();
        let program_id = program_id_from_request(body);
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

