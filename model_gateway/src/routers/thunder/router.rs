//! `ThunderRouter` — program-aware proxy. Phase 5: default-mode program tracking.
//!
//! Currently selects the first configured worker URL on every request (no load balancing and no
//! scheduling). Capacity-aware backend state arrives in Phase 6+.

use std::any::Any;
use std::sync::Arc;

use axum::http::HeaderMap;
use axum::response::Response;
use axum::routing::get;
use axum::Json;
use axum::Router;
use openai_protocol::chat::ChatCompletionRequest;
use serde_json::Value;

use crate::app_context::AppContext;
use crate::middleware::TenantRequestMeta;
use crate::routers::error;

use super::program::{snapshot_programs, Program, ProgramRegistry};
use super::proxy::{forward_non_streaming_chat, forward_streaming_chat, StreamingFinishCallback};

pub struct ThunderRouter {
    /// Worker URLs from `RoutingMode::Thunder { worker_urls }`. Phase 3 picks the first one
    /// for every request; Phase 6+ replaces this with capacity-aware selection.
    worker_urls: Vec<String>,
    /// Shared HTTP client (cloned from AppContext); reqwest::Client is internally Arc'd, so
    /// cloning is cheap and connection pooling is shared with the rest of smg.
    client: reqwest::Client,
    programs: ProgramRegistry,
}

impl std::fmt::Debug for ThunderRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThunderRouter")
            .field("worker_urls", &self.worker_urls)
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
        Ok(Self {
            worker_urls,
            client: ctx.client.clone(),
            programs: Arc::new(dashmap::DashMap::new()),
        })
    }

    pub fn worker_urls(&self) -> &[String] {
        &self.worker_urls
    }

    /// Pick the upstream worker for this request.
    ///
    /// Phase 3: always the first configured URL. Replaced by capacity-aware selection in
    /// Phase 6 (`BackendState`) and pause/resume scheduling in Phase 8.
    fn select_worker_url(&self) -> Option<&str> {
        self.worker_urls.first().map(String::as_str)
    }

    fn prepare_program(&self, program_id: &str, body: &ChatCompletionRequest) {
        let context_len = serde_json::to_vec(body).map_or(0, |bytes| bytes.len());
        let mut program = self
            .programs
            .entry(program_id.to_owned())
            .or_insert_with(|| Program::new(program_id));
        program.before_request(context_len);
    }

    fn complete_program(programs: &ProgramRegistry, program_id: &str, total_tokens: Option<u64>) {
        if let Some(mut program) = programs.get_mut(program_id) {
            program.after_request(total_tokens);
        }
    }
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
        Router::new().route(
            "/programs",
            get(move || {
                let programs = Arc::clone(&programs);
                async move { Json(snapshot_programs(&programs)) }
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
        let Some(worker_url) = self.select_worker_url() else {
            return error::service_unavailable(
                "no_workers",
                "Thunder mode has no worker URLs configured",
            );
        };
        let program_id = program_id_from_request(body);
        self.prepare_program(&program_id, body);

        if body.stream {
            let programs = Arc::clone(&self.programs);
            let on_finish: StreamingFinishCallback = Box::new(move |total_tokens| {
                Self::complete_program(&programs, &program_id, total_tokens);
            });
            return forward_streaming_chat(&self.client, worker_url, body, Some(on_finish)).await;
        }

        let forwarded = forward_non_streaming_chat(&self.client, worker_url, body).await;
        Self::complete_program(&self.programs, &program_id, forwarded.total_tokens);
        forwarded.response
    }
}
