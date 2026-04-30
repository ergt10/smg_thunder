//! `ThunderRouter` — program-aware proxy. Phase 3: non-streaming chat passthrough.
//!
//! Currently selects the first configured worker URL on every request (no load balancing,
//! no program tracking, no scheduling). Real backend selection arrives in Phase 6+.

use std::any::Any;
use std::sync::Arc;

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use openai_protocol::chat::ChatCompletionRequest;

use crate::app_context::AppContext;
use crate::middleware::TenantRequestMeta;
use crate::routers::error;

use super::proxy::forward_non_streaming_chat;

pub struct ThunderRouter {
    /// Worker URLs from `RoutingMode::Thunder { worker_urls }`. Phase 3 picks the first one
    /// for every request; Phase 6+ replaces this with capacity-aware selection.
    worker_urls: Vec<String>,
    /// Shared HTTP client (cloned from AppContext); reqwest::Client is internally Arc'd, so
    /// cloning is cheap and connection pooling is shared with the rest of smg.
    client: reqwest::Client,
}

impl std::fmt::Debug for ThunderRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThunderRouter")
            .field("worker_urls", &self.worker_urls)
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
}

#[async_trait::async_trait]
impl crate::routers::RouterTrait for ThunderRouter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn router_type(&self) -> &'static str {
        "thunder"
    }

    async fn route_chat(
        &self,
        _headers: Option<&HeaderMap>,
        _tenant_meta: &TenantRequestMeta,
        body: &ChatCompletionRequest,
        _model_id: &str,
    ) -> Response {
        // Streaming branch lands in Phase 4.
        if body.stream {
            return (
                StatusCode::NOT_IMPLEMENTED,
                "Thunder streaming chat completions not implemented yet (Phase 4)",
            )
                .into_response();
        }

        let Some(worker_url) = self.select_worker_url() else {
            return error::service_unavailable(
                "no_workers",
                "Thunder mode has no worker URLs configured",
            );
        };

        forward_non_streaming_chat(&self.client, worker_url, body).await
    }
}
