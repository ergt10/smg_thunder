//! `ThunderRouter` — program-aware proxy. Phase 8: TR pause/resume scheduling.
//!
//! Default mode remains a transparent proxy. TR mode queues new programs when capacity is full,
//! then a periodic scheduler resumes them when backend capacity becomes available.

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::Path;
use axum::http::HeaderMap;
use axum::response::Response;
use axum::routing::get;
use axum::Json;
use axum::Router;
use openai_protocol::chat::ChatCompletionRequest;
use serde_json::{json, Value};

use crate::app_context::AppContext;
use crate::config::{ThunderBackendType, ThunderSubMode};
use crate::middleware::TenantRequestMeta;
use crate::routers::error;

use super::backend::{BackendState, BUFFER_PER_PROGRAM};
use super::metrics::{MetricsClient, SglangMetricsClient, SkyrlMetricsClient, VllmMetricsClient};
use super::profile::{ProfileRegistry, ProfileState};
use super::program::{snapshot_programs, Program, ProgramRegistry};
use super::proxy::{
    forward_non_streaming_chat, forward_streaming_chat, StreamingFinishCallback,
    StreamingFirstTokenCallback, StreamingProgressCallback, UsageTokens,
};
use super::scheduler::{wait_for_resume_or_force, SchedulerState};

/// Interval for the per-backend metrics polling loop. Kept tight enough for e2e tests to observe
/// dynamic capacity changes within a few seconds.
const METRICS_POLL_INTERVAL: Duration = Duration::from_millis(1000);
const SCHEDULER_INTERVAL: Duration = Duration::from_millis(500);
const FORCE_RESUME_TIMEOUT: Duration = Duration::from_secs(30 * 60);

pub struct ThunderRouter {
    backends: Vec<Arc<BackendState>>,
    client: reqwest::Client,
    programs: ProgramRegistry,
    profiles: ProfileRegistry,
    profile_enabled: bool,
    sub_mode: ThunderSubMode,
    scheduler: Option<SchedulerState>,
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
        let (worker_urls, sub_mode, backend_type, profile_enabled) = match &ctx.router_config.mode {
            crate::config::RoutingMode::Thunder {
                worker_urls,
                sub_mode,
                backend_type,
                profile,
            } => (worker_urls.clone(), *sub_mode, *backend_type, *profile),
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
            let metrics: Arc<dyn MetricsClient> = match backend_type {
                ThunderBackendType::Vllm => {
                    Arc::new(VllmMetricsClient::new(url.clone(), client.clone()))
                }
                ThunderBackendType::Sglang => {
                    Arc::new(SglangMetricsClient::new(url.clone(), client.clone()))
                }
                ThunderBackendType::Skyrl => {
                    Arc::new(SkyrlMetricsClient::new(url.clone(), client.clone()))
                }
            };
            let _ = metrics.fetch_cache_config().await;
            let backend = Arc::new(BackendState::new(url.clone(), metrics.clone()));
            spawn_metrics_poller(metrics);
            backends.push(backend);
        }

        let programs = Arc::new(dashmap::DashMap::new());
        let profiles = Arc::new(dashmap::DashMap::new());
        let scheduler = (sub_mode == ThunderSubMode::Tr).then(|| {
            let scheduler =
                SchedulerState::new(backends.clone(), Arc::clone(&programs), SCHEDULER_INTERVAL);
            scheduler.spawn();
            scheduler
        });

        Ok(Self {
            backends,
            client,
            programs,
            profiles,
            profile_enabled,
            sub_mode,
            scheduler,
        })
    }

    pub fn worker_urls(&self) -> Vec<String> {
        self.backends.iter().map(|b| b.url().to_string()).collect()
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

    async fn admit_program(
        &self,
        program_id: &str,
        body: &ChatCompletionRequest,
    ) -> Option<Arc<BackendState>> {
        match self.sub_mode {
            ThunderSubMode::Default => {
                let backend = self.backends.first().cloned()?;
                self.prepare_program(program_id, body, backend.url());
                Some(backend)
            }
            ThunderSubMode::Tr => self.admit_program_tr(program_id, body).await,
        }
    }

    async fn admit_program_tr(
        &self,
        program_id: &str,
        body: &ChatCompletionRequest,
    ) -> Option<Arc<BackendState>> {
        if let Some(url) = self
            .programs
            .get(program_id)
            .and_then(|program| program.backend_url.clone())
        {
            let backend = self
                .backends
                .iter()
                .find(|backend| backend.url() == url)
                .cloned()
                .or_else(|| self.backends.first().cloned())?;
            self.prepare_program(program_id, body, backend.url());
            return Some(backend);
        }

        if let Some(notify) = self
            .programs
            .get(program_id)
            .and_then(|program| program.waiting_notify.clone())
        {
            self.mark_waiting_request(program_id, body);
            let scheduler = self.scheduler.clone()?;
            wait_for_resume_or_force(
                notify,
                scheduler,
                program_id.to_string(),
                FORCE_RESUME_TIMEOUT,
            )
            .await;
            return self.backend_for_program(program_id);
        }

        if let Some(backend) = self.select_backend_for_new_program() {
            self.prepare_program(program_id, body, backend.url());
            return Some(backend);
        }

        let notify = self.pause_new_program(program_id, body);
        let scheduler = self.scheduler.clone()?;
        wait_for_resume_or_force(
            notify,
            scheduler,
            program_id.to_string(),
            FORCE_RESUME_TIMEOUT,
        )
        .await;
        self.backend_for_program(program_id)
    }

    fn backend_for_program(&self, program_id: &str) -> Option<Arc<BackendState>> {
        let url = self
            .programs
            .get(program_id)
            .and_then(|program| program.backend_url.clone())?;
        self.backends
            .iter()
            .find(|backend| backend.url() == url)
            .cloned()
    }

    fn prepare_program(&self, program_id: &str, body: &ChatCompletionRequest, backend_url: &str) {
        let context_len = serde_json::to_vec(body).map_or(0, |bytes| bytes.len());
        let mut program = self
            .programs
            .entry(program_id.to_owned())
            .or_insert_with(|| Program::new(program_id));
        program.before_request(context_len, backend_url);
    }

    fn mark_waiting_request(&self, program_id: &str, body: &ChatCompletionRequest) {
        let context_len = serde_json::to_vec(body).map_or(0, |bytes| bytes.len());
        if let Some(mut program) = self.programs.get_mut(program_id) {
            program.before_waiting_request(context_len);
        }
    }

    fn pause_new_program(
        &self,
        program_id: &str,
        body: &ChatCompletionRequest,
    ) -> Arc<tokio::sync::Notify> {
        let context_len = serde_json::to_vec(body).map_or(0, |bytes| bytes.len());
        let mut program = Program::new(program_id);
        program.before_waiting_request(context_len);
        let notify = program.pause(None);
        self.programs.insert(program_id.to_owned(), program);
        notify
    }

    fn complete_program(programs: &ProgramRegistry, program_id: &str, total_tokens: Option<u64>) {
        if let Some(mut program) = programs.get_mut(program_id) {
            program.after_request(total_tokens);
        }
    }

    fn update_streaming_tokens(programs: &ProgramRegistry, program_id: &str, delta_tokens: u64) {
        if let Some(mut program) = programs.get_mut(program_id) {
            program.update_streaming_tokens(delta_tokens);
        }
    }

    fn profile_arrive(&self, program_id: &str) {
        if !self.profile_enabled {
            return;
        }
        let mut profile = self
            .profiles
            .entry(program_id.to_owned())
            .or_insert_with(|| ProfileState::new(program_id));
        profile.on_request_arrive();
    }

    fn profile_start(&self, program_id: &str) {
        if !self.profile_enabled {
            return;
        }
        if let Some(mut profile) = self.profiles.get_mut(program_id) {
            profile.on_request_start();
        }
    }

    fn profile_first_token(profiles: &ProfileRegistry, program_id: &str) {
        if let Some(mut profile) = profiles.get_mut(program_id) {
            profile.on_first_token();
        }
    }

    fn profile_tokens(profiles: &ProfileRegistry, program_id: &str, delta_tokens: u64) {
        if let Some(mut profile) = profiles.get_mut(program_id) {
            profile.on_token(delta_tokens);
        }
    }

    fn profile_end(profiles: &ProfileRegistry, program_id: &str, usage: Option<UsageTokens>) {
        if let Some(mut profile) = profiles.get_mut(program_id) {
            profile.on_request_end(usage);
        }
    }
}

fn spawn_metrics_poller(metrics: Arc<dyn MetricsClient>) {
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
        let profiles = Arc::clone(&self.profiles);
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
            .route(
                "/profiles",
                get({
                    let profiles = Arc::clone(&profiles);
                    move || {
                        let profiles = Arc::clone(&profiles);
                        async move {
                            let snapshot: std::collections::BTreeMap<_, _> = profiles
                                .iter()
                                .map(|entry| {
                                    let profile = entry.value();
                                    (
                                        entry.key().clone(),
                                        json!({
                                            "program_id": profile.program_id.clone(),
                                            "request_arrive_ms": profile.request_arrive_ms,
                                            "request_start_ms": profile.request_start_ms,
                                            "first_token_ms": profile.first_token_ms,
                                            "request_end_ms": profile.request_end_ms,
                                            "first_token_time_ms": profile.first_token_time_ms(),
                                            "decode_time_ms": profile.decode_time_ms(),
                                            "token_count": profile.token_count,
                                            "prompt_tokens": profile.prompt_tokens,
                                            "completion_tokens": profile.completion_tokens,
                                            "cached_tokens": profile.cached_tokens,
                                        }),
                                    )
                                })
                                .collect();
                            Json(snapshot)
                        }
                    }
                }),
            )
            .route(
                "/profiles/{program_id}",
                get({
                    let profiles = Arc::clone(&profiles);
                    move |Path(program_id): Path<String>| {
                        let profiles = Arc::clone(&profiles);
                        async move {
                            let payload = profiles
                                .get(&program_id)
                                .map(|profile| {
                                    json!({
                                        "program_id": profile.program_id.clone(),
                                        "request_arrive_ms": profile.request_arrive_ms,
                                        "request_start_ms": profile.request_start_ms,
                                        "first_token_ms": profile.first_token_ms,
                                        "request_end_ms": profile.request_end_ms,
                                        "first_token_time_ms": profile.first_token_time_ms(),
                                        "decode_time_ms": profile.decode_time_ms(),
                                        "token_count": profile.token_count,
                                        "prompt_tokens": profile.prompt_tokens,
                                        "completion_tokens": profile.completion_tokens,
                                        "cached_tokens": profile.cached_tokens,
                                    })
                                })
                                .unwrap_or(Value::Null);
                            Json(payload)
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
        self.profile_arrive(&program_id);

        if self.backends.is_empty() {
            return error::service_unavailable(
                "no_workers",
                "Thunder mode has no worker URLs configured",
            );
        }

        let backend = match self.admit_program(&program_id, body).await {
            Some(b) => b,
            None => {
                return error::service_unavailable(
                    "no_workers",
                    "Thunder mode has no worker URLs configured",
                )
            }
        };

        let backend_url = backend.url().to_string();
        self.profile_start(&program_id);

        if body.stream {
            let programs = Arc::clone(&self.programs);
            let finish_profiles = Arc::clone(&self.profiles);
            let finish_program_id = program_id.clone();
            let first_token_profiles = Arc::clone(&self.profiles);
            let first_token_program_id = program_id.clone();
            let progress_profiles = Arc::clone(&self.profiles);
            let progress_programs = Arc::clone(&self.programs);
            let progress_program_id = program_id.clone();
            let profile_enabled = self.profile_enabled;
            let on_finish: StreamingFinishCallback = Box::new(move |usage| {
                Self::complete_program(
                    &programs,
                    &program_id,
                    usage.and_then(|usage| usage.total_tokens()),
                );
                if profile_enabled {
                    Self::profile_end(&finish_profiles, &finish_program_id, usage);
                }
            });
            let on_first_token: Option<StreamingFirstTokenCallback> =
                self.profile_enabled.then(|| {
                    Box::new(move || {
                        Self::profile_first_token(&first_token_profiles, &first_token_program_id);
                    }) as StreamingFirstTokenCallback
                });
            let profile_enabled = self.profile_enabled;
            let on_progress: StreamingProgressCallback = Box::new(move |delta_tokens| {
                Self::update_streaming_tokens(
                    &progress_programs,
                    &progress_program_id,
                    delta_tokens,
                );
                if profile_enabled {
                    Self::profile_tokens(&progress_profiles, &progress_program_id, delta_tokens);
                }
            });
            return forward_streaming_chat(
                &self.client,
                &backend_url,
                body,
                Some(on_finish),
                on_first_token,
                Some(on_progress),
            )
            .await;
        }

        let forwarded = forward_non_streaming_chat(&self.client, &backend_url, body).await;
        Self::complete_program(
            &self.programs,
            &program_id,
            forwarded.usage.and_then(|usage| usage.total_tokens()),
        );
        if self.profile_enabled {
            Self::profile_end(&self.profiles, &program_id, forwarded.usage);
        }
        forwarded.response
    }
}
