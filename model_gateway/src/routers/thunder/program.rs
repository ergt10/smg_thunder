//! Program state tracked by ThunderRouter.

use std::{collections::BTreeMap, sync::Arc, time::SystemTime};

use dashmap::DashMap;
use serde::Serialize;
use tokio::sync::Notify;

pub type ProgramRegistry = Arc<DashMap<String, Program>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ProgramStatus {
    Reasoning,
    Acting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ProgramState {
    Active,
    Paused,
    Terminated,
}

#[derive(Debug, Clone, Serialize)]
pub struct Program {
    pub program_id: String,
    pub status: ProgramStatus,
    pub state: ProgramState,
    pub context_len: usize,
    pub total_tokens: u64,
    pub step_count: u64,
    /// Backend URL this program is currently bound to (set when the router admits the request
    /// in Phase 6+). `None` until first request is dispatched, after which it persists across
    /// retries — Python pins programs to one backend for prefix-cache locality.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin_backend: Option<String>,
    #[serde(skip)]
    pub waiting_notify: Option<Arc<Notify>>,
    pub marked_for_pause: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paused_at_ms: Option<u128>,
}

impl Program {
    pub fn new(program_id: impl Into<String>) -> Self {
        Self {
            program_id: program_id.into(),
            status: ProgramStatus::Reasoning,
            state: ProgramState::Active,
            context_len: 0,
            total_tokens: 0,
            step_count: 0,
            backend_url: None,
            origin_backend: None,
            waiting_notify: None,
            marked_for_pause: false,
            paused_at_ms: None,
        }
    }

    pub fn before_request(&mut self, context_len: usize, backend_url: &str) {
        self.step_count += 1;
        self.context_len = context_len;
        self.status = ProgramStatus::Reasoning;
        self.state = ProgramState::Active;
        if self.backend_url.is_none() {
            self.backend_url = Some(backend_url.to_owned());
        }
    }

    pub fn before_waiting_request(&mut self, context_len: usize) {
        self.step_count += 1;
        self.context_len = context_len;
        self.status = ProgramStatus::Reasoning;
    }

    pub fn pause(&mut self, origin_backend: Option<String>) -> Arc<Notify> {
        self.origin_backend = origin_backend;
        self.backend_url = None;
        self.state = ProgramState::Paused;
        self.paused_at_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .map(|d| d.as_millis());
        let notify = Arc::new(Notify::new());
        self.waiting_notify = Some(Arc::clone(&notify));
        notify
    }

    pub fn resume(&mut self, backend_url: &str) -> Option<Arc<Notify>> {
        self.backend_url = Some(backend_url.to_owned());
        self.origin_backend = None;
        self.state = ProgramState::Active;
        self.paused_at_ms = None;
        self.waiting_notify.take()
    }

    pub fn after_request(&mut self, total_tokens: Option<u64>) {
        if let Some(total_tokens) = total_tokens {
            self.total_tokens = total_tokens;
        }
        self.status = ProgramStatus::Acting;
    }

    pub fn update_streaming_tokens(&mut self, delta_tokens: u64) {
        self.total_tokens = self.total_tokens.saturating_add(delta_tokens);
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProgramSnapshot {
    pub context_len: usize,
    pub total_tokens: u64,
    pub step_count: u64,
    pub status: ProgramStatus,
    pub state: ProgramState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin_backend: Option<String>,
    pub marked_for_pause: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paused_at_ms: Option<u128>,
}

impl From<&Program> for ProgramSnapshot {
    fn from(program: &Program) -> Self {
        Self {
            context_len: program.context_len,
            total_tokens: program.total_tokens,
            step_count: program.step_count,
            status: program.status,
            state: program.state,
            backend_url: program.backend_url.clone(),
            origin_backend: program.origin_backend.clone(),
            marked_for_pause: program.marked_for_pause,
            paused_at_ms: program.paused_at_ms,
        }
    }
}

pub fn snapshot_programs(programs: &ProgramRegistry) -> BTreeMap<String, ProgramSnapshot> {
    programs
        .iter()
        .map(|entry| (entry.key().clone(), ProgramSnapshot::from(entry.value())))
        .collect()
}
