//! Program state tracked by ThunderRouter.

use std::collections::BTreeMap;
use std::sync::Arc;

use dashmap::DashMap;
use serde::Serialize;

pub type ProgramRegistry = Arc<DashMap<String, Program>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ProgramStatus {
    Reasoning,
    Acting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[expect(
    dead_code,
    reason = "pause/resume lifecycle states are introduced before scheduler phases construct them"
)]
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

    pub fn after_request(&mut self, total_tokens: Option<u64>) {
        if let Some(total_tokens) = total_tokens {
            self.total_tokens = total_tokens;
        }
        self.status = ProgramStatus::Acting;
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
        }
    }
}

pub fn snapshot_programs(programs: &ProgramRegistry) -> BTreeMap<String, ProgramSnapshot> {
    programs
        .iter()
        .map(|entry| (entry.key().clone(), ProgramSnapshot::from(entry.value())))
        .collect()
}
