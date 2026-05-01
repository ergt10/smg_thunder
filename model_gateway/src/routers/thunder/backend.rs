//! Per-backend state: capacity calculations + metrics handle.
//!
//! Ports `ThunderAgent/backend/state.py` minus pause/resume scheduling (those land in Phase 7+).
//! Phase 6 only computes the observable capacity numbers — `active_program_tokens`,
//! `remaining_capacity`, etc. — from the global `ProgramRegistry` filtered by `backend_url`.

use std::{sync::Arc, time::SystemTime};

use serde_json::{json, Value};

use super::metrics::{CacheConfig, MetricsClient};
use super::program::{ProgramRegistry, ProgramStatus};

/// Decode-headroom buffer reserved per active program (Python: `BUFFER_PER_PROGRAM = 100`).
pub const BUFFER_PER_PROGRAM: u64 = 100;

#[derive(Debug)]
pub struct BackendState {
    url: String,
    metrics: Arc<dyn MetricsClient>,
    tool_coefficient: f64,
    use_acting_token_decay: bool,
}

impl BackendState {
    pub fn with_options(
        url: impl Into<String>,
        metrics: Arc<dyn MetricsClient>,
        tool_coefficient: f64,
        use_acting_token_decay: bool,
    ) -> Self {
        Self {
            url: url.into(),
            metrics,
            tool_coefficient,
            use_acting_token_decay,
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn cache_config(&self) -> Option<CacheConfig> {
        self.metrics.cache_config()
    }

    /// Reserved for Phase 7+ admission logic / scheduler health gates.
    #[allow(dead_code)]
    pub fn healthy(&self) -> bool {
        self.metrics.healthy()
    }

    /// Iterate token totals for programs assigned to this backend that match `predicate`.
    fn fold_programs<F>(&self, programs: &ProgramRegistry, mut predicate: F) -> u64
    where
        F: FnMut(ProgramStatus) -> bool,
    {
        programs
            .iter()
            .filter(|entry| {
                entry.value().backend_url.as_deref() == Some(&self.url)
                    && predicate(entry.value().status)
            })
            .map(|entry| entry.value().total_tokens)
            .sum()
    }

    pub fn reasoning_program_tokens(&self, programs: &ProgramRegistry) -> u64 {
        self.fold_programs(programs, |status| status == ProgramStatus::Reasoning)
    }

    pub fn acting_program_tokens(&self, programs: &ProgramRegistry) -> u64 {
        self.fold_programs(programs, |status| status == ProgramStatus::Acting)
    }

    /// `reasoning_tokens + tool_coefficient * acting_tokens` (Python `active_program_tokens`).
    pub fn active_program_tokens(&self, programs: &ProgramRegistry) -> u64 {
        let reasoning = self.reasoning_program_tokens(programs) as f64;
        let acting = self.acting_program_tokens(programs) as f64;
        (reasoning + self.tool_coefficient * acting) as u64
    }

    pub fn active_program_tokens_with_decay(&self, programs: &ProgramRegistry) -> u64 {
        if !self.use_acting_token_decay {
            return self.active_program_tokens(programs);
        }
        let now_ms = now_ms();
        let reasoning = self.reasoning_program_tokens(programs) as f64;
        let acting = programs
            .iter()
            .filter(|entry| {
                entry.value().backend_url.as_deref() == Some(&self.url)
                    && entry.value().status == ProgramStatus::Acting
            })
            .map(|entry| {
                let elapsed_minutes = entry
                    .value()
                    .acting_since_ms
                    .and_then(|since| now_ms.checked_sub(since))
                    .map(|elapsed_ms| elapsed_ms as f64 / 60_000.0)
                    .unwrap_or_default();
                let decay = 2.0_f64.powf(-elapsed_minutes);
                entry.value().total_tokens as f64 * decay
            })
            .sum::<f64>();
        (reasoning + self.tool_coefficient * acting) as u64
    }

    pub fn active_program_count(&self, programs: &ProgramRegistry) -> u64 {
        programs
            .iter()
            .filter(|entry| entry.value().backend_url.as_deref() == Some(&self.url))
            .count() as u64
    }

    /// Tokens still available for new programs after the per-program decode buffer.
    /// Returns `None` when the cache config has not been fetched yet (treat as "unknown" —
    /// Phase 7 admission falls back to allowing new programs in this case, matching Python).
    pub fn remaining_capacity(&self, programs: &ProgramRegistry) -> Option<i64> {
        let cfg = self.cache_config()?;
        let capacity = cfg.total_tokens_capacity() as i64;
        let used = self.active_program_tokens(programs) as i64;
        let buffer = (self.active_program_count(programs) * BUFFER_PER_PROGRAM) as i64;
        Some(capacity - used - buffer)
    }

    pub fn remaining_capacity_with_decay(&self, programs: &ProgramRegistry) -> Option<i64> {
        let cfg = self.cache_config()?;
        let capacity = cfg.total_tokens_capacity() as i64;
        let used = self.active_program_tokens_with_decay(programs) as i64;
        let buffer = (self.active_program_count(programs) * BUFFER_PER_PROGRAM) as i64;
        Some(capacity - used - buffer)
    }

    pub fn snapshot(&self, programs: &ProgramRegistry) -> Value {
        json!({
            "url": self.url,
            "active_program_tokens": self.active_program_tokens(programs),
            "active_program_tokens_with_decay": self.active_program_tokens_with_decay(programs),
            "reasoning_program_tokens": self.reasoning_program_tokens(programs),
            "acting_program_tokens": self.acting_program_tokens(programs),
            "active_program_count": self.active_program_count(programs),
            "remaining_capacity": self.remaining_capacity(programs),
            "remaining_capacity_with_decay": self.remaining_capacity_with_decay(programs),
            "buffer_per_program": BUFFER_PER_PROGRAM,
            "tool_coefficient": self.tool_coefficient,
            "use_acting_token_decay": self.use_acting_token_decay,
            "metrics": self.metrics.snapshot(),
        })
    }
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_millis())
        .unwrap_or_default()
}
