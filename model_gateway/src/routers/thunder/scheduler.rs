//! Pause/resume scheduler for Thunder TR mode.

use std::{sync::Arc, time::Duration};

use tokio::sync::Notify;
use tracing::debug;

use super::{
    backend::{BackendState, BUFFER_PER_PROGRAM},
    program::{ProgramRegistry, ProgramState, ProgramStatus},
};

#[derive(Debug, Clone)]
pub struct SchedulerState {
    backends: Arc<Vec<Arc<BackendState>>>,
    programs: ProgramRegistry,
    interval: Duration,
}

impl SchedulerState {
    pub fn new(
        backends: Vec<Arc<BackendState>>,
        programs: ProgramRegistry,
        interval: Duration,
    ) -> Self {
        Self {
            backends: Arc::new(backends),
            programs,
            interval,
        }
    }

    pub fn spawn(&self) {
        let scheduler = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(scheduler.interval);
            loop {
                interval.tick().await;
                scheduler.greedy_resume();
            }
        });
    }

    pub fn greedy_resume(&self) {
        let mut backend_caps = self.backend_capacities();
        if backend_caps.is_empty() {
            return;
        }

        let total_capacity: i64 = backend_caps.iter().map(|(_, cap)| *cap).sum();
        if total_capacity <= 0 {
            return;
        }

        let mut candidates = self.paused_candidates();
        if candidates.is_empty() {
            return;
        }

        candidates.sort_by(|a, b| {
            a.priority
                .cmp(&b.priority)
                .then(a.total_tokens.cmp(&b.total_tokens))
        });

        let mut selected = Vec::new();
        let mut cumulative = 0_i64;
        for candidate in candidates {
            let required = candidate.required_tokens();
            if cumulative + required <= total_capacity {
                cumulative += required;
                selected.push(candidate);
            }
        }
        if selected.is_empty() {
            return;
        }

        selected.sort_by(|a, b| b.total_tokens.cmp(&a.total_tokens));
        backend_caps.sort_by(|a, b| b.1.cmp(&a.1));

        for candidate in selected {
            if backend_caps.is_empty() {
                break;
            }

            let required = candidate.required_tokens();
            let Some(idx) = backend_caps
                .iter()
                .position(|(_, remaining)| *remaining >= required)
            else {
                continue;
            };

            let backend = Arc::clone(&backend_caps[idx].0);
            if self.resume_program(&candidate.program_id, backend.url()) {
                backend_caps[idx].1 -= required;
                backend_caps.retain(|(_, remaining)| *remaining >= BUFFER_PER_PROGRAM as i64);
                backend_caps.sort_by(|a, b| b.1.cmp(&a.1));
            }
        }
    }

    fn backend_capacities(&self) -> Vec<(Arc<BackendState>, i64)> {
        self.backends
            .iter()
            .filter(|backend| backend.healthy())
            .filter_map(|backend| {
                backend
                    .remaining_capacity_with_decay(&self.programs)
                    .filter(|remaining| *remaining >= BUFFER_PER_PROGRAM as i64)
                    .map(|remaining| (Arc::clone(backend), remaining))
            })
            .collect()
    }

    fn paused_candidates(&self) -> Vec<ResumeCandidate> {
        self.programs
            .iter()
            .filter_map(|entry| {
                let program = entry.value();
                if program.state != ProgramState::Paused {
                    return None;
                }
                let priority = if program.step_count == 1 {
                    1
                } else if program.status == ProgramStatus::Reasoning {
                    0
                } else {
                    2
                };
                Some(ResumeCandidate {
                    program_id: entry.key().clone(),
                    total_tokens: program.total_tokens,
                    priority,
                })
            })
            .collect()
    }

    pub fn resume_program(&self, program_id: &str, backend_url: &str) -> bool {
        let notify = {
            let Some(mut program) = self.programs.get_mut(program_id) else {
                return false;
            };
            if program.state != ProgramState::Paused {
                return false;
            }
            program.resume(backend_url)
        };

        if let Some(notify) = notify {
            notify.notify_waiters();
        }
        debug!(program_id, backend_url, "resumed paused Thunder program");
        true
    }

    pub fn first_backend_url(&self) -> Option<String> {
        self.backends.first().map(|b| b.url().to_string())
    }
}

#[derive(Debug)]
struct ResumeCandidate {
    program_id: String,
    total_tokens: u64,
    priority: u8,
}

impl ResumeCandidate {
    fn required_tokens(&self) -> i64 {
        self.total_tokens.saturating_add(BUFFER_PER_PROGRAM) as i64
    }
}

pub async fn wait_for_resume_or_force(
    notify: Arc<Notify>,
    scheduler: SchedulerState,
    program_id: String,
    timeout: Duration,
) {
    if tokio::time::timeout(timeout, notify.notified())
        .await
        .is_ok()
    {
        return;
    }

    if let Some(url) = scheduler.first_backend_url() {
        scheduler.resume_program(&program_id, &url);
    }
}
