//! Per-program profiling state for Thunder.

use std::time::SystemTime;

use serde::Serialize;

use super::proxy::UsageTokens;

pub type ProfileRegistry = std::sync::Arc<dashmap::DashMap<String, ProfileState>>;

#[derive(Debug, Clone, Default, Serialize)]
pub struct ProfileState {
    pub program_id: String,
    pub request_arrive_ms: Option<u128>,
    pub request_start_ms: Option<u128>,
    pub first_token_ms: Option<u128>,
    pub request_end_ms: Option<u128>,
    pub token_count: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub cached_tokens: u64,
}

impl ProfileState {
    pub fn new(program_id: impl Into<String>) -> Self {
        Self {
            program_id: program_id.into(),
            ..Self::default()
        }
    }

    pub fn on_request_arrive(&mut self) {
        self.request_arrive_ms = Some(now_ms());
        self.request_start_ms = None;
        self.first_token_ms = None;
        self.request_end_ms = None;
        self.token_count = 0;
        self.prompt_tokens = 0;
        self.completion_tokens = 0;
        self.cached_tokens = 0;
    }

    pub fn on_request_start(&mut self) {
        self.request_start_ms = Some(now_ms());
    }

    pub fn on_first_token(&mut self) {
        if self.first_token_ms.is_none() {
            self.first_token_ms = Some(now_ms());
        }
    }

    pub fn on_token(&mut self, delta_tokens: u64) {
        self.token_count = self.token_count.saturating_add(delta_tokens);
    }

    pub fn on_request_end(&mut self, usage: Option<UsageTokens>) {
        self.request_end_ms = Some(now_ms());
        if let Some(usage) = usage {
            self.prompt_tokens = usage.prompt_tokens.unwrap_or_default();
            self.completion_tokens = usage.completion_tokens.unwrap_or_default();
            self.cached_tokens = usage.cached_tokens.unwrap_or_default();
            self.token_count = usage
                .total_tokens
                .unwrap_or(self.prompt_tokens.saturating_add(self.completion_tokens));
        }
    }

    pub fn first_token_time_ms(&self) -> Option<u128> {
        Some(self.first_token_ms?.saturating_sub(self.request_start_ms?))
    }

    pub fn decode_time_ms(&self) -> Option<u128> {
        Some(self.request_end_ms?.saturating_sub(self.first_token_ms?))
    }
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_millis())
        .unwrap_or_default()
}
