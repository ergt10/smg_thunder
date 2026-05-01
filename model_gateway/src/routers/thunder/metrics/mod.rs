//! Metrics clients for Thunder backends.
//!
//! Phase 6 ports the Python `MetricsClient` abstraction (see
//! `ThunderAgent/backend/metrics_base.py`). Phase 6 only ships the vLLM client; SGLang and
//! Phase 10 adds SGLang and SkyRL client wrappers.

use std::fmt::Debug;

use serde::Serialize;

pub mod sglang;
pub mod skyrl;
pub mod vllm;

pub use sglang::SglangMetricsClient;
pub use skyrl::SkyrlMetricsClient;
pub use vllm::VllmMetricsClient;

/// Static KV cache configuration reported by a backend (vLLM `cache_config` shape).
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct CacheConfig {
    pub block_size: u64,
    pub num_gpu_blocks: u64,
    pub total_kv_cache_tokens: u64,
}

impl CacheConfig {
    /// Total KV cache tokens. Prefers the explicit `total_kv_cache_tokens` field when present
    /// (vLLM mock convenience field); falls back to `block_size * num_gpu_blocks`.
    pub fn total_tokens_capacity(&self) -> u64 {
        if self.total_kv_cache_tokens > 0 {
            self.total_kv_cache_tokens
        } else {
            self.block_size.saturating_mul(self.num_gpu_blocks)
        }
    }
}

/// Abstract metrics client for a single backend URL.
///
/// Implementations are responsible for periodic polling and exposing the latest cache config +
/// liveness flag. The router owns the `Arc<dyn MetricsClient>`; capacity calculations live on
/// `BackendState` (see `routers/thunder/backend.rs`).
#[async_trait::async_trait]
pub trait MetricsClient: Send + Sync + Debug {
    /// Backend base URL (e.g. `http://localhost:8001`). Used by Phase 7+ for fan-out logging.
    #[allow(dead_code)]
    fn url(&self) -> &str;

    /// Latest cache config, or `None` if it has never been fetched successfully.
    fn cache_config(&self) -> Option<CacheConfig>;

    /// Whether the most recent poll succeeded.
    fn healthy(&self) -> bool;

    /// Force-fetch the cache config now (used at startup and for tests).
    async fn fetch_cache_config(&self) -> bool;

    /// Serialize the client's observable state for `/thunder/metrics`.
    fn snapshot(&self) -> serde_json::Value;
}
