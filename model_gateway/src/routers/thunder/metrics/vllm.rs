//! vLLM metrics client.
//!
//! Polls vLLM's `GET /get_server_info` for the static cache config (block size, GPU blocks).
//! The Python original parses Prometheus `/metrics` text for both static config and runtime
//! counters. Phase 6 keeps only the static part — the dynamic prefix-cache-savings calculation
//! arrives with Phase 12 polish, and counter-style metrics are not yet needed for capacity
//! admission.

use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::RwLock;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{CacheConfig, MetricsClient};

/// Shape returned by mock_vllm.py's `/get_server_info` (and the subset of fields real vLLM
/// exposes that Phase 6 cares about).
#[derive(Debug, Deserialize)]
struct ServerInfoResponse {
    #[serde(default)]
    cache_config: ServerCacheConfig,
}

#[derive(Debug, Default, Deserialize)]
struct ServerCacheConfig {
    #[serde(default)]
    block_size: u64,
    #[serde(default)]
    num_gpu_blocks: u64,
    #[serde(default)]
    total_kv_cache_tokens: u64,
}

#[derive(Debug)]
pub struct VllmMetricsClient {
    url: String,
    client: reqwest::Client,
    cache_config: RwLock<Option<CacheConfig>>,
    healthy: AtomicBool,
}

impl VllmMetricsClient {
    pub fn new(url: impl Into<String>, client: reqwest::Client) -> Self {
        let url = url.into().trim_end_matches('/').to_string();
        Self {
            url,
            client,
            cache_config: RwLock::new(None),
            healthy: AtomicBool::new(false),
        }
    }

    fn server_info_url(&self) -> String {
        format!("{}/get_server_info", self.url)
    }
}

#[async_trait::async_trait]
impl MetricsClient for VllmMetricsClient {
    fn url(&self) -> &str {
        &self.url
    }

    fn cache_config(&self) -> Option<CacheConfig> {
        *self.cache_config.read()
    }

    fn healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    async fn fetch_cache_config(&self) -> bool {
        let url = self.server_info_url();
        let resp = match self.client.get(&url).send().await {
            Ok(resp) => resp,
            Err(_) => {
                self.healthy.store(false, Ordering::Release);
                return false;
            }
        };
        if !resp.status().is_success() {
            self.healthy.store(false, Ordering::Release);
            return false;
        }
        let info = match resp.json::<ServerInfoResponse>().await {
            Ok(info) => info,
            Err(_) => {
                self.healthy.store(false, Ordering::Release);
                return false;
            }
        };
        let cfg = CacheConfig {
            block_size: info.cache_config.block_size,
            num_gpu_blocks: info.cache_config.num_gpu_blocks,
            total_kv_cache_tokens: info.cache_config.total_kv_cache_tokens,
        };
        *self.cache_config.write() = Some(cfg);
        self.healthy.store(true, Ordering::Release);
        true
    }

    fn snapshot(&self) -> Value {
        let mut obj = serde_json::Map::new();
        obj.insert("url".into(), Value::String(self.url.clone()));
        obj.insert("healthy".into(), Value::Bool(self.healthy()));
        if let Some(cfg) = self.cache_config() {
            obj.insert(
                "cache_config".into(),
                json!({
                    "block_size": cfg.block_size,
                    "num_gpu_blocks": cfg.num_gpu_blocks,
                    "total_kv_cache_tokens": cfg.total_kv_cache_tokens,
                    "total_tokens_capacity": cfg.total_tokens_capacity(),
                }),
            );
        }
        Value::Object(obj)
    }
}
