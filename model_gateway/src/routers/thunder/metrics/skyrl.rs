//! SkyRL metrics client.

use serde_json::Value;

use super::{CacheConfig, MetricsClient, VllmMetricsClient};

#[derive(Debug)]
pub struct SkyrlMetricsClient {
    inner: VllmMetricsClient,
}

impl SkyrlMetricsClient {
    pub fn new(url: impl Into<String>, client: reqwest::Client) -> Self {
        Self {
            inner: VllmMetricsClient::new(url, client),
        }
    }
}

#[async_trait::async_trait]
impl MetricsClient for SkyrlMetricsClient {
    fn url(&self) -> &str {
        self.inner.url()
    }

    fn cache_config(&self) -> Option<CacheConfig> {
        self.inner.cache_config()
    }

    fn healthy(&self) -> bool {
        self.inner.healthy()
    }

    async fn fetch_cache_config(&self) -> bool {
        self.inner.fetch_cache_config().await
    }

    fn snapshot(&self) -> Value {
        let mut snapshot = self.inner.snapshot();
        if let Some(obj) = snapshot.as_object_mut() {
            obj.insert("backend_type".into(), Value::String("skyrl".into()));
        }
        snapshot
    }
}
