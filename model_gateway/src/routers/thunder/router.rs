//! Empty `ThunderRouter` (Phase 1).
//!
//! Implements `RouterTrait` with only the two required methods (`as_any`, `router_type`); every
//! other endpoint inherits the trait's default 501 response. Subsequent phases progressively
//! override `route_chat`, add a scheduler, etc.

use std::any::Any;
use std::sync::Arc;

use crate::app_context::AppContext;

pub struct ThunderRouter {
    /// Worker URLs from `RoutingMode::Thunder { worker_urls }`. Used in Phase 3+ as upstream
    /// backends; held here so Phase 1's stub still owns the configured list and can return it
    /// from a future `/programs` or health endpoint.
    worker_urls: Vec<String>,
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
        Ok(Self { worker_urls })
    }

    pub fn worker_urls(&self) -> &[String] {
        &self.worker_urls
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
}
