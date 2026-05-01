//! Thunder routing mode — program-aware proxy with capacity-based pause/resume scheduling.
//!
//! Ports the Python ThunderAgent (https://github.com/HaoKang-Timmy/ThunderAgent) into smg as a
//! first-class routing mode. See `THUNDER_ROADMAP.md` at the repo root for the phased plan.

mod backend;
mod metrics;
mod program;
mod proxy;
pub mod router;
mod scheduler;

pub use router::ThunderRouter;
