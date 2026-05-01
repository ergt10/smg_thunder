//! Thunder routing mode — program-aware proxy with capacity-based pause/resume scheduling.
//!
//! Ports the Python ThunderAgent (https://github.com/HaoKang-Timmy/ThunderAgent) into smg as a
//! first-class routing mode. See `THUNDER_ROADMAP.md` at the repo root for the phased plan.
//!
//! Phase 1: empty router that returns 501 for every endpoint, just so the factory plumbing and
//! CLI flags compile and route end-to-end. Real chat passthrough lands in Phase 3.

mod program;
mod proxy;
pub mod router;

pub use router::ThunderRouter;
