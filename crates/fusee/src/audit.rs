//! Audit: who asked for a signal, cancel, terminate or reset.
//!
//! The API that takes a request names its requester ([`as_requester`]);
//! the record the request writes carries it as `by` (see
//! [`fusee_core::record::Record`]), and the run's journal shows it. The
//! HTTP API names a token's scope, a namespace, or a proxy's identity
//! header; the CLI names its user. Requests made in code name nobody,
//! and their records carry no `by` field at all.

use std::future::Future;
use std::sync::Arc;

tokio::task_local! {
    static BY: Arc<str>;
}

/// Runs `f` with `by` as the requester of the signals, cancellations,
/// terminations and resets it asks of an engine.
pub async fn as_requester<F: Future>(by: impl Into<Arc<str>>, f: F) -> F::Output {
    BY.scope(by.into(), f).await
}

/// The requester of the current task, if one was named.
pub fn current() -> Option<Arc<str>> {
    BY.try_with(Arc::clone).ok()
}

/// As a record carries it.
pub(crate) fn current_bytes() -> Option<fusee_core::record::Payload> {
    BY.try_with(|b| Arc::from(b.as_bytes())).ok()
}
