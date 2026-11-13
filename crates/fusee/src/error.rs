use crate::failure::Failure;
use std::fmt;

/// Where each error code's page lives. One constant, so the docs domain
/// (an open decision) changes in one place.
pub const DOCS_BASE: &str = "https://github.com/bitsinfra/fusee/blob/main/docs/errors.md";

/// Everything that can go wrong in Fusee, as seen by application code.
///
/// Every SDK has the same set, named by [`Error::code`]; each code has a
/// page at [`Error::docs`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// The shard's queues are full. Retry later; nothing was recorded.
    Busy,
    /// No run with this workflow ID is known (never started, or its
    /// retention ended).
    NotFound(String),
    /// A workflow with this ID already exists and the start asked for a new
    /// run only. Carries the ID and what holds it.
    AlreadyExists(String),
    /// No code is registered for this workflow type (or version). Carries
    /// the message, which lists the types that are registered.
    UnknownWorkflow(String),
    /// A step failed after its last attempt, or with a failure that is not
    /// retried. Carries the step name and the step's failure.
    Step { name: String, failure: Failure },
    /// The workflow (or an update, or an object's handler) failed.
    Failed(Failure),
    /// A child workflow failed. Carries its workflow ID and its failure.
    Child { id: String, failure: Failure },
    /// Cancellation reached this operation. Carries the reason.
    Cancelled(String),
    /// A timeout elapsed (step, wait, or the workflow's deadline).
    TimedOut,
    /// The run was terminated.
    Terminated,
    /// The code made different journal operations than its journal records.
    Determinism(String),
    /// A request was refused and nothing was recorded: an update's
    /// validator refused it, a completion token is stale, an argument is
    /// invalid. Carries the reason.
    Rejected(String),
    /// A value could not be serialized or deserialized.
    Codec(String),
    /// No connection to a Fusee server, or it was lost.
    Connection(String),
    /// A disk or network failure. After a failed flush the shard stops.
    Io(String),
    /// The engine is closed.
    Closed,
}

impl Error {
    /// The error's code, the same in every SDK.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Busy => "Busy",
            Self::NotFound(_) => "NotFound",
            Self::AlreadyExists(_) => "AlreadyExists",
            Self::UnknownWorkflow(_) => "UnknownWorkflow",
            Self::Step { .. } => "StepFailed",
            Self::Failed(_) => "WorkflowFailed",
            Self::Child { .. } => "ChildFailed",
            Self::Cancelled(_) => "Cancelled",
            Self::TimedOut => "TimedOut",
            Self::Terminated => "Terminated",
            Self::Determinism(_) => "Determinism",
            Self::Rejected(_) => "Rejected",
            Self::Codec(_) => "Codec",
            Self::Connection(_) => "Connection",
            Self::Io(_) => "Io",
            Self::Closed => "Closed",
        }
    }

    /// A link to the page for this error's code.
    pub fn docs(&self) -> String {
        docs_link(self.code())
    }

    /// An error with a failure to wrap: a workflow's failure from a message.
    pub fn failed(message: impl Into<String>) -> Self {
        Self::Failed(Failure::new(message))
    }

    /// The failure this error carries, if it carries one.
    pub fn failure(&self) -> Option<&Failure> {
        match self {
            Self::Step { failure, .. } | Self::Failed(failure) | Self::Child { failure, .. } => {
                Some(failure)
            }
            _ => None,
        }
    }

    /// Whether a step that returned this error may be retried.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Step { failure, .. } | Self::Failed(failure) | Self::Child { failure, .. } => {
                failure.is_retryable()
            }
            Self::Codec(_) | Self::Determinism(_) | Self::Rejected(_) | Self::Terminated => false,
            _ => true,
        }
    }

    /// The error as a recorded failure. Fusee's own errors record their
    /// code as the type, so every SDK reads them back as the same error.
    pub fn to_failure(&self) -> Failure {
        match self {
            Self::Step { name, failure } => crate::failure::FailureData {
                message: self.to_string(),
                kind: Some("StepFailed".into()),
                details: Some(serde_json::json!({ "step": name })),
                retryable: failure.retryable,
                cause: Some(Box::new(failure.clone())),
                ..Default::default()
            }
            .into(),
            Self::Child { id, failure } => crate::failure::FailureData {
                message: self.to_string(),
                kind: Some("ChildFailed".into()),
                details: Some(serde_json::json!({ "workflowId": id })),
                retryable: failure.retryable,
                cause: Some(Box::new(failure.clone())),
                ..Default::default()
            }
            .into(),
            // The workflow's own failure passes through as it is.
            Self::Failed(failure) => failure.clone(),
            other => {
                let mut f = Failure::new(other.to_string()).kind(other.code());
                if !other.is_retryable() {
                    f.retryable = Some(false);
                }
                f
            }
        }
    }

    /// Reads a recorded failure back: one of Fusee's own errors when its
    /// type is one of their codes, otherwise a workflow failure.
    pub fn from_failure(f: Failure) -> Self {
        let detail = |key: &str| {
            f.details
                .as_ref()
                .and_then(|d| d.get(key))
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        };
        match f.kind.as_deref() {
            Some("StepFailed") => {
                if let (Some(name), Some(cause)) = (detail("step"), f.cause.clone()) {
                    return Self::Step {
                        name,
                        failure: *cause,
                    };
                }
            }
            Some("ChildFailed") => {
                if let (Some(id), Some(cause)) = (detail("workflowId"), f.cause.clone()) {
                    return Self::Child {
                        id,
                        failure: *cause,
                    };
                }
            }
            _ => {}
        }
        Self::Failed(f)
    }
}

/// The message for a start of a type no code is registered for: the types
/// that are, and the nearest name when it looks like a typo.
pub fn unknown_workflow(wtype: &str, registered: &[String]) -> String {
    // Fusee's own coordinator types are not the application's to start.
    let mut names: Vec<&str> = registered
        .iter()
        .map(String::as_str)
        .filter(|n| !n.starts_with("fusee."))
        .collect();
    names.sort_unstable();
    let mut m = format!("no code is registered for workflow type '{wtype}'");
    if let Some(near) = nearest(wtype, &names) {
        m.push_str(&format!(" (did you mean '{near}'?)"));
    }
    if names.is_empty() {
        m.push_str(". No workflow types are registered: register one before starting it");
    } else {
        m.push_str(&format!(". Registered types: {}", names.join(", ")));
    }
    m
}

/// The message for a start refused because its workflow ID is taken.
/// `holder` is the run that has the ID and its status, when known.
pub fn already_exists(id: &str, holder: Option<(u64, &str)>) -> String {
    match holder {
        Some((run, status)) if is_live(status) => {
            let state = if status == "running" {
                String::new()
            } else {
                format!(", {status}")
            };
            format!(
                "workflow '{id}' is already running (run {run}{state}). Start it with the ID \
                 policy running = use-existing to attach to that run, or use another ID"
            )
        }
        Some((run, status)) => format!(
            "workflow '{id}' already ran (run {run}, {status}) and this start refuses a finished \
             ID. Start it with the ID policy finished = allow to run it again"
        ),
        None => format!(
            "workflow '{id}' already exists. Start it with the ID policy running = use-existing \
             to attach to it"
        ),
    }
}

fn is_live(status: &str) -> bool {
    matches!(
        status,
        "running" | "suspended" | "parked" | "paused" | "stuck" | "waiting_for_code" | "queued"
    )
}

/// The registered name closest to `name` by edit distance, if close enough
/// to be a typo (at most a third of its length, and at most 3).
pub fn nearest<'a>(name: &str, candidates: &[&'a str]) -> Option<&'a str> {
    let limit = (name.chars().count() / 3).clamp(1, 3);
    candidates
        .iter()
        .map(|c| (levenshtein(name, c), *c))
        .filter(|(d, _)| *d <= limit)
        .min()
        .map(|(_, c)| c)
}

fn levenshtein(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut prev = row[0];
        row[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cur = row[j + 1];
            row[j + 1] = if ca == *cb {
                prev
            } else {
                1 + prev.min(row[j]).min(row[j + 1])
            };
            prev = cur;
        }
    }
    row[b.len()]
}

/// The page for an error code.
pub fn docs_link(code: &str) -> String {
    format!("{DOCS_BASE}#{}", code.to_ascii_lowercase())
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy => write!(f, "engine busy: the shard's queue is full; retry later"),
            Self::NotFound(id) => write!(f, "workflow '{id}' not found"),
            Self::AlreadyExists(m) => write!(f, "{m}"),
            Self::UnknownWorkflow(m) => write!(f, "{m}"),
            Self::Step { name, failure } => write!(f, "step '{name}' failed: {failure}"),
            Self::Failed(failure) => write!(f, "{failure}"),
            Self::Child { id, failure } => write!(f, "child '{id}' failed: {failure}"),
            Self::Cancelled(r) => write!(f, "cancelled: {r}"),
            Self::TimedOut => write!(f, "timed out"),
            Self::Terminated => write!(f, "terminated"),
            Self::Determinism(m) => write!(f, "{m}"),
            Self::Rejected(m) => write!(f, "{m}"),
            Self::Codec(m) => write!(f, "{m}"),
            Self::Connection(m) => write!(f, "{m}"),
            Self::Io(m) => write!(f, "{m}"),
            Self::Closed => write!(f, "engine closed"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.failure()
            .map(|f| f as &(dyn std::error::Error + 'static))
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::Codec(format!("codec error: {e}"))
    }
}

impl From<fusee_core::run::DeterminismError> for Error {
    fn from(e: fusee_core::run::DeterminismError) -> Self {
        Self::Determinism(e.to_string())
    }
}

impl From<Failure> for Error {
    fn from(f: Failure) -> Self {
        Self::Failed(f)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_error_round_trips_through_its_failure() {
        let e = Error::Step {
            name: "charge".into(),
            failure: Failure::fatal("card declined")
                .kind("CardDeclined")
                .code("card_declined"),
        };
        let f = e.to_failure();
        assert_eq!(f.kind.as_deref(), Some("StepFailed"));
        assert!(!f.is_retryable());
        assert_eq!(f.root().code.as_deref(), Some("card_declined"));
        assert_eq!(
            Error::from_failure(Failure::from_payload(&f.to_payload())),
            e
        );
        assert_eq!(e.code(), "StepFailed");
        assert!(e.docs().ends_with("#stepfailed"));
    }

    #[test]
    fn unknown_workflow_names_the_nearest() {
        let m = unknown_workflow("chekout", &["checkout".into(), "refund".into()]);
        assert_eq!(
            m,
            "no code is registered for workflow type 'chekout' (did you mean 'checkout'?). \
             Registered types: checkout, refund"
        );
        assert!(unknown_workflow("x", &[]).contains("No workflow types are registered"));
    }

    #[test]
    fn workflow_failure_passes_through() {
        let f = Failure::new("out of stock").code("oos");
        assert_eq!(Error::Failed(f.clone()).to_failure(), f);
        assert_eq!(Error::from_failure(f.clone()), Error::Failed(f));
    }
}
