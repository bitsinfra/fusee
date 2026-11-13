//! A failure as recorded: what a failed attempt, step, update, object call
//! or workflow carries, in every SDK.
//!
//! The journal stores a failure's payload as bytes; the envelope is JSON,
//! with absent fields left out:
//!
//! ```json
//! {"message":"card declined","type":"CardDeclined","code":"card_declined",
//!  "details":{"last4":"4242"},"retryable":false,"stack":"...","cause":null}
//! ```
//!
//! A payload that is not an envelope (text recorded by an older writer, or
//! by `fail(token, "reason")`) reads as a failure with that message.
//!
//! A step function's error is anything that converts into a [`Failure`]:
//! every [`std::error::Error`] does, keeping its type name and its source
//! chain, and Fusee's own [`Error`](crate::Error) keeps the failure it
//! carries. To mark a failure as final, or to give it a code or details,
//! return a `Failure` directly:
//!
//! ```
//! use fusee::Failure;
//! # fn charge() -> Result<(), Failure> {
//! return Err(Failure::fatal("card declined")
//!     .code("card_declined")
//!     .details(serde_json::json!({ "last4": "4242" })));
//! # }
//! ```

use fusee_core::record::Payload;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// Stacks longer than this are cut (on a character boundary).
pub const STACK_LIMIT: usize = 4096;

/// A failure, as recorded and as returned to callers. One pointer wide (the
/// fields are boxed), so a `Result` that may carry one stays small; it
/// reads as its [`FailureData`].
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Failure(Box<FailureData>);

impl std::ops::Deref for Failure {
    type Target = FailureData;
    fn deref(&self) -> &FailureData {
        &self.0
    }
}

impl std::ops::DerefMut for Failure {
    fn deref_mut(&mut self) -> &mut FailureData {
        &mut self.0
    }
}

impl From<FailureData> for Failure {
    fn from(d: FailureData) -> Self {
        Self(Box::new(d))
    }
}

impl fmt::Debug for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The fields of a [`Failure`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct FailureData {
    pub message: String,
    /// The error's type (a class or type name), if it is not the
    /// language's plain error type.
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// An application code, such as `card_declined`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Structured fields the application attached.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
    /// `Some(false)`: the failure is final and is never retried.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retryable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stack: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause: Option<Box<Failure>>,
}

impl Failure {
    /// A retryable failure with this message.
    pub fn new(message: impl Into<String>) -> Self {
        FailureData {
            message: message.into(),
            ..FailureData::default()
        }
        .into()
    }

    /// A failure that is never retried (`FatalError` in TypeScript,
    /// `fusee.Fatal` in Python).
    pub fn fatal(message: impl Into<String>) -> Self {
        Self::new(message).retryable(false)
    }

    /// Sets the type name.
    pub fn kind(mut self, kind: impl Into<String>) -> Self {
        self.kind = Some(kind.into());
        self
    }

    pub fn code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    pub fn details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }

    /// `false` makes the failure final: no retry policy retries it.
    pub fn retryable(mut self, retryable: bool) -> Self {
        self.retryable = Some(retryable);
        self
    }

    /// Records a stack trace, cut to [`STACK_LIMIT`] bytes.
    pub fn stack(mut self, stack: impl Into<String>) -> Self {
        let mut s: String = stack.into();
        cut(&mut s, STACK_LIMIT);
        self.stack = Some(s);
        self
    }

    pub fn caused_by(mut self, cause: impl Into<Failure>) -> Self {
        self.cause = Some(Box::new(cause.into()));
        self
    }

    /// Whether a retry policy may retry this failure (it was not marked
    /// final).
    pub fn is_retryable(&self) -> bool {
        self.retryable != Some(false)
    }

    /// The innermost cause (this failure if it has none).
    pub fn root(&self) -> &Failure {
        let mut f = self;
        while let Some(c) = &f.cause {
            f = c;
        }
        f
    }

    /// Whether this failure, or one of its causes, has type `kind`.
    pub fn has_kind(&self, kind: &str) -> bool {
        let mut f = Some(self);
        while let Some(x) = f {
            if x.kind.as_deref() == Some(kind) {
                return true;
            }
            f = x.cause.as_deref();
        }
        false
    }

    /// The failure as it is recorded.
    pub fn to_payload(&self) -> Payload {
        match serde_json::to_vec(self) {
            Ok(v) => Arc::from(v),
            // A `details` value always serializes; this is not reached.
            Err(_) => Arc::from(self.message.as_bytes()),
        }
    }

    /// Reads a recorded failure: an envelope, or plain text as its message.
    pub fn from_payload(p: &[u8]) -> Self {
        if p.first() == Some(&b'{')
            && let Ok(f) = serde_json::from_slice::<Failure>(p)
        {
            return f;
        }
        Self::new(String::from_utf8_lossy(p))
    }

    /// A failure from an error and its source chain. `kind` names the
    /// outermost error's type.
    pub fn from_error(e: &(dyn std::error::Error + 'static), kind: Option<String>) -> Self {
        if let Some(f) = e.downcast_ref::<Failure>() {
            return f.clone();
        }
        if let Some(own) = e.downcast_ref::<crate::Error>() {
            return own.to_failure();
        }
        let mut f = Self::new(e.to_string());
        f.kind = kind;
        if let Some(src) = e.source() {
            f.cause = Some(Box::new(Self::from_error(src, None)));
        }
        f
    }
}

/// The last path segment of a type name, without generic arguments:
/// `my_app::pay::CardDeclined` is `CardDeclined`. Plain error types
/// (strings, boxed errors, `io::Error`) have none.
pub(crate) fn short_type_name<T: ?Sized>() -> Option<String> {
    let full = std::any::type_name::<T>();
    let base = full.split('<').next().unwrap_or(full);
    let short = base.rsplit("::").next().unwrap_or(base);
    match short.trim_start_matches('&') {
        "" | "String" | "str" | "Box" | "Error" => None,
        s => Some(s.to_owned()),
    }
}

/// What a step function's error becomes when it is recorded.
///
/// Every type that implements `Display` converts: a [`Failure`] as it is,
/// Fusee's own [`Error`](crate::Error) with the failure it carries, boxed
/// errors and `io::Error` with their source chains, and any other error
/// with its message and type name. Return a [`Failure`] to choose the
/// code, details or `retryable` yourself.
pub trait IntoFailure {
    fn into_failure(self) -> Failure;
}

impl<E: fmt::Display + 'static> IntoFailure for E {
    fn into_failure(self) -> Failure {
        let mut slot = Some(self);
        let any = &mut slot as &mut dyn Any;
        if let Some(f) = any.downcast_mut::<Option<Failure>>() {
            return f.take().unwrap_or_default();
        }
        if let Some(Some(e)) = any.downcast_mut::<Option<crate::Error>>() {
            return e.to_failure();
        }
        if let Some(Some(e)) =
            any.downcast_mut::<Option<Box<dyn std::error::Error + Send + Sync>>>()
        {
            return Failure::from_error(&**e, None);
        }
        if let Some(Some(e)) = any.downcast_mut::<Option<std::io::Error>>() {
            return Failure::from_error(e, None);
        }
        let e = slot.take().map(|e| e.to_string()).unwrap_or_default();
        let mut f = Failure::new(e);
        f.kind = short_type_name::<E>();
        f
    }
}

impl From<String> for Failure {
    fn from(message: String) -> Self {
        Self::new(message)
    }
}

impl From<&str> for Failure {
    fn from(message: &str) -> Self {
        Self::new(message)
    }
}

impl From<crate::Error> for Failure {
    fn from(e: crate::Error) -> Self {
        e.to_failure()
    }
}

impl From<std::io::Error> for Failure {
    fn from(e: std::io::Error) -> Self {
        Self::from_error(&e, None)
    }
}

impl From<serde_json::Error> for Failure {
    fn from(e: serde_json::Error) -> Self {
        crate::Error::from(e).to_failure()
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for Failure {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self::from_error(&*e, None)
    }
}

impl std::error::Error for Failure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause
            .as_deref()
            .map(|c| c as &(dyn std::error::Error + 'static))
    }
}

impl fmt::Display for Failure {
    /// The message; `{:#}` adds each cause after a colon.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)?;
        if f.alternate() {
            let mut c = self.cause.as_deref();
            while let Some(x) = c {
                write!(f, ": {}", x.message)?;
                c = x.cause.as_deref();
            }
        }
        Ok(())
    }
}

fn cut(s: &mut String, max: usize) {
    if s.len() > max {
        let mut at = max;
        while !s.is_char_boundary(at) {
            at -= 1;
        }
        s.truncate(at);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct CardDeclined;
    impl fmt::Display for CardDeclined {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("card declined")
        }
    }
    impl std::error::Error for CardDeclined {}

    #[derive(Debug)]
    struct Outer(CardDeclined);
    impl fmt::Display for Outer {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("charge failed")
        }
    }
    impl std::error::Error for Outer {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn plain_message_is_minimal() {
        let f = Failure::new("flaky");
        assert_eq!(&*f.to_payload(), br#"{"message":"flaky"}"#);
        assert_eq!(Failure::from_payload(&f.to_payload()), f);
    }

    #[test]
    fn text_reads_as_message() {
        assert_eq!(Failure::from_payload(b"boom"), Failure::new("boom"));
        assert_eq!(
            Failure::from_payload(b"{not json"),
            Failure::new("{not json")
        );
    }

    #[test]
    fn error_keeps_type_and_chain() {
        let f = Failure::from_error(&Outer(CardDeclined), Some("Outer".into()));
        assert_eq!(f.message, "charge failed");
        assert_eq!(f.kind.as_deref(), Some("Outer"));
        assert_eq!(f.cause.as_ref().unwrap().message, "card declined");
        assert_eq!(format!("{f:#}"), "charge failed: card declined");
        assert!(f.is_retryable());
    }

    #[test]
    fn fatal_round_trips() {
        let f = Failure::fatal("card declined")
            .kind("CardDeclined")
            .code("card_declined")
            .details(serde_json::json!({"last4": "4242"}));
        let back = Failure::from_payload(&f.to_payload());
        assert_eq!(back, f);
        assert!(!back.is_retryable());
        assert!(back.has_kind("CardDeclined"));
    }

    #[test]
    fn conversions() {
        let f = std::io::Error::other("disk").into_failure();
        assert_eq!(f.message, "disk");
        assert_eq!(f.kind, None);
        assert_eq!(
            CardDeclined.into_failure().kind.as_deref(),
            Some("CardDeclined")
        );
        assert_eq!(String::from("x").into_failure(), Failure::new("x"));
        assert_eq!("x".into_failure(), Failure::new("x"));
        let fatal = Failure::fatal("no");
        assert_eq!(fatal.clone().into_failure(), fatal);
        let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(Outer(CardDeclined));
        assert_eq!(
            boxed.into_failure().cause.clone().unwrap().message,
            "card declined"
        );
    }

    #[test]
    fn stack_is_cut() {
        let f = Failure::new("x").stack("é".repeat(3000));
        assert!(f.stack.as_ref().unwrap().len() <= STACK_LIMIT);
    }
}
