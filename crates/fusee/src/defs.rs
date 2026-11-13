//! Typed references: a workflow type, signal, query or update named once,
//! as a `const`, with the types of what it carries.
//!
//! ```
//! use fusee::{Query, Signal, Update, WorkflowRef};
//!
//! pub const CHECKOUT: WorkflowRef<u64, String> = WorkflowRef::new("checkout");
//! pub const REVIEW: Signal<bool> = Signal::new("review");
//! pub const STATUS: Query<(), String> = Query::new("status");
//! pub const APPROVE: Update<u32, bool> = Update::new("approve");
//! ```
//!
//! A plain `&str` works wherever a reference does (for calls across
//! languages); the types then come from the call.

use std::borrow::Cow;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

macro_rules! named {
    ($(#[$doc:meta])* $ty:ident<$($p:ident),+>, $trait:ident) => {
        $(#[$doc])*
        pub struct $ty<$($p),+> {
            name: Cow<'static, str>,
            _t: PhantomData<fn($($p),+)>,
        }

        impl<$($p),+> $ty<$($p),+> {
            pub const fn new(name: &'static str) -> Self {
                Self { name: Cow::Borrowed(name), _t: PhantomData }
            }

            /// A reference whose name is made at run time.
            pub fn named(name: impl Into<String>) -> Self {
                Self { name: Cow::Owned(name.into()), _t: PhantomData }
            }

            pub fn name(&self) -> &str {
                &self.name
            }
        }

        impl<$($p),+> Clone for $ty<$($p),+> {
            fn clone(&self) -> Self {
                Self { name: self.name.clone(), _t: PhantomData }
            }
        }

        impl<$($p),+> fmt::Debug for $ty<$($p),+> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($ty), "({:?})"), self.name)
            }
        }

        impl<$($p),+> $trait<$($p),+> for $ty<$($p),+> {
            fn name(&self) -> &str {
                &self.name
            }
        }

        impl<$($p),+> $trait<$($p),+> for &$ty<$($p),+> {
            fn name(&self) -> &str {
                &self.name
            }
        }

        impl<$($p),+> $trait<$($p),+> for &str {
            fn name(&self) -> &str {
                self
            }
        }

        impl<$($p),+> $trait<$($p),+> for String {
            fn name(&self) -> &str {
                self
            }
        }

        impl<$($p),+> $trait<$($p),+> for &String {
            fn name(&self) -> &str {
                self
            }
        }
    };
}

/// Anything that names a signal carrying `T`: a [`Signal<T>`] or a string.
pub trait AsSignal<T> {
    fn name(&self) -> &str;
}

/// Anything that names a query taking `A` and answering `R`.
pub trait AsQuery<A, R> {
    fn name(&self) -> &str;
}

/// Anything that names an update taking `A` and answering `R`.
pub trait AsUpdate<A, R> {
    fn name(&self) -> &str;
}

named!(
    /// A signal's name and payload type.
    Signal<T>,
    AsSignal
);
named!(
    /// A query's name, argument and answer types.
    Query<A, R>,
    AsQuery
);
named!(
    /// An update's name, argument and result types.
    Update<A, R>,
    AsUpdate
);

/// A workflow type: its name, the version a start runs (the newest
/// registered unless set), its input and its result.
pub struct WorkflowRef<I, O> {
    name: Cow<'static, str>,
    version: Option<u32>,
    _t: PhantomData<fn(I) -> O>,
}

impl<I, O> WorkflowRef<I, O> {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name: Cow::Borrowed(name),
            version: None,
            _t: PhantomData,
        }
    }

    /// A reference whose name is made at run time.
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: Cow::Owned(name.into()),
            version: None,
            _t: PhantomData,
        }
    }

    /// This type at `version`: registering registers that version;
    /// starting starts it.
    pub const fn version(mut self, version: u32) -> Self {
        self.version = Some(version);
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version_number(&self) -> Option<u32> {
        self.version
    }
}

impl<I, O> Clone for WorkflowRef<I, O> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            version: self.version,
            _t: PhantomData,
        }
    }
}

impl<I, O> fmt::Debug for WorkflowRef<I, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WorkflowRef({:?}", self.name)?;
        if let Some(v) = self.version {
            write!(f, ", version {v}")?;
        }
        f.write_str(")")
    }
}

/// Anything that names a workflow type with input `I` and result `O`: a
/// [`WorkflowRef`] or a string.
pub trait AsWorkflow<I, O> {
    fn name(&self) -> &str;
    fn version(&self) -> Option<u32> {
        None
    }
}

impl<I, O> AsWorkflow<I, O> for WorkflowRef<I, O> {
    fn name(&self) -> &str {
        &self.name
    }
    fn version(&self) -> Option<u32> {
        self.version
    }
}

impl<I, O> AsWorkflow<I, O> for &WorkflowRef<I, O> {
    fn name(&self) -> &str {
        &self.name
    }
    fn version(&self) -> Option<u32> {
        self.version
    }
}

impl<I, O> AsWorkflow<I, O> for &std::sync::Arc<WorkflowRef<I, O>> {
    fn name(&self) -> &str {
        &self.name
    }
    fn version(&self) -> Option<u32> {
        self.version
    }
}

impl<I, O> AsWorkflow<I, O> for &str {
    fn name(&self) -> &str {
        self
    }
}

impl<I, O> AsWorkflow<I, O> for String {
    fn name(&self) -> &str {
        self
    }
}

impl<I, O> AsWorkflow<I, O> for &String {
    fn name(&self) -> &str {
        self
    }
}

/// Anything that names a workflow type with input `I`, whatever its result
/// (for starts that do not wait for it).
pub trait NamesWorkflow<I> {
    fn name(&self) -> &str;
}

impl<I, O> NamesWorkflow<I> for WorkflowRef<I, O> {
    fn name(&self) -> &str {
        &self.name
    }
}

impl<I, O> NamesWorkflow<I> for &WorkflowRef<I, O> {
    fn name(&self) -> &str {
        &self.name
    }
}

impl<I> NamesWorkflow<I> for &str {
    fn name(&self) -> &str {
        self
    }
}

impl<I> NamesWorkflow<I> for String {
    fn name(&self) -> &str {
        self
    }
}

impl<I> NamesWorkflow<I> for &String {
    fn name(&self) -> &str {
        self
    }
}

/// Builds an operation's future from its options, when it is first
/// polled: what lets an operation take its options by builder methods and
/// still compile to one future, with nothing boxed.
pub trait Make<O> {
    type Output;
    type Fut: Future<Output = Self::Output>;
    fn make(self, options: O) -> Self::Fut;
}

impl<O, G, F> Make<O> for G
where
    G: FnOnce(O) -> F,
    F: Future,
{
    type Output = F::Output;
    type Fut = F;
    fn make(self, options: O) -> F {
        self(options)
    }
}

/// An operation with options `O`, set by its builder methods, that runs
/// when awaited: `ctx.step("charge", f).retry(policy).await`.
#[must_use = "an operation does nothing until it is awaited"]
pub struct Op<O, M: Make<O>> {
    options: Option<O>,
    make: Option<M>,
    fut: Option<M::Fut>,
}

impl<O, M: Make<O>> Op<O, M> {
    pub(crate) fn new(options: O, make: M) -> Self {
        Self {
            options: Some(options),
            make: Some(make),
            fut: None,
        }
    }

    /// The options, to change before the first poll.
    pub(crate) fn options_mut(&mut self) -> &mut O {
        match self.options.as_mut() {
            Some(o) => o,
            None => unreachable!("options are taken only when the operation is polled"),
        }
    }

    /// Replaces every option at once.
    pub fn options(mut self, options: O) -> Self {
        self.options = Some(options);
        self
    }
}

impl<O, M: Make<O>> Future for Op<O, M> {
    type Output = M::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<M::Output> {
        // SAFETY: `fut` is never moved once it is set: it is created here,
        // in place, and only polled through a pinned reference after; the
        // other fields are not structurally pinned.
        let this = unsafe { self.get_unchecked_mut() };
        if this.fut.is_none() {
            let (Some(make), Some(options)) = (this.make.take(), this.options.take()) else {
                unreachable!("an operation is made once")
            };
            this.fut = Some(make.make(options));
        }
        match this.fut.as_mut() {
            // SAFETY: see above; `this` is pinned, so is `fut`.
            Some(f) => unsafe { Pin::new_unchecked(f) }.poll(cx),
            None => unreachable!("made above"),
        }
    }
}
