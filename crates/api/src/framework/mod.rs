use std::sync::{Arc, Mutex};
/// Exposes Actix framework routes.
#[cfg(feature = "actix")]
pub mod actix;

/// Exposes Axum framework routes.
#[cfg(feature = "axum")]
pub mod axum;

/// Exposes Salvo framework routes.
#[cfg(feature = "salvo")]
pub mod salvo;

/// Trait for registering routes with a backend
pub trait RegisterRoute<B, T> {
    /// Register routes with the given backend
    #[must_use]
    fn register(self, backend: B) -> Self;
}

/// Builder for API routes
#[derive(Clone, Debug)]
pub struct ApiBuilder<R> {
    router: R,
    #[allow(unused)]
    /// may not be used in some conditional compilation
    root: bool,
    // ...
    /// Broadcaster used to fan out tracing spans/events to SSE subscribers.
    /// `None` disables the `/events` endpoint.
    #[cfg(feature = "sse")]
    pub broadcaster: Option<Arc<Mutex<crate::sse::TracingBroadcaster>>>,
}

impl<R> ApiBuilder<R> {
    /// Create a new ApiBuilder with default settings
    pub fn new(router: R) -> Self {
        Self {
            router,
            root: true,
            #[cfg(feature = "sse")]
            broadcaster: None,
        }
    }
    /// Create a new ApiBuilder with a custom scope
    /// If `register_root` is true, the root routes (/queues, /tasks, /workers, /overview)
    /// will be registered on the provided scope.
    pub fn new_with_router(router: R, register_root: bool) -> Self {
        Self {
            router,
            root: register_root,
            #[cfg(feature = "sse")]
            broadcaster: None,
        }
    }

    /// Finalize the builder and return the router
    pub fn build(self) -> R {
        self.router
    }
    /// Attaches a tracing broadcaster, enabling the `/events` SSE endpoint.
    #[cfg(feature = "sse")]
    #[must_use]
    pub fn with_broadcaster(
        mut self,
        broadcaster: Arc<Mutex<crate::sse::TracingBroadcaster>>,
    ) -> Self {
        self.broadcaster = Some(broadcaster);
        self
    }
}
