use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use futures::{Future, Poll};
use http;
use tower_service::Service;

/// Keeps an account of how many HTTP requests are active
pub struct HttpIdle<S, A, B>
where
    S: Service<Request = http::Request<A>, Response = http::Response<B>>,
{
    inner: S,
    idle: Idle,
}

pub struct Respond<F, B>
where
    F: Future<Item = http::Response<B>>,
{
    inner: F,
    idle: Idle,
}

/// Counts the number of active messages to determine idleness.
#[derive(Debug, Default, Clone)]
struct Idle(Arc<AtomicUsize>);

/// A handle that decrements the number of active messages on drop.
#[derive(Debug)]
pub struct Active(Option<Arc<AtomicUsize>>);

impl<S, A, B> From<S> for HttpIdle<S, A, B>
where
    S: Service<Request = http::Request<A>, Response = http::Response<B>>,
{
    fn from(inner: S) -> Self {
        Self {
            inner,
            idle: Idle::default(),
        }
    }
}

// ===== impl HttpIdle =====

impl<S, A, B> Service for HttpIdle<S, A, B>
where
    S: Service<Request = http::Request<A>, Response = http::Response<B>>,
{
    type Request = S::Request;
    type Response = S::Response;
    type Error = S::Error;
    type Future = Respond<S::Future, B>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.inner.poll_ready()
    }

    fn call(&mut self, mut req: Self::Request) -> Self::Future {
        req.extensions_mut().insert(self.idle.active());
        Respond {
            inner: self.inner.call(req),
            idle: self.idle.clone(),
        }
    }
}

impl<F, B> Future for Respond<F, B>
where
    F: Future<Item = http::Response<B>>,
{
    type Item = http::Response<B>;
    type Error = F::Error;

    fn poll(&mut self) -> Poll<http::Response<B>, Self::Error> {
        let mut rsp = try_ready!(self.inner.poll());
        rsp.extensions_mut().insert(self.idle.active());
        Ok(rsp.into())
    }
}

// ===== impl Idle =====

impl Idle {
    pub fn active(&mut self) -> Active {
        self.0.fetch_add(1, Ordering::AcqRel);
        Active(Some(self.0.clone()))
    }
}

// ===== impl Active =====

impl Drop for Active {
    fn drop(&mut self) {
        if let Some(active) = self.0.take() {
            active.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[test]
fn not_idle_while_active() {
    let mut idle = Idle::default();
    let act0 = idle.active();
    assert!(!idle.is_idle());
    let act1 = idle.active();
    assert!(!idle.is_idle());
    drop(act0);
    assert!(!idle.is_idle());
    drop(act1);
    assert!(idle.is_idle());
}
