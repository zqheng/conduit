use conduit_proxy_router::{IsIdle, Idle};
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

impl<S, A, B> IsIdle for HttpIdle<S, A, B>
where
    S: Service<Request = http::Request<A>, Response = http::Response<B>>,
{
    fn is_idle(&self) -> bool {
        self.idle.is_idle()
    }
}

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
