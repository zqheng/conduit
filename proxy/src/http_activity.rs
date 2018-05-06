use conduit_proxy_router::{IsIdle, TrackActivity};
use futures::{Future, Poll};
use http;
use tower_service::Service;

/// Keeps an account of how many HTTP requests are active
pub struct HttpActivity<S, A, B>
where
    S: Service<Request = http::Request<A>, Response = http::Response<B>>,
{
    inner: S,
    track: TrackActivity,
}

pub struct Respond<F, B>
where
    F: Future<Item = http::Response<B>>,
{
    inner: F,
    track: TrackActivity,
}


impl<S, A, B> From<S> for HttpActivity<S, A, B>
where
    S: Service<Request = http::Request<A>, Response = http::Response<B>>,
{
    fn from(inner: S) -> Self {
        Self {
            inner,
            track: TrackActivity::default(),
        }
    }
}

impl<S, A, B> IsIdle for HttpActivity<S, A, B>
where
    S: Service<Request = http::Request<A>, Response = http::Response<B>>,
{
    fn is_idle(&self) -> bool {
        self.track.is_idle()
    }
}

impl<S, A, B> Service for HttpActivity<S, A, B>
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
        req.extensions_mut().insert(self.track.active());
        Respond {
            inner: self.inner.call(req),
            track: self.track.clone(),
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
        rsp.extensions_mut().insert(self.track.active());
        Ok(rsp.into())
    }
}
