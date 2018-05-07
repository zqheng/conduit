use futures::{Future, Poll};
use std::{error, fmt, mem, sync::{Arc, Mutex}};
use tower_service::Service;

use Recognize;
use cache::{self, Cache};
use retain::Retain;

/// Routes requests based on a configurable `Key`.
pub struct Router<T, R>
where
    T: Recognize,
    R: Retain<T::Service>,
{
    cache: Arc<Mutex<Cache<T::Key, T::Service, R>>>,
    recognize: T,
}

#[derive(Debug, PartialEq)]
pub enum Error<T, U> {
    Inner(T),
    Route(U),
    NoCapacity(usize),
    NotRecognized,
}

pub struct ResponseFuture<T: Recognize>(State<T>);

enum State<T: Recognize> {
    Inner(<T::Service as Service>::Future),
    RouteError(T::RouteError),
    NoCapacity(usize),
    NotRecognized,
    Invalid,
}

// ===== impl Router =====

impl<T: Recognize, R: Retain<T::Service>> Router<T, R> {
    pub fn new(recognize: T, capacity: usize, retain: R) -> Self {
        let cache = Arc::new(Mutex::new(Cache::new(capacity, retain)));
        Self { cache, recognize }
    }
}

impl<T, R> Clone for Router<T, R>
where
    T: Recognize + Clone,
    R: Retain<T::Service>
{
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            recognize: self.recognize.clone(),
        }
    }
}

impl<T, R> Service for Router<T, R>
where
    T: Recognize,
    R: Retain<T::Service>
{
    type Request = T::Request;
    type Response = T::Response;
    type Error = Error<T::Error, T::RouteError>;
    type Future = ResponseFuture<T>;

    /// Always ready to serve.
    ///
    /// Graceful backpressure is **not** supported at this level, since each request may
    /// be routed to different resources. Instead, requests should be issued and each
    /// route should support a queue of requests.
    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(().into())
    }

    /// Routes the request through an underlying service.
    ///
    /// The response fails when the request cannot be routed.
    fn call(&mut self, request: Self::Request) -> Self::Future {
        let key = match self.recognize.recognize(&request) {
            Some(key) => key,
            None => return ResponseFuture::not_recognized(),
        };


        let cache = &mut *self.cache.lock().expect("router lock");

        // First, try to load a cached route for `key`.
        if let Some(mut svc) = cache.access(&key) {
            return ResponseFuture::new(svc.call(request));
        }

        // Since there wasn't a cached route, ensure that there is capacity for a
        // new one.
        if let Err(cache::Exhausted { capacity }) = cache.reserve() {
            return ResponseFuture::no_capacity(capacity);
        }

        // Bind a new route, send the request on the route, and cache the route.
        let mut service = match self.recognize.bind_service(&key) {
            Ok(s) => s,
            Err(e) => return ResponseFuture::route_err(e),
        };

        let response = service.call(request);
        cache.store(key, service).expect("cache capacity");
        ResponseFuture::new(response)
    }
}

// ===== impl ResponseFuture =====

impl<T: Recognize> ResponseFuture<T> {
    fn new(inner: <T::Service as Service>::Future) -> Self {
        ResponseFuture(State::Inner(inner))
    }

    fn not_recognized() -> Self {
        ResponseFuture(State::NotRecognized)
    }

    fn no_capacity(capacity: usize) -> Self {
        ResponseFuture(State::NoCapacity(capacity))
    }

    fn route_err(e: T::RouteError) -> Self {
        ResponseFuture(State::RouteError(e))
    }
}

impl<T: Recognize> Future for ResponseFuture<T> {
    type Item = T::Response;
    type Error = Error<T::Error, T::RouteError>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        use self::State::*;

        match self.0 {
            Inner(ref mut fut) => fut.poll().map_err(Error::Inner),
            RouteError(..) => {
                match mem::replace(&mut self.0, Invalid) {
                    RouteError(e) => Err(Error::Route(e)),
                    _ => unreachable!(),
                }
            }
            NotRecognized => Err(Error::NotRecognized),
            NoCapacity(capacity) => Err(Error::NoCapacity(capacity)),
            Invalid => panic!(),
        }
    }
}

// ===== impl Error =====

impl<T, U> fmt::Display for Error<T, U>
where
    T: fmt::Display,
    U: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Inner(ref why) => why.fmt(f),
            Error::Route(ref why) => write!(f, "route recognition failed: {}", why),
            Error::NoCapacity(capacity) => write!(f, "router capacity reached ({})", capacity),
            Error::NotRecognized => f.pad("route not recognized"),
        }
    }
}

impl<T, U> error::Error for Error<T, U>
where
    T: error::Error,
    U: error::Error,
{
    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::Inner(ref why) => Some(why),
            Error::Route(ref why) => Some(why),
            _ => None,
        }
    }

    fn description(&self) -> &str {
        match *self {
            Error::Inner(_) => "inner service error",
            Error::Route(_) => "route recognition failed",
            Error::NoCapacity(_) => "router capacity reached",
            Error::NotRecognized => "route not recognized",
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::Future;
    use tower_service::Service;

    use test_util::{MultiplyAndAssign, Recognize, Request, Response};
    use {retain, Error, Retain, Router};

    // ===== impl Router =====

    impl<R: Retain<MultiplyAndAssign>> Router<Recognize, R> {
        pub fn call_ok(&mut self, req: Request) -> Response {
            self.call(req).wait().expect("should route")
        }

        pub fn call_err(&mut self, req: Request) -> super::Error<(), ()> {
            self.call(req).wait().expect_err("should not route")
        }
    }

    #[test]
    fn invalid() {
        let mut router = Router::new(Recognize, 1, retain::ALWAYS);

        let err = router.call_err(Request::NotRecognized);
        assert_eq!(err, Error::NotRecognized);
    }

    #[test]
    fn reuses_routes() {
        let mut router = Router::new(Recognize, 1, retain::ALWAYS);

        let rsp = router.call_ok(2.into());
        assert_eq!(rsp.value, 2);

        let rsp = router.call_ok(2.into());
        assert_eq!(rsp.value, 4);
    }

    #[test]
    fn limited_by_capacity() {
        let mut router = Router::new(Recognize, 1, retain::ALWAYS);

        // Holding this response prevents the route from being evicted.
        let rsp = router.call_ok(2.into());
        assert_eq!(rsp.value, 2);

        let err = router.call_err(3.into());
        assert_eq!(err, Error::NoCapacity(1));
    }

    #[test]
    fn reclaims_idle_capacity() {
        let mut router = Router::new(Recognize, 1, retain::NEVER);

        let rsp = router.call_ok(2.into());
        assert_eq!(rsp.value, 2);

        let rsp = router.call_ok(3.into());
        assert_eq!(rsp.value, 3);
    }
}
