extern crate futures;
extern crate indexmap;
extern crate tower_service;

use futures::{Future, Poll};
use std::{
    error, fmt, mem,
    hash::Hash,
    sync::{Arc, Mutex},
    time::Duration,
};
use tower_service::Service;

pub mod idle;
mod cache;

pub use self::idle::{Active, Idle, IsIdle};
use self::cache::Cache;

/// Routes requests based on a configurable `Key`.
pub struct Router<T>
where
    T: Recognize,
    T::Service: IsIdle,
{
    cache: Arc<Mutex<Cache<T::Key, T::Service>>>,
    recognize: T,
}

/// Provides a strategy for routing a Request to a Service.
///
/// Implementors must provide a `Key` type that identifies each unique route. The
/// `recognize()` method is used to determine the key for a given request. This key is
/// used to look up a route in a cache (i.e. in `Router`), or can be passed to
/// `bind_service` to instantiate the identified route.
pub trait Recognize {
    /// Requests handled by the discovered services
    type Request;

    /// Responses given by the discovered services
    type Response;

    /// Errors produced by the discovered services
    type Error;

    /// Identifies a Route.
    type Key: Clone + Eq + Hash;

    /// Error produced by failed routing
    type RouteError;

    /// A route.
    type Service: Service<
        Request = Self::Request,
        Response = Self::Response,
        Error = Self::Error
    >;

    /// Determines the key for a route to handle the given request.
    fn recognize(&self, req: &Self::Request) -> Option<Self::Key>;

    /// Return a `Service` to handle requests.
    ///
    /// The returned service must always be in the ready state (i.e.
    /// `poll_ready` must always return `Ready` or `Err`).
    fn bind_service(&mut self, key: &Self::Key) -> Result<Self::Service, Self::RouteError>;
}

pub struct Single<S>(Option<S>);

#[derive(Debug, PartialEq)]
pub enum Error<T, U> {
    Inner(T),
    Route(U),
    NoCapacity(usize),
    NotRecognized,
}

pub struct ResponseFuture<T>
where
    T: Recognize,
    T::Service: IsIdle,
{
    state: State<T>,
}

enum State<T>
where
    T: Recognize,
    T::Service: IsIdle,
{
    Inner(<T::Service as Service>::Future),
    RouteError(T::RouteError),
    NoCapacity(usize),
    NotRecognized,
    Invalid,
}

// ===== impl Router =====

impl<T> Router<T>
where
    T: Recognize,
    T::Service: IsIdle,
{
    pub fn new(recognize: T, capacity: usize, min_idle_age: Duration) -> Self {
        Self {
            recognize,
            cache: Arc::new(Mutex::new(Cache::new(capacity, min_idle_age))),
        }
    }
}

impl<T> Clone for Router<T>
where
    T: Recognize + Clone,
    T::Service: IsIdle,
{
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            recognize: self.recognize.clone(),
        }
    }
}

impl<T> Service for Router<T>
where
    T: Recognize,
    T::Service: IsIdle,
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
    ///
    /// TODO Attempt to free capacity in the router.
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
        if let Err(cache::Exhausted { capacity }) = cache.ensure_can_store() {
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

// ===== impl Single =====

impl<S: Service> Single<S> {
    pub fn new(svc: S) -> Self {
        Single(Some(svc))
    }
}

impl<S: Service> Recognize for Single<S> {
    type Request = S::Request;
    type Response = S::Response;
    type Error = S::Error;
    type Key = ();
    type RouteError = ();
    type Service = S;

    fn recognize(&self, _: &Self::Request) -> Option<Self::Key> {
        Some(())
    }

    fn bind_service(&mut self, _: &Self::Key) -> Result<S, Self::RouteError> {
        Ok(self.0.take().expect("static route bound twice"))
    }
}

// ===== impl ResponseFuture =====

impl<T> ResponseFuture<T>
where
    T: Recognize,
    T::Service: IsIdle,
{
    fn new(inner: <T::Service as Service>::Future) -> Self {
        ResponseFuture { state: State::Inner(inner) }
    }

    fn not_recognized() -> Self {
        ResponseFuture { state: State::NotRecognized }
    }

    fn no_capacity(capacity: usize) -> Self {
        ResponseFuture { state: State::NoCapacity(capacity) }
    }

    fn route_err(e: T::RouteError) -> Self {
        ResponseFuture { state: State::RouteError(e) }
    }
}

impl<T> Future for ResponseFuture<T>
where
    T: Recognize,
    T::Service: IsIdle,
{
    type Item = T::Response;
    type Error = Error<T::Error, T::RouteError>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        use self::State::*;

        match self.state {
            Inner(ref mut fut) => fut.poll().map_err(Error::Inner),
            RouteError(..) => {
                match mem::replace(&mut self.state, Invalid) {
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
    use futures::{Poll, Future, future};
    use std::time::Duration;
    use tower_service::Service;
    use super::{Error, Router};

    #[derive(Clone)]
    pub struct Recognize;

    pub struct MultiplyAndAssign {
        value: usize,
        idle: super::Idle
    }

    pub enum Request {
        NotRecognized,
        Recgonized(usize),
    }

    #[derive(Debug)]
    pub struct Response {
        pub value: usize,
        pub active: Option<super::Active>,
    }

    impl super::Recognize for Recognize {
        type Request = Request;
        type Response = Response;
        type Error = ();
        type Key = usize;
        type RouteError = ();
        type Service = MultiplyAndAssign;

        fn recognize(&self, req: &Self::Request) -> Option<Self::Key> {
            match *req {
                Request::NotRecognized => None,
                Request::Recgonized(n) => Some(n),
            }
        }

        fn bind_service(&mut self, _: &Self::Key) -> Result<Self::Service, Self::RouteError> {
            Ok(MultiplyAndAssign::default())
        }
    }

    impl Service for MultiplyAndAssign {
        type Request = Request;
        type Response = Response;
        type Error = ();
        type Future = future::FutureResult<Response, ()>;

        fn poll_ready(&mut self) -> Poll<(), ()> {
            unimplemented!()
        }

        fn call(&mut self, req: Self::Request) -> Self::Future {
            let n = match req {
                Request::NotRecognized => unreachable!(),
                Request::Recgonized(n) => n,
            };
            self.value *= n;

            let active = Some(self.idle.active());
            future::ok(Response { active, value: self.value })
        }
    }

    impl Default for MultiplyAndAssign {
        fn default() -> Self {
            Self {
                value: 1,
                idle: super::Idle::default(),
            }
        }
    }

    impl super::IsIdle for MultiplyAndAssign {
        fn is_idle(&self) -> bool {
            self.idle.is_idle()
        }
    }

    impl From<usize> for Request {
        fn from(n: usize) -> Self {
            Request::Recgonized(n)
        }
    }

    impl Router<Recognize> {
        fn call_ok(&mut self, req: Request) -> Response {
            self.call(req).wait().expect("should route")
        }

        fn call_err(&mut self, req: Request) -> super::Error<(), ()> {
            self.call(req).wait().expect_err("should not route")
        }
    }

    #[test]
    fn invalid() {
        let mut router = Router::new(Recognize, 1, Duration::from_secs(0));

        let err = router.call_err(Request::NotRecognized);
        assert_eq!(err, Error::NotRecognized);
    }

    #[test]
    fn cache_reuses_routes() {
        let mut router = Router::new(Recognize, 1, Duration::from_secs(0));

        let rsp = router.call_ok(2.into());
        assert_eq!(rsp.value, 2);

        let rsp = router.call_ok(2.into());
        assert_eq!(rsp.value, 4);
    }

    #[test]
    fn cache_limited_by_capacity() {
        let mut router = Router::new(Recognize, 1, Duration::from_secs(0));

        // Holding this response prevents the route from being evicted.
        let rsp = router.call_ok(2.into());
        assert_eq!(rsp.value, 2);

        let err = router.call_err(3.into());
        assert_eq!(err, Error::NoCapacity(1));
    }

    #[test]
    fn cache_reclaims_idle_capacity() {
        let mut router = Router::new(Recognize, 1, Duration::from_secs(0));

        let rsp = router.call_ok(2.into());
        assert_eq!(rsp.value, 2);
        drop(rsp);

        let rsp = router.call_ok(3.into());
        assert_eq!(rsp.value, 3);
    }
}
