extern crate futures;
extern crate indexmap;
extern crate tower_service;

use std::{hash::Hash, time::Instant};
use tower_service::Service;

mod access;
mod cache;
pub mod retain;
mod router;
mod single;

pub use self::access::Access;
pub use self::retain::Retain;
pub use self::router::{Error, Router};
pub use self::single::Single;

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

/// Provides the current time within the module. Useful for testing.
pub trait Now {
    fn now(&self) -> Instant;
}

// ===== impl Now =====

/// Default source of time.
impl Now for () {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[cfg(test)]
mod tests {
    use futures::{Poll, Future, future};
    use tower_service::Service;

    use {retain, Error, Retain, Router};

    #[derive(Clone)]
    pub struct Recognize;

    pub struct MultiplyAndAssign {
        value: usize,
    }

    pub enum Request {
        NotRecognized,
        Recgonized(usize),
    }

    #[derive(Debug)]
    pub struct Response {
        pub value: usize,
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

            future::ok(Response { value: self.value })
        }
    }

    impl Default for MultiplyAndAssign {
        fn default() -> Self {
            Self { value: 1 }
        }
    }

    impl From<usize> for Request {
        fn from(n: usize) -> Self {
            Request::Recgonized(n)
        }
    }

    impl<R: Retain<MultiplyAndAssign>> Router<Recognize, R> {
        fn call_ok(&mut self, req: Request) -> Response {
            self.call(req).wait().expect("should route")
        }

        fn call_err(&mut self, req: Request) -> super::Error<(), ()> {
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
