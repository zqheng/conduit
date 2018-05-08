extern crate futures;
extern crate indexmap;
extern crate tower_service;

use std::hash::Hash;
use tower_service::Service;

pub mod access;
mod cache;
mod router;
mod single;

pub use self::access::{Access, Now};
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

#[cfg(test)]
mod test_util {
    use futures::{Poll, future};
    use std::{cell::RefCell, rc::Rc, time::{Duration, Instant}};
    use tower_service::Service;

    use access;

    /// An implementation of Recognize that binds with MultiplyAndAssign.
    #[derive(Clone)]
    pub struct Recognize;

    /// A Service that updates its internal value with the product of that and a request's
    /// value.
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

    /// A mocked instance of `Now` to drive tests.
    #[derive(Clone)]
    pub struct Clock(Rc<RefCell<Instant>>);

    // ===== impl Recognize =====

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

    // ===== impl MultiplyAndAssign =====

    impl Default for MultiplyAndAssign {
        fn default() -> Self {
            Self { value: 1 }
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

    // ===== impl Request =====

    impl From<usize> for Request {
        fn from(n: usize) -> Self {
            Request::Recgonized(n)
        }
    }

    // ===== impl Clock =====

    impl Default for Clock {
        fn default() -> Clock {
            Clock(Rc::new(RefCell::new(Instant::now())))
        }
    }

    impl Clock {
        pub fn advance(&mut self, d: Duration) {
            *self.0.borrow_mut() += d;
        }
    }

    impl access::Now for Clock {
        fn now(&self) -> Instant {
            self.0.borrow().clone()
        }
    }
}
