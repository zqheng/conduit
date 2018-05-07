use std::hash::Hash;

use access;
use retain::Retain;

/// An LRU cache for routes.
///
/// ## Assumptions
///
/// - `access` is common;
/// - `store` is less common;
/// - `capacity` is large enough that idle routes need not be removed frequently.
///
/// ## Complexity
///
/// - `access` computes in O(1) time (amortized average).
/// - `store` computes in O(1) time (average) when capacity is available.
/// - When capacity is not available, `reserve` computes in O(n) time (average).
pub struct Cache<K, V, R, N = ()>
where
    K: Clone + Eq + Hash,
    R: Retain<V>,
    N: access::Now,
{
    /// A cache that retains the last access time of each target.
    routes: access::AccessMap<K, V>,

    /// The maximum number of entries in `routes`.
    capacity: usize,

    retain: R,

    /// The time source.
    now: N,
}

/// An error indicating that capacity has been exhausted.
#[derive(Debug, PartialEq)]
pub struct Exhausted {
    pub capacity: usize,
}

// ===== impl Cache =====

impl<K, V, R> Cache<K, V, R, ()>
where
    K: Clone + Eq + Hash,
    R: Retain<V>,
{
    pub fn new(capacity: usize, retain: R) -> Self {
        Self {
            routes: access::AccessMap::default(),
            capacity,
            retain,
            now: (),
        }
    }
}


impl<K, V, R, N> Cache<K, V, R, N>
where
    K: Clone + Eq + Hash,
    R: Retain<V>,
    N: access::Now,
{
    /// Accesses a route.
    ///
    /// A mutable reference to the route is wrapped in the returned `Access` to
    /// ensure that the access-time is updated when the reference is released.
    pub fn access<'a, Q>(&'a mut self, key: &Q) -> Option<access::Access<'a, V, N>>
    where
        Q: Hash + access::Equivalent<K>,
    {
        let route = self.routes.get_mut(key)?;
        let access = route.access(&self.now);
        Some(access)
    }

    /// Stores a route in the cache.
    ///
    /// If the cache is full, idle routes may be evicted to create space for the new
    /// route. If no capacity can be reclaimed, an error is returned.
    pub fn store<U: Into<V>>(&mut self, key: K, route: U) -> Result<(), Exhausted> {
        self.reserve()?;
        self.routes.insert(key, access::Node::new(route.into(), self.now.now()));
        Ok(())
    }

    /// Ensures that there is capacity to store an additional route.
    ///
    /// Returns the number of additional routes that may be stored. If there are no
    /// available routes, idle routes may be evicted to create capacity. If capacity
    /// cannot be created, then an error is returned.
    pub fn reserve(&mut self) -> Result<usize, Exhausted> {
        let mut avail = self.capacity - self.routes.len();

        if avail == 0 {
            let retain = &self.retain;
            self.routes.retain(|_, ref t| retain.retain(t));

            avail = self.capacity - self.routes.len();
            if avail == 0 {
                return Err(Exhausted { capacity: self.capacity });
            }
        }

        Ok(avail)
    }

    /// Overrides the time source for tests.
    #[cfg(test)]
    fn with_clock<M: access::Now>(self, now: M) -> Cache<K, V, R, M> {
        Cache {
            now,
            routes: self.routes,
            capacity: self.capacity,
            retain: self.retain,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::Future;
    use std::{cell::RefCell, rc::Rc, time::Duration};
    use tower_service::Service;

    use {retain, Now};
    use test_util::{Clock, MultiplyAndAssign};
    use super::*;

    #[test]
    fn reserve_honors_retain() {
        pub struct Bool(Rc<RefCell<bool>>);
        impl<T> Retain<T> for Bool {
            fn retain(&self, _: &access::Node<T>) -> bool {
                *self.0.borrow()
            }
        }

        let retain = Rc::new(RefCell::new(true));
        let mut cache =
            Cache::<usize, MultiplyAndAssign, _>::new(1, Bool(retain.clone()));

        let mut service = MultiplyAndAssign::default();
        service.call(1.into()).wait().unwrap();

        cache.store(1, service).unwrap();
        assert_eq!(cache.routes.len(), 1);

        assert_eq!(cache.reserve(), Err(Exhausted { capacity: 1 }));
        assert_eq!(cache.routes.len(), 1);

        *retain.borrow_mut() = false;
        assert_eq!(cache.reserve(), Ok(1));
        assert_eq!(cache.routes.len(), 0);
    }

    #[test]
    fn reserve_does_nothing_when_capacity_exists() {
        let mut cache = Cache::<_, MultiplyAndAssign, _, _>::new(2, retain::NEVER);

        // Create a route that goes idle immediately:
        {
            let mut service = MultiplyAndAssign::default();
            service.call(1.into()).wait().unwrap();
            cache.store(1, service).unwrap();
        };
        assert_eq!(cache.routes.len(), 1);

        assert_eq!(cache.reserve(), Ok(1));
        assert_eq!(cache.routes.len(), 1);
    }

    #[test]
    fn tracks_access() {
        let mut clock = Clock::default();
        let mut cache = Cache::<_, MultiplyAndAssign, _, _>::new(1, retain::ALWAYS)
            .with_clock(clock.clone());

        let t0 = clock.now();
        cache.store(123, MultiplyAndAssign::default()).unwrap();

        clock.advance(Duration::from_secs(1));
        let t1 = clock.now();
        {
            let access = cache.access(&123).unwrap();
            assert_eq!(access.last_access(), t0);
        }

        clock.advance(Duration::from_secs(1));
        {
            let access = cache.access(&123).unwrap();
            assert_eq!(access.last_access(), t1);
        }
    }
}
