use indexmap::{self, IndexMap};
use std::{hash::Hash, ops::{Deref, DerefMut}, time::{Duration, Instant}};

use Activity;

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
/// - When capacity is not available, `ensure_can_store` computes in O(n) time (average).
pub struct Cache<K, V, N = ()>
where
    K: Clone + Eq + Hash,
    V: Activity,
    N: Now,
{
    /// The cache. The order of the map is not relevant (currently).
    routes: IndexMap<K, Access<V>>,

    /// The maximum number of entries in `routes`.
    capacity: usize,

    /// The maximum age of idle instances that must be retained in the cache.
    max_idle_age: Duration,

    /// The time source.
    now: N,
}

/// An error indicating that capacity has been exhausted.
#[derive(Debug, PartialEq)]
pub struct Exhausted {
    pub capacity: usize,
}

/// Provides the current time to `Cache`. Useful for testing.
pub trait Now: Clone {
    fn now(&self) -> Instant;
}

/// Holds the last-access time of a value.
#[derive(Debug, PartialEq)]
struct Access<V> {
    value: V,
    last_access: Instant,
}

/// A smart pointer that updates an access time wheb dropped.
///
/// Wraps a mutable reference to a `V`-typed value.
///
/// When the guard is dropped, the value's `last_access` time is updated with the provided
/// time source.
pub struct AccessGuard<'a, V: 'a, N: Now + 'a> {
    access: &'a mut Access<V>,
    now: &'a N,
}

// ===== impl Cache =====

impl<K, V> Cache<K, V, ()>
where
    K: Clone + Eq + Hash,
    V: Activity,
{
    pub fn new(capacity: usize, max_idle_age: Duration) -> Self {
        Self {
            routes: IndexMap::default(),
            capacity,
            max_idle_age,
            now: (),
        }
    }

    /// Overrides the time source for tests.
    #[cfg(test)]
    fn with_clock<M: Now>(self, now: M) -> Cache<K, V, M> {
        Cache {
            now,
            routes: self.routes,
            capacity: self.capacity,
            max_idle_age: self.max_idle_age,
        }
    }
}

impl<K, V, N> Cache<K, V, N>
where
    K: Clone + Eq + Hash,
    V: Activity,
    N: Now,
{
    /// Accesses a route.
    ///
    /// A mutable reference the route is wrapped in the returned `AccessGuard` to ensure
    /// that the access-time is updated when the reference is released.
    pub fn access<'a, Q>(&'a mut self, key: &Q) -> Option<AccessGuard<'a, V, N>>
    where
        Q: Hash + indexmap::Equivalent<K>,
    {
        let route = self.routes.get_mut(key)?;
        let access = route.access(&self.now);
        Some(access)
    }

    /// Stores a route in the cache.
    ///
    /// If the cache is full, idle routes may be evicted to create space for the new
    /// route. If no capacity can be reclaimed, an error is returned.
    pub fn store<R: Into<V>>(&mut self, key: K, route: R) -> Result<(), Exhausted> {
        self.ensure_can_store()?;

        self.routes.insert(key, Access{
            value: route.into(),
            last_access: self.now.now(),
        });

        Ok(())
    }

    /// Returns the number of additional routes that may be stored.
    ///
    /// If there are no available routes, try to evict idle routes to create capacity.
    pub fn ensure_can_store(&mut self) -> Result<usize, Exhausted> {
        let mut avail = self.capacity - self.routes.len();

        if avail == 0 {
            self.retain_active_and_recent_routes();

            avail = self.capacity - self.routes.len();
            if avail == 0 {
                return Err(Exhausted { capacity: self.capacity });
            }
        }

        Ok(avail)
    }

    /// Drops all routes that are idle and have not been accessed in
    /// `max_idle_age`.
    fn retain_active_and_recent_routes(&mut self) {
        let epoch = self.now.now() - self.max_idle_age;
        self.routes.retain(|_, &mut Access{value: ref route, last_access}| {
            epoch < last_access || !route.is_idle()
        });
    }
}

// ===== impl Access =====

impl<V> Access<V> {
    fn access<'a, N: Now + 'a>(&'a mut self, now: &'a N) -> AccessGuard<'a, V, N> {
        AccessGuard { now, access: self, }
    }
}

// ===== impl AccessGuard =====

impl<'a, V: 'a, N: Now + 'a> Deref for AccessGuard<'a, V, N> {
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &self.access.value
    }
}

impl<'a, V: 'a, N: Now + 'a> DerefMut for AccessGuard<'a, V, N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.access.value
    }
}

impl<'a, V: 'a, N: Now + 'a> Drop for AccessGuard<'a, V, N> {
    fn drop(&mut self) {
        self.access.last_access = self.now.now();
    }
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
    use super::*;
    use futures::Future;
    use std::{cell::RefCell, rc::Rc, time::{Duration, Instant}};
    use tests::MultiplyAndAssign;
    use tower_service::Service;

    #[derive(Clone)]
    struct Clock(Rc<RefCell<Instant>>);
    impl Default for Clock {
        fn default() -> Clock {
            Clock(Rc::new(RefCell::new(Instant::now())))
        }
    }
    impl Clock {
        fn advance(&self, d: Duration) {
            *self.0.borrow_mut() += d;
        }
    }
    impl Now for Clock {
        fn now(&self) -> Instant {
            self.0.borrow().clone()
        }
    }

    #[test]
    fn ensure_can_store_preserves_active_route() {
        let mut cache = Cache::<usize, MultiplyAndAssign>::new(1, Duration::from_secs(0));

        let mut service = MultiplyAndAssign::default();
        let mut rsp = service.call(1.into()).wait().unwrap();

        cache.store(1, service).unwrap();
        assert_eq!(cache.routes.len(), 1);

        assert_eq!(cache.ensure_can_store(), Err(Exhausted { capacity: 1 }));
        assert_eq!(cache.routes.len(), 1);

        rsp.active.take();
        assert_eq!(cache.ensure_can_store(), Ok(1));
        assert_eq!(cache.routes.len(), 0);
    }

    #[test]
    fn ensure_can_store_after_max_idle_age() {
        let clock = Clock::default();
        let mut cache = Cache::<usize, MultiplyAndAssign>::new(1, Duration::from_secs(10))
            .with_clock(clock.clone());

        let rsp = {
            let mut service = MultiplyAndAssign::default();
            let rsp = service.call(1.into()).wait().unwrap();
            cache.store(1, service).unwrap();
            rsp
        };

        assert_eq!(cache.ensure_can_store(), Err(Exhausted { capacity: 1 }));
        assert_eq!(cache.routes.len(), 1);

        clock.advance(Duration::from_secs(5));
        drop(rsp);
        assert_eq!(cache.ensure_can_store(), Err(Exhausted { capacity: 1 }));
        assert_eq!(cache.routes.len(), 1);

        clock.advance(Duration::from_secs(6));
        assert_eq!(cache.ensure_can_store(), Ok(1));
        assert_eq!(cache.routes.len(), 0);
    }

    #[test]
    fn ensure_can_store_does_nothing_when_capacity_exists() {
        let clock = Clock::default();
        let mut cache = Cache::<usize, MultiplyAndAssign>::new(2, Duration::from_secs(10))
            .with_clock(clock.clone());

        // Create a route that goes idle immediately:
        let rsp = {
            let mut service = MultiplyAndAssign::default();
            let rsp = service.call(1.into()).wait().unwrap();
            cache.store(1, service).unwrap();
            rsp
        };
        assert_eq!(cache.routes.len(), 1);

        drop(rsp);
        assert_eq!(cache.ensure_can_store(), Ok(1));
        assert_eq!(cache.routes.len(), 1);

        // Some time later, create another route that does _not_ go idle:
        clock.advance(Duration::from_secs(5));
        let rsp = {
            let mut service = MultiplyAndAssign::default();
            let rsp = service.call(2.into()).wait().unwrap();
            cache.store(2, service).unwrap();
            rsp
        };
        assert_eq!(cache.ensure_can_store(), Err(Exhausted { capacity: 2 }));
        assert_eq!(cache.routes.len(), 2);

        // The first route should expire now:
        clock.advance(Duration::from_secs(6));
        assert_eq!(cache.ensure_can_store(), Ok(1));
        assert_eq!(cache.routes.len(), 1);

        // The second route should expire now; but it's still active, so it shouldn't be
        // dropped:
        clock.advance(Duration::from_secs(5));
        assert_eq!(cache.ensure_can_store(), Ok(1));
        assert_eq!(cache.routes.len(), 1);

        // Once the route is no longer active, it can be dropped; but since there's
        // available capacity it is not.
        drop(rsp);
        assert_eq!(cache.ensure_can_store(), Ok(1));
        assert_eq!(cache.routes.len(), 1);
    }
}
