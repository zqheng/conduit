use indexmap::IndexMap;
use std::{hash::Hash, time::{Duration, Instant}};

use Activity;

/// Caches routes.
pub struct Cache<K, R, N = ()>
where
    K: Clone + Eq + Hash,
    R: Activity,
    N: Now,
{
    routes: IndexMap<K, Access<R>>,
    min_idle_age: Duration,
    capacity: usize,
    now: N,
}

#[derive(Debug)]
pub struct OutOfCapacity {
    pub capacity: usize,
}

/// Provides the current time to `Cache`. Useful for testing.
pub trait Now {
    fn now(&self) -> Instant;
}

#[derive(Debug, PartialEq)]
struct Access<R> {
    route: R,
    last_access: Instant,
}

// ===== impl Now =====

impl Now for () {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

// ===== impl Access =====

impl<R> Access<R> {
    fn access(&mut self, now: Instant) -> &mut R {
        self.last_access = now;
        &mut self.route
    }
}

// ===== impl Cache =====

impl<K, R> Cache<K, R, ()>
where
    K: Clone + Eq + Hash,
    R: Activity,
{
    pub fn new(capacity: usize, min_idle_age: Duration) -> Self {
        Self {
            routes: IndexMap::default(),
            capacity,
            min_idle_age,
            now: (),
        }
    }

    #[cfg(test)]
    fn with_now<M: Now>(self, now: M) -> Cache<K, R, M> {
        Cache {
            now,
            routes: self.routes,
            capacity: self.capacity,
            min_idle_age: self.min_idle_age,
        }
    }
}

impl<K, R, N> Cache<K, R, N>
where
    K: Clone + Eq + Hash,
    R: Activity,
    N: Now,
{
    /// The total number of routes the Cache may store.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The total number of additional routes the Cache may store.
    pub fn available_capacity(&self) -> usize {
        self.capacity - self.routes.len()
    }

    /// Evicts all idle routes that have not been accessed in at least `min_idle_age`.
    pub fn evict_idle_routes(&mut self) {
        let epoch = self.now.now() - self.min_idle_age;

        // Sort the routes from most-recently used to least-recently-used. We want to
        // leave the least-recently-used near the end so it's minimally-disturbing to
        // remove them. (We have to remove items in reverse-order to avoid disturbing the
        // captured indices.)
        self.routes.sort_by(|_, ref a, _, ref b| b.last_access.cmp(&a.last_access));

        let remove_indices = {
            // Iterate through the indexed routes from least-to-most recently used.
            let least_to_most = self.routes.values().enumerate().rev();

            let old_enough = least_to_most
                .take_while(|&(_, &Access{last_access, ..})| last_access <= epoch);

            let idle = old_enough
                .filter(|&(_, &Access{ref route, ..})| route.is_idle());

            idle.map(|(i, _)| i).collect::<Vec<usize>>()
        };

        for i in &remove_indices {
            self.routes.swap_remove_index(*i);
        }
    }

    pub fn get_route(&mut self, key: &K) -> Option<&mut R> {
        let now = self.now.now();
        self.routes.get_mut(key).map(|r| r.access(now))
    }

    pub fn add_route(&mut self, key: K, route: R) -> Result<(), OutOfCapacity> {
        if self.available_capacity() == 0 {
            self.evict_idle_routes();
            if self.available_capacity() == 0 {
                return Err(OutOfCapacity { capacity: self.capacity })
            }
        }

        let last_access = self.now.now();
        self.routes.insert(key, Access{route, last_access});
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::Future;
    use std::time::{Duration, Instant};
    use tests::MultiplyAndAssign;
    use tower_service::Service;

    #[test]
    fn evict_idle_routes_preserves_active_route() {
        let mut cache = Cache::new(1, Duration::from_secs(0));

        let mut service = MultiplyAndAssign::default();
        let mut rsp = service.call(1.into()).wait().unwrap();

        cache.add_route(1, service).unwrap();
        assert_eq!(cache.routes.len(), 1);

        cache.evict_idle_routes();
        assert_eq!(cache.routes.len(), 1);

        rsp.active.take();
        cache.evict_idle_routes();
        assert_eq!(cache.routes.len(), 0);
    }

    #[test]
    fn evict_idle_routes_after_min_time() {
        use std::cell::RefCell;
        use std::rc::Rc;

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

        let clock = Clock::default();
        let mut cache = Cache::new(2, Duration::from_secs(10)).with_now(clock.clone());

        // Create a route that goes idle immediately:
        let rsp = {
            let mut service = MultiplyAndAssign::default();
            let rsp = service.call(1.into()).wait().unwrap();
            cache.add_route(1, service).unwrap();
            rsp
        };
        assert_eq!(cache.routes.len(), 1);

        drop(rsp);
        cache.evict_idle_routes();
        assert_eq!(cache.routes.len(), 1);

        // Some time later, create another route that does _not_ go idle:
        clock.advance(Duration::from_secs(5));
        let rsp = {
            let mut service = MultiplyAndAssign::default();
            let rsp = service.call(2.into()).wait().unwrap();
            cache.add_route(2, service).unwrap();
            rsp
        };
        assert_eq!(cache.routes.len(), 2);

        // The first route should expire now:
        clock.advance(Duration::from_secs(6));
        cache.evict_idle_routes();
        assert_eq!(cache.routes.len(), 1);

        // The second route should expire now; but it's still active, so it shouldn't be
        // dropped:
        clock.advance(Duration::from_secs(5));
        cache.evict_idle_routes();
        assert_eq!(cache.routes.len(), 1);

        // Once the route is no longer active, it can be dropped:
        drop(rsp);
        cache.evict_idle_routes();
        assert_eq!(cache.routes.len(), 0);
    }
}
