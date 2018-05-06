use indexmap::IndexMap;
use std::{
    collections::VecDeque,
    hash::Hash,
    time::{Duration, Instant},
};

use ::Activity;

/// Caches routes.
///
///
pub struct Cache<K, R, N = ()>
where
    K: Clone + Eq + Hash,
    R: Activity,
    N: Now
{
    routes: IndexMap<K, R>,
    last_used: LastUsed<K>,
    min_idle_age: Duration,
    capacity: usize,
    now: N,
}

/// Provides the current time to `Cache`. Useful for testing.
pub trait Now {
    fn now(&self) -> Instant;
}

/// Stores `K` typed keys in access-order.
///
/// The least-recently-used key is stored at index 0. The most-recently-used key is stored
/// at the highest index.
type LastUsed<K> = VecDeque<Used<K>>;

#[derive(Debug, PartialEq)]
struct Used<K> {
    key: K,
    at: Instant,
}

// ===== impl Now =====

impl Now for () {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

// ===== impl Used =====

impl<K> From<K> for Used<K> {
    fn from(key: K) -> Self {
        Self {
            key,
            at: Instant::now(),
        }
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
            last_used: LastUsed::new(),
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
            last_used: self.last_used,
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

    /// Evicts the oldest idle route.
    ///
    /// Iterates through routes from least- to most-recently used. If a route not used in
    /// `min_idle_age` has no active
    pub fn evict_oldest_idle_route(&mut self) -> Option<(K, R)> {
        for idx in 0..self.last_used.len() {
            let idle = {
                let &Used{ref key, ref at} = &self.last_used[idx];

                // Since `last_used` is stored in time-order, give up searching once we've
                // exhausted keys that could potentially be considered as idle.
                if *at > self.now.now() - self.min_idle_age {
                    break;
                }

                self.routes.get(key)
                    .expect("key in last_used must also exist in routes")
                    .is_idle()
            };

            if idle {
                Self::move_to_end(&mut self.last_used, idx);
                let Used{key, ..} = self.last_used.pop_back().unwrap();
                let route = self.routes.remove(&key).unwrap();
                return Some((key, route));
            }
        }

        None
    }

    pub fn get_route(&mut self, key: &K) -> Option<&mut R> {
        match self.routes.get_mut(key) {
            None => None,
            Some(svc) => {
                Self::mark_used(&mut self.last_used, key, self.now.now());
                Some(svc)
            }
        }
    }

    pub fn add_route(&mut self, key: K, route: R) {
        debug_assert!(self.available_capacity() != 0, "capacity exhausted");
        Self::mark_used(&mut self.last_used, &key, self.now.now());
        self.routes.insert(key, route);
    }

    fn move_to_end(last_used: &mut LastUsed<K>, idx: usize) {
        for i in idx..last_used.len()-1 {
            last_used.swap(i, i + 1);
        }
    }

    fn mark_used(last_used: &mut LastUsed<K>, key: &K, now: Instant) {
        // Search `last_used` in reverse-order, assuming that recently-used routes are
        // more likely to be reused.
        match last_used.iter().rposition(|&Used{key: ref k, ..}| k == key) {
            Some(idx) => {
                last_used[idx].at = now;
                Self::move_to_end(last_used, idx);
            }

            None => {
                last_used.push_back(key.clone().into());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::Future;
    use std::time::{Duration, Instant};
    use tower_service::Service;
    use::tests::MultiplyAndAssign;
    use super::*;

    #[test]
    fn mark_used() {
        let t = {
            let t0 = Instant::now();
            vec![
                t0,
                t0 + Duration::from_secs(10),
                t0 + Duration::from_secs(10),
                t0 + Duration::from_secs(30),
                t0 + Duration::from_secs(40),
                t0 + Duration::from_secs(50),
            ]
        };
        let mut last_used = VecDeque::new();
        last_used.push_back(Used{key: 0, at: t[0]});
        last_used.push_back(Used{key: 1, at: t[1]});
        last_used.push_back(Used{key: 2, at: t[2]});

        Cache::<_, MultiplyAndAssign>::mark_used(&mut last_used, &0, t[3]);
        assert_eq!(last_used[0], Used{key: 1, at: t[1]});
        assert_eq!(last_used[1], Used{key: 2, at: t[2]});
        assert_eq!(last_used[2], Used{key: 0, at: t[3]});

        Cache::<_, MultiplyAndAssign>::mark_used(&mut last_used, &2, t[4]);
        assert_eq!(last_used[0], Used{key: 1, at: t[1]});
        assert_eq!(last_used[1], Used{key: 0, at: t[3]});
        assert_eq!(last_used[2], Used{key: 2, at: t[4]});

        Cache::<_, MultiplyAndAssign>::mark_used(&mut last_used, &1, t[5]);
        assert_eq!(last_used[0], Used{key: 0, at: t[3]});
        assert_eq!(last_used[1], Used{key: 2, at: t[4]});
        assert_eq!(last_used[2], Used{key: 1, at: t[5]});
    }

    #[test]
    fn evict_oldest_idle_route_preserves_active_route() {
        let mut cache = Cache::new(1, Duration::from_secs(0));

        let mut service = MultiplyAndAssign::default();
        let mut rsp = service.call(1.into()).wait().unwrap();

        cache.add_route(1, service);
        assert_eq!(cache.routes.len(), 1);
        assert_eq!(cache.last_used.len(), 1);

        cache.evict_oldest_idle_route();
        assert_eq!(cache.routes.len(), 1);
        assert_eq!(cache.last_used.len(), 1);

        rsp.active.take();
        cache.evict_oldest_idle_route();
        assert_eq!(cache.routes.len(), 0);
        assert_eq!(cache.last_used.len(), 0);
    }

    #[test]
    fn evict_idle_after_min_time() {
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
        let mut cache = Cache::new(1, Duration::from_secs(10)).with_now(clock.clone());

        let mut service = MultiplyAndAssign::default();
        let rsp = service.call(1.into()).wait().unwrap();
        cache.add_route(1, service);
        assert_eq!(cache.routes.len(), 1);
        assert_eq!(cache.last_used.len(), 1);

        drop(rsp);
        cache.evict_oldest_idle_route();
        assert_eq!(cache.routes.len(), 1);
        assert_eq!(cache.last_used.len(), 1);

        clock.advance(Duration::from_secs(11));
        cache.evict_oldest_idle_route();
        assert_eq!(cache.routes.len(), 0);
        assert_eq!(cache.last_used.len(), 0);
    }

}
