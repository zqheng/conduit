
use indexmap::IndexMap;
pub use indexmap::Equivalent;
use std::{ops::{Deref, DerefMut}, time::Instant};

/// The order of the indexmap is not relevant (currently).
pub type AccessMap<K, V> = IndexMap<K, Node<V>>;

/// Provides the current time within the module. Useful for testing.
pub trait Now {
    fn now(&self) -> Instant;
}

/// A smart pointer that updates an access time when dropped.
///
/// Wraps a mutable reference to a `V`-typed value.
///
/// When the guard is dropped, the value's `last_access` time is updated with the provided
/// time source.
pub struct Access<'a, T: 'a, N: Now + 'a = ()> {
    node: &'a mut Node<T>,
    now: &'a N,
}

/// Holds the last-access time of a value.
#[derive(Debug, PartialEq)]
pub struct Node<T> {
    target: T,
    last_access: Instant,
}

// ===== impl Access =====

impl<'a, T: 'a, N: Now + 'a> Deref for Access<'a, T, N> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.node
    }
}

impl<'a, T: 'a, N: Now + 'a> DerefMut for Access<'a, T, N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.node
    }
}

impl<'a, T: 'a, N: Now + 'a> Access<'a, T, N> {
    pub fn last_access(&self) -> Instant {
        self.node.last_access
    }
}

impl<'a, T: 'a, N: Now + 'a> Drop for Access<'a, T, N> {
    fn drop(&mut self) {
        self.node.last_access = self.now.now();
    }
}

// ===== impl Node =====

impl<T> Node<T> {
    pub fn new(target: T, last_access: Instant) -> Self {
        Node { target, last_access }
    }

    pub fn access<'a, N: Now + 'a>(&'a mut self, now: &'a N) -> Access<'a, T, N> {
        Access { now, node: self, }
    }

    pub fn last_access(&self) -> Instant {
        self.last_access
    }
}

impl<T> Deref for Node<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.target
    }
}

impl<T> DerefMut for Node<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.target
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
    use std::time::Duration;

    use test_util::*;
    use super::*;

    #[test]
    fn last_access_updated_on_drop() {
        let mut clock = Clock::default();
        let t0 = clock.now();
        let mut node = Node::new(123, t0);

        clock.advance(Duration::from_secs(1));
        {
            let access = node.access(&clock);
            assert_eq!(access.last_access(), t0);
        }

        let t1 = clock.now();
        assert_eq!(node.last_access(), t1);
        assert_ne!(t0, t1);
    }
}
