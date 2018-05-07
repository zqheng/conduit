use std::{marker::{PhantomData, Sized}, time::Duration};

use access;

pub const ALWAYS: Const = Const(true);
pub const NEVER: Const = Const(false);

pub trait Retain<T> {
    fn retain(&self, t: &access::Node<T>) -> bool;

    fn and<B: Retain<T>>(self, b: B) -> And<T, Self, B>
    where
        Self: Sized,
        B: Sized
    {
        And(self, b, PhantomData)
    }

    fn or<B: Retain<T>>(self, b: B) -> Or<T, Self, B>
    where
        Self: Sized,
        B: Sized
    {
        Or(self, b, PhantomData)
    }
}

pub struct Const(bool);

pub struct And<T, A: Retain<T>, B: Retain<T>>(A, B, PhantomData<T>);

pub struct Or<T, A: Retain<T>, B: Retain<T>>(A, B, PhantomData<T>);

pub struct MaxAccessAge<T, N = ()> {
    age: Duration,
    now: N,
    _p: PhantomData<T>,
}

// ===== impl Const =====

impl<T> Retain<T> for Const {
    fn retain(&self, _: &access::Node<T>) -> bool {
        self.0
    }
}

// ===== impl And =====

impl<T, A: Retain<T>, B: Retain<T>> Retain<T> for And<T, A, B> {
    fn retain(&self, t: &access::Node<T>) -> bool {
        self.0.retain(t) && self.1.retain(t)
    }
}

// ===== impl Or =====

impl<T, A: Retain<T>, B: Retain<T>> Retain<T> for Or<T, A, B> {
    fn retain(&self, t: &access::Node<T>) -> bool {
        self.0.retain(t) || self.1.retain(t)
    }
}

// ===== impl MaxAccessAge =====

impl<T> MaxAccessAge<T> {
    pub fn new(age: Duration) -> Self {
        MaxAccessAge { age, now: (), _p: PhantomData }
    }
}

impl<T, N: access::Now> Retain<T> for MaxAccessAge<T, N> {
    fn retain(&self, t: &access::Node<T>) -> bool {
        t.last_access() >= self.now.now() - self.age
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use access::{self, Now};
    use test_util::*;
    use super::*;

    #[test]
    fn always() {
        let clock = Clock::default();
        let node = access::Node::new(666, clock.now());
        assert!(ALWAYS.retain(&node));
    }

    #[test]
    fn never() {
        let clock = Clock::default();
        let node = access::Node::new(666, clock.now());
        assert!(!NEVER.retain(&node));
    }

    #[test]
    fn and() {
        let clock = Clock::default();
        let node = access::Node::new(666, clock.now());
        assert!(ALWAYS.and(ALWAYS).retain(&node));
        assert!(!NEVER.and(ALWAYS).retain(&node));
        assert!(!ALWAYS.and(NEVER).retain(&node));
        assert!(!NEVER.and(NEVER).retain(&node));
    }

    #[test]
    fn or() {
        let clock = Clock::default();
        let node = access::Node::new(666, clock.now());
        assert!(ALWAYS.or(ALWAYS).retain(&node));
        assert!(NEVER.or(ALWAYS).retain(&node));
        assert!(ALWAYS.or(NEVER).retain(&node));
        assert!(!NEVER.or(NEVER).retain(&node));
    }

    #[test]
    fn max_access_age() {
        let mut clock = Clock::default();
        let mag = MaxAccessAge::new(Duration::from_secs(2), clock.clone());

        let t0 = clock.now();
        let mut node = access::Node::new(666, t0);

        assert!(mag.retain(&node));

        // Update the access time from the original.
        clock.advance(Duration::from_secs(1));
        {
            let access = node.access(&clock);
            assert_eq!(access.last_access(), t0);
        }
        assert!(mag.retain(&node));

        // Advance until retain fails.
        clock.advance(Duration::from_secs(1));
        assert!(mag.retain(&node));

        clock.advance(Duration::from_secs(1));
        assert!(mag.retain(&node));

        clock.advance(Duration::from_secs(1));
        assert!(!mag.retain(&node));
    }
}
