use crate::units::Requests;
use crate::{Policy, RateLimitError};
use governor::clock::{Clock, DefaultClock};
use governor::middleware::NoOpMiddleware;
use std::hash::Hash;
use std::marker::PhantomData;

type StateStore<K> = governor::state::keyed::DashMapStateStore<K, ahash::RandomState>;

/// A keyed rate limiter for one policy and one unit type.
pub struct RateLimiter<K, U = Requests, C: Clock = DefaultClock>
where
    K: Eq + Hash + Clone,
{
    inner: governor::RateLimiter<K, StateStore<K>, C, NoOpMiddleware<C::Instant>>,
    _unit: PhantomData<U>,
}

impl<K, U> RateLimiter<K, U>
where
    K: Eq + Hash + Clone,
{
    pub fn new(policy: Policy) -> Self {
        Self::with_clock(policy, DefaultClock::default())
    }
}

impl<K, U, C> RateLimiter<K, U, C>
where
    K: Eq + Hash + Clone,
    C: Clock,
{
    pub fn with_clock(policy: Policy, clock: C) -> Self {
        Self {
            inner: governor::RateLimiter::new(policy.to_governor_quota(), <_>::default(), clock),
            _unit: PhantomData,
        }
    }
    pub fn check(&self, key: &K) -> Result<(), RateLimitError> {
        self.inner
            .check_key(key)
            .map_err(|err| RateLimitError::new(err.wait_time_from(self.inner.clock().now())))
    }

    pub fn retain_recent(&self) {}
}

impl<K, C> RateLimiter<K, Requests, C>
where
    K: Eq + Hash + Clone,
    C: Clock,
{
    pub fn check_request(&self, key: &K) -> Result<(), RateLimitError> {
        self.check(key)
    }
}
