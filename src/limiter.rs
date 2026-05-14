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

    pub fn retain_recent(&self) {
        self.inner.retain_recent();
    }
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

#[cfg(test)]
mod tests {
    use crate::limiter;

    use super::*;
    use governor::clock::FakeRelativeClock;
    use std::num::NonZeroU32;
    use std::time::Duration;

    fn make_limiter(
        burst: u32,
    ) -> (
        RateLimiter<u64, Requests, FakeRelativeClock>,
        FakeRelativeClock,
    ) {
        let clock = FakeRelativeClock::default();
        let policy =
            Policy::per_second(NonZeroU32::new(1).unwrap(), NonZeroU32::new(burst).unwrap());
        let limiter = RateLimiter::with_clock(policy, clock.clone());
        (limiter, clock)
    }

    #[test]
    fn allows_requests_under_quota() {
        let (limiter, _) = make_limiter(1);

        assert!(limiter.check_request(&1).is_ok());
    }

    #[test]
    fn denies_requests_over_quota() {
        let (limiter, _) = make_limiter(1);

        assert!(limiter.check_request(&1).is_ok());
        assert!(limiter.check_request(&1).is_err());
    }

    #[test]
    fn independent_keys_have_independent_quota() {
        let (limiter, _) = make_limiter(1);

        assert!(limiter.check_request(&1).is_ok());
        assert!(limiter.check_request(&1).is_err());

        assert!(limiter.check_request(&2).is_ok());
    }

    #[test]
    fn quota_replenishes_after_time() {
        let (limiter, clock) = make_limiter(1);

        assert!(limiter.check_request(&1).is_ok());
        assert!(limiter.check_request(&1).is_err());

        clock.advance(Duration::from_secs(1));

        assert!(limiter.check_request(&1).is_ok());
    }

    #[test]
    fn retain_recent_doesnt_panic() {
        let (limiter, clock) = make_limiter(1);

        assert!(limiter.check_request(&1).is_ok());

        clock.advance(Duration::from_secs(60));
        limiter.retain_recent();

        assert!(limiter.check_request(&1).is_ok());
    }
}
