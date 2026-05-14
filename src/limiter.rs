use crate::units::{DurationUnits, Requests};
use crate::{Policy, RateLimitError};
use governor::clock::{Clock, DefaultClock};
use governor::middleware::NoOpMiddleware;
use std::hash::Hash;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::time::Duration;
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
    pub fn check_and_consume_one(&self, key: &K) -> Result<(), RateLimitError> {
        self.check_and_consume_n(key, NonZeroU32::new(1).unwrap())
    }

    pub fn check_and_consume_n(&self, key: &K, units: NonZeroU32) -> Result<(), RateLimitError> {
        match self.inner.check_key_n(key, units) {
            Ok(Ok(())) => Ok(()),

            Ok(Err(not_until)) => Err(RateLimitError::Limited {
                retry_after: not_until.wait_time_from(self.inner.clock().now()),
            }),
            Err(_) => Err(RateLimitError::InsufficientCapacity),
        }
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
        self.check_and_consume_one(key)
    }
}

impl<K, C> RateLimiter<K, DurationUnits, C>
where
    K: Eq + Hash + Clone,
    C: Clock,
{
    pub fn consume_duration(&self, key: &K, duration: Duration) -> Result<(), RateLimitError> {
        if duration.is_zero() {
            return Ok(());
        }

        let millis = duration.as_millis();

        let units = if millis == 0 {
            NonZeroU32::new(1).unwrap()
        } else {
            let millis = u32::try_from(millis).map_err(|_| RateLimitError::InsufficientCapacity)?;

            NonZeroU32::new(millis).unwrap()
        };
        self.check_and_consume_n(key, units)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use governor::clock::FakeRelativeClock;
    use std::num::NonZeroU32;
    use std::time::Duration;

    fn make_request_limiter(
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

    fn make_duration_limiter(
        burst: u32,
    ) -> (
        RateLimiter<u64, DurationUnits, FakeRelativeClock>,
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
        let (limiter, _) = make_request_limiter(1);

        assert!(limiter.check_request(&1).is_ok());
    }

    #[test]
    fn denies_requests_over_quota() {
        let (limiter, _) = make_request_limiter(1);

        assert!(limiter.check_request(&1).is_ok());
        assert!(limiter.check_request(&1).is_err());
    }

    #[test]
    fn independent_keys_have_independent_quota() {
        let (limiter, _) = make_request_limiter(1);

        assert!(limiter.check_request(&1).is_ok());
        assert!(limiter.check_request(&1).is_err());

        assert!(limiter.check_request(&2).is_ok());
    }

    #[test]
    fn quota_replenishes_after_time() {
        let (limiter, clock) = make_request_limiter(1);

        assert!(limiter.check_request(&1).is_ok());
        assert!(limiter.check_request(&1).is_err());

        clock.advance(Duration::from_secs(1));

        assert!(limiter.check_request(&1).is_ok());
    }

    #[test]
    fn retain_recent_doesnt_panic() {
        let (limiter, clock) = make_request_limiter(1);

        assert!(limiter.check_request(&1).is_ok());

        clock.advance(Duration::from_secs(60));
        limiter.retain_recent();

        assert!(limiter.check_request(&1).is_ok());
    }

    #[test]
    fn allows_n_requests_under_quota() {
        let (limiter, _) = make_request_limiter(20);

        assert!(
            limiter
                .check_and_consume_n(&1, NonZeroU32::new(20).unwrap())
                .is_ok()
        );
    }

    #[test]
    fn denies_n_requests_over_quota() {
        let (limiter, _) = make_request_limiter(20);

        assert!(
            limiter
                .check_and_consume_n(&1, NonZeroU32::new(40).unwrap())
                .is_err()
        );
    }

    #[test]
    fn temp_limited_over_quota() {
        let (limiter, _) = make_request_limiter(3);

        assert!(
            limiter
                .check_and_consume_n(&1, NonZeroU32::new(3).unwrap())
                .is_ok()
        );
        assert!(limiter.check_and_consume_one(&1).is_err());
    }

    #[test]
    fn zero_duration_is_noop() {
        let (limiter, _) = make_duration_limiter(1);

        assert!(limiter.consume_duration(&1, Duration::ZERO).is_ok());
        assert!(
            limiter
                .consume_duration(&1, Duration::from_millis(1))
                .is_ok()
        );
    }

    #[test]
    fn sub_millisecond_duration_rounds_up_to_one_unit() {
        let (limiter, _) = make_duration_limiter(1);

        assert!(
            limiter
                .consume_duration(&1, Duration::from_nanos(1))
                .is_ok()
        );
        assert!(
            limiter
                .consume_duration(&1, Duration::from_millis(1))
                .is_err()
        );
    }

    #[test]
    fn duration_consumes_millisecond_units() {
        let (limiter, _) = make_duration_limiter(3);

        assert!(
            limiter
                .consume_duration(&1, Duration::from_millis(3))
                .is_ok()
        );
        assert!(
            limiter
                .consume_duration(&1, Duration::from_millis(1))
                .is_err()
        );
    }
}
