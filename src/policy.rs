use std::num::NonZeroU32;

/// Refill policy for one rate limiter bucket.
///
/// `rate_per_second` is the number of units replenished per second. `burst` is the maximum
/// number of units that can be consumed in a burst.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    pub rate_per_second: NonZeroU32, // units replenished per second
    pub burst: NonZeroU32,           // max temp capacity
}

/// Creates a policy that replenishes `rate_per_second` units per second with the given
/// burst capacity.
impl Policy {
    pub(crate) fn to_governor_quota(self) -> governor::Quota {
        governor::Quota::per_second(self.rate_per_second).allow_burst(self.burst)
    }
}
