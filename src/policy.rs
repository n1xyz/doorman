use std::num::NonZeroU32;

/// Refill policy for one rate limiter bucket.
///
/// `rate` is the number of units replenished per second. `burst` is the maximum
/// number of units that can be consumed in a burst.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    rate: NonZeroU32,  // units replenished per second
    burst: NonZeroU32, // max temp capacity
}

impl Policy {
    /// Creates a policy that replenishes `rate` units per second with the given
    /// burst capacity.
    pub fn per_second(rate: NonZeroU32, burst: NonZeroU32) -> Self {
        Self { rate, burst }
    }

    /// Returns the number of units replenished per second.
    pub fn rate(self) -> NonZeroU32 {
        self.rate
    }

    /// Returns the maximum burst capacity in units.
    pub fn burst(self) -> NonZeroU32 {
        self.burst
    }

    pub(crate) fn to_governor_quota(self) -> governor::Quota {
        governor::Quota::per_second(self.rate).allow_burst(self.burst)
    }
}
