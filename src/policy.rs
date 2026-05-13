use std::num::NonZeroU32;

/// Rate limiter policy for one bucket.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    rate: NonZeroU32,  // units replenished per second
    burst: NonZeroU32, // max temp capacity
}

impl Policy {
    pub fn per_second(rate: NonZeroU32, burst: NonZeroU32) -> Self {
        Self { rate, burst }
    }

    pub fn rate(self) -> NonZeroU32 {
        self.rate
    }

    pub fn burst(self) -> NonZeroU32 {
        self.burst
    }

    pub(crate) fn to_governor_quota(self) -> governor::Quota {
        governor::Quota::per_second(self.rate).allow_burst(self.burst)
    }
}
