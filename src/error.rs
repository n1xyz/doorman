use std::time::Duration;

/// Error returned when a rate limit is exceeded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RateLimitError {
    Limited { retry_after: Duration }, // retry later after Duration
    InsufficientCapacity,              // requested units exceeds max burst
}

impl RateLimitError {
    pub fn limited(retry_after: Duration) -> Self {
        Self::Limited { retry_after }
    }

    pub fn retry_after(self) -> Option<Duration> {
        match self {
            Self::Limited { retry_after } => Some(retry_after),
            Self::InsufficientCapacity => None,
        }
    }
}
