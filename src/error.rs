use std::time::Duration;

/// Error returned when a limiter cannot accept a request or cost.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RateLimitError {
    /// The key is temporarily over budget.
    Limited { retry_after: Duration },

    /// The requested units exceed this limiter's burst capacity.
    InsufficientCapacity,
}

impl RateLimitError {
    /// Creates a temporary limit error with the given retry delay.
    pub fn limited(retry_after: Duration) -> Self {
        Self::Limited { retry_after }
    }

    /// Returns the retry delay when waiting can make the request fit.
    pub fn retry_after(self) -> Option<Duration> {
        match self {
            Self::Limited { retry_after } => Some(retry_after),
            Self::InsufficientCapacity => None,
        }
    }
}
