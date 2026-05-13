use std::time::Duration;

/// Error returned when a rate limit is exceeded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RateLimitError {
    retry_after: Duration,
}

impl RateLimitError {
    pub fn new(retry_after: Duration) -> Self {
        Self { retry_after }
    }

    pub fn retry_after(self) -> Duration {
        self.retry_after
    }
}
