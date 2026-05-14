use crate::RateLimiter;

/// Unit marker for request-count rate limiting.
pub enum Requests {}

/// A rate limiter where one unit represents one request.
pub type RequestRateLimiter<K> = RateLimiter<K, Requests>;

/// Unit market for duration-count rate limiting.
pub enum DurationUnits {}

/// A rate limiter where one unit represents one millisecond
pub type DurationBudgetLimiter<K> = RateLimiter<K, DurationUnits>;
