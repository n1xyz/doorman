//! Reusable rate limiting primitives.

pub mod error;
pub mod http;
pub mod key;
pub mod limiter;
pub mod policy;
pub mod units;

pub use error::RateLimitError;
pub use key::IpKey;
pub use limiter::RateLimiter;
pub use policy::Policy;
pub use units::{DurationBudgetLimiter, DurationUnits, RequestRateLimiter, Requests};
