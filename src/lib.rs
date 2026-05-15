//! Reusable keyed rate limiting primitives.
//!
//! Doorman models each bucket as one [`RateLimiter`] with one [`Policy`] and
//! one unit meaning. Applications compose multiple limiters when different
//! traffic classes need different budgets.
//!
//! For example, a service can use a [`RequestRateLimiter`] for fixed-cost
//! request limits and a [`DurationBudgetLimiter`] for post-work duration
//! accounting, both keyed by [`IpKey`] or by an application-specific key type.
//!
//! The optional [`http`] module provides HTTP client IP extraction and a Tower
//! layer for fixed-cost request limiting. Route classification and bucket
//! composition remain application responsibilities.

pub mod error;
pub mod http;
pub mod key;
pub mod limiter;
pub mod policy;
pub mod units;

pub use error::RateLimitError;
pub use key::IpKey;
pub use limiter::{DurationTimer, RateLimiter};
pub use policy::Policy;
pub use units::{DurationBudgetLimiter, DurationUnits, RequestRateLimiter, Requests};
