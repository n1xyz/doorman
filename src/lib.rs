//! Reusable keyed rate limiting primitives.
//!
//! Doorman models each bucket as one [`RateLimiter`] with one [`Policy`] and
//! one unit meaning. Applications compose multiple limiters when different
//! traffic classes need different budgets.
//!
//! For example, a service can use a [`RequestRateLimiter`] for fixed-cost
//! request limits and a [`DurationBudgetLimiter`] for post-work duration
//! accounting. Low-level limiters can be keyed by [`IpKey`] or by an
//! application-specific key type; the built-in HTTP strategies handle client-IP
//! keys internally.
//!
//! The optional [`http`] module provides HTTP client IP extraction, built-in
//! client-IP request and elapsed-time strategies, and a Tower layer that can run
//! compatible lifecycle strategies. Strategies can decide whether a request is
//! allowed before the inner service runs, account after the service future
//! completes, and provide an optional timeout. Route classification and bucket
//! composition remain application responsibilities.
//!
//! Built-in HTTP strategies provide `with_policy` for simple construction and
//! `with_limiter` for applications that need to own the limiter handle, for
//! example to call [`RateLimiter::retain_recent`] from a maintenance task or to
//! share one bucket across multiple layers.
//!
//! # Bucket composition
//!
//! Use separate limiter objects for separate buckets. For example, an
//! application can put a lax limiter on `/action` and a stricter limiter on
//! all other routes:
//!
//! ```rust
//! use doorman::http::{ClientIpExtractor, RateLimitLayer, RequestCountByIp};
//! use doorman::Policy;
//! use ipnet::IpNet;
//! use std::num::NonZeroU32;
//!
//! let action_policy = Policy {
//!     rate_per_second: NonZeroU32::new(200).unwrap(),
//!     burst: NonZeroU32::new(400).unwrap(),
//! };
//! let general_policy = Policy {
//!     rate_per_second: NonZeroU32::new(20).unwrap(),
//!     burst: NonZeroU32::new(50).unwrap(),
//! };
//!
//! let action_extractor =
//!     ClientIpExtractor::with_trusted_proxies(["127.0.0.0/8".parse::<IpNet>().unwrap()]);
//! let general_extractor =
//!     ClientIpExtractor::with_trusted_proxies(["127.0.0.0/8".parse::<IpNet>().unwrap()]);
//!
//! let action_strategy = RequestCountByIp::with_policy(action_policy, action_extractor)
//!     .with_whitelist(["10.0.0.0/8".parse::<IpNet>().unwrap()]);
//! let general_strategy = RequestCountByIp::with_policy(general_policy, general_extractor);
//!
//! let action_layer = RateLimitLayer::with_strategy(action_strategy);
//! let general_layer = RateLimitLayer::with_strategy(general_strategy);
//!
//! // Apply `action_layer` only to /action.
//! // Apply `general_layer` to the rest of the router.
//! # let _ = (action_layer, general_layer);
//! ```
//!
//! # Request limits plus resource budgets
//!
//! Fixed request quota and post-work resource accounting are separate buckets.
//! The HTTP layer can enforce the request bucket before the handler runs, while
//! service code consumes duration units after expensive work is measured:
//!
//! ```rust
//! use doorman::{DurationBudgetLimiter, IpKey, Policy};
//! use std::net::IpAddr;
//! use std::num::NonZeroU32;
//!
//! let db_policy = Policy {
//!     rate_per_second: NonZeroU32::new(2_000).unwrap(),
//!     burst: NonZeroU32::new(2_000).unwrap(),
//! };
//! let db_budget = DurationBudgetLimiter::<IpKey>::new(db_policy);
//! let key = IpKey::from("203.0.113.10".parse::<IpAddr>().unwrap());
//!
//! let timer = db_budget.start_timer(&key);
//! // Run the database work here.
//! timer.consume_elapsed()?;
//! # Ok::<(), doorman::RateLimitError>(())
//! ```
//!
//! # HTTP elapsed-time budgets
//!
//! The HTTP layer can also account elapsed inner-service future time with
//! [`http::DurationBudgetByIp`]. This measures until the inner service future
//! resolves; it does not include response body streaming after that point.
//!
//! ```rust
//! use doorman::http::{ClientIpExtractor, DurationBudgetByIp, RateLimitLayer};
//! use doorman::Policy;
//! use ipnet::IpNet;
//! use std::num::NonZeroU32;
//! use std::time::Duration;
//!
//! let policy = Policy {
//!     rate_per_second: NonZeroU32::new(2_000).unwrap(),
//!     burst: NonZeroU32::new(2_000).unwrap(),
//! };
//! let extractor =
//!     ClientIpExtractor::with_trusted_proxies(["127.0.0.0/8".parse::<IpNet>().unwrap()]);
//!
//! let strategy =
//!     DurationBudgetByIp::with_policy(policy, extractor).with_timeout(Duration::from_secs(2));
//! let layer = RateLimitLayer::with_strategy(strategy);
//! # let _ = layer;
//! ```

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
