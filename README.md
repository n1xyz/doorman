# Doorman

Doorman provides reusable keyed rate limiting primitives for Rust services.

The core model is:

```text
one RateLimiter object = one bucket / one policy
key -> limiter state
```

Applications compose multiple limiters for different traffic classes. For
example, a service might use one limiter for cheap action requests, another for
read requests, and another for database time budgets.

## Core Model

Each limiter has one unit meaning. Low-level limiters are generic over the key
type, so applications can key them by IP, API key, account ID, or another stable
identifier.

```text
RequestRateLimiter<IpKey>
  1 unit = 1 request

DurationBudgetLimiter<IpKey>
  1 unit = 1 millisecond
```

Do not mix unrelated units in the same limiter. Use separate limiter objects for
separate budgets.

## Request Limits

Use `RequestRateLimiter` when each event costs one request unit.
Calling `consume_request` is not a read-only check: it spends capacity for the
given key when the request is allowed.

```rust
use doorman::{IpKey, Policy, RequestRateLimiter};
use std::net::IpAddr;
use std::num::NonZeroU32;

let policy = Policy {
    rate_per_second: NonZeroU32::new(200).unwrap(),
    burst: NonZeroU32::new(400).unwrap(),
};
let limiter = RequestRateLimiter::<IpKey>::new(policy);

let key = IpKey::from("1.2.3.4".parse::<IpAddr>().unwrap());
limiter.consume_request(&key)?;
# Ok::<(), doorman::RateLimitError>(())
```

## Duration Budgets

Use `DurationBudgetLimiter` when the cost is measured as elapsed time, such as
database time.

```rust
use doorman::{DurationBudgetLimiter, IpKey, Policy};
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::time::Duration;

let policy = Policy {
    rate_per_second: NonZeroU32::new(2_000).unwrap(),
    burst: NonZeroU32::new(2_000).unwrap(),
};
let db_budget = DurationBudgetLimiter::<IpKey>::new(policy);

let key = IpKey::from("1.2.3.4".parse::<IpAddr>().unwrap());
db_budget.consume_duration(&key, Duration::from_millis(40))?;
# Ok::<(), doorman::RateLimitError>(())
```

### Timing Scoped Work

For work where the cost is elapsed wall-clock time, create a timer before the
work and explicitly consume it after the work finishes.

```rust
# use doorman::{DurationBudgetLimiter, IpKey, Policy};
# use std::net::IpAddr;
# use std::num::NonZeroU32;
# let policy = Policy {
#     rate_per_second: NonZeroU32::new(2_000).unwrap(),
#     burst: NonZeroU32::new(2_000).unwrap(),
# };
# let db_budget = DurationBudgetLimiter::<IpKey>::new(policy);
# let key = IpKey::from("1.2.3.4".parse::<IpAddr>().unwrap());
let timer = db_budget.start_timer(&key);

// run_db_query().await;

timer.consume_elapsed()?;
# Ok::<(), doorman::RateLimitError>(())
```

Timers do not charge on drop. Call `consume_elapsed` explicitly. The timer
borrows the limiter and key, so both must live until the elapsed duration is
charged.

Duration charging rules:

```text
Duration::ZERO      -> no-op
0 < duration < 1ms  -> consumes 1 unit
N ms duration       -> consumes N units
too large for u32   -> InsufficientCapacity
```

## IP Keys

`IpKey` is a convenience key type for low-level IP-based limiters. The built-in
HTTP strategies construct and use this key internally, so normal HTTP middleware
callers usually only provide a `Policy` and a `ClientIpExtractor`.

```text
IPv4 -> exact u32 address bits embedded in an internal sentinel range
IPv6 -> /64 prefix represented as u64
```

The IPv6 grouping intentionally limits by prefix rather than individual address.
The IPv4 sentinel uses `2001:db8::/32`, which is reserved for documentation and
should not appear as real client traffic. This keeps `IpKey` compact at one
`u64`, but it is not collision-free for arbitrary synthetic IPv6 addresses in
that documentation range.

## HTTP Client IP Extraction

`ClientIpExtractor` turns peer connection info and HTTP headers into the real
client IP.

If the peer IP is trusted, headers are checked in this order:

```text
X-Forwarded-For
X-Real-IP
Forwarded
```

If the peer IP is not trusted, forwarding headers are ignored and the peer IP is
used directly.

## HTTP Layer

`RateLimitLayer` is a Tower layer that applies a rate-limit strategy.
`RequestCountByIp` is the built-in strategy for fixed-cost request limits keyed
by client IP: it extracts the client IP, applies whitelist bypasses, consumes
one request unit, and stores the resolved client identity for downstream code.
`DurationBudgetByIp` is the built-in strategy for elapsed inner-service time
accounting keyed by client IP, with an optional per-request timeout.
Applications that need a different key or policy can provide their own type that
implements `RateLimitStrategy`.

Strategies have three lifecycle hooks: `before_request`, `after_response`, and
an optional `timeout`. If a timeout fires before the inner service future
resolves, the layer returns `429 Too Many Requests` and still runs
`after_response` so elapsed work can be accounted. Timeout drops the inner
future, but only async work that respects cancellation is actually stopped.

```rust
use doorman::http::{ClientIpExtractor, RateLimitLayer, RequestCountByIp};
use doorman::Policy;
use ipnet::IpNet;
use std::num::NonZeroU32;

let policy = Policy {
    rate_per_second: NonZeroU32::new(200).unwrap(),
    burst: NonZeroU32::new(400).unwrap(),
};
let extractor = ClientIpExtractor::with_trusted_proxies(["127.0.0.0/8".parse::<IpNet>().unwrap()]);

let strategy = RequestCountByIp::with_policy(policy, extractor);
let layer = RateLimitLayer::with_strategy(strategy);
```

Elapsed-time budgets use the same layer with a different strategy. The elapsed
duration is measured until the inner service future resolves; response body
streaming after that point is not included.

```rust
use doorman::http::{ClientIpExtractor, DurationBudgetByIp, RateLimitLayer};
use doorman::Policy;
use ipnet::IpNet;
use std::num::NonZeroU32;
use std::time::Duration;

let policy = Policy {
    rate_per_second: NonZeroU32::new(2_000).unwrap(),
    burst: NonZeroU32::new(2_000).unwrap(),
};
let extractor = ClientIpExtractor::with_trusted_proxies(["127.0.0.0/8".parse::<IpNet>().unwrap()]);

let strategy =
    DurationBudgetByIp::with_policy(policy, extractor).with_timeout(Duration::from_secs(2));
let layer = RateLimitLayer::with_strategy(strategy);
```

Whitelist bypasses are scoped to the specific layer. Use this for traffic classes
where bypassing is intentional, such as a high-throughput action endpoint.

```rust
# use doorman::http::{ClientIpExtractor, RateLimitLayer, RequestCountByIp};
# use doorman::Policy;
# use ipnet::IpNet;
# use std::num::NonZeroU32;
# let policy = Policy {
#     rate_per_second: NonZeroU32::new(200).unwrap(),
#     burst: NonZeroU32::new(400).unwrap(),
# };
# let extractor = ClientIpExtractor::with_trusted_proxies(["127.0.0.0/8".parse::<IpNet>().unwrap()]);
let strategy = RequestCountByIp::with_policy(policy, extractor)
    .with_whitelist(["10.0.0.0/8".parse::<IpNet>().unwrap()]);
let layer = RateLimitLayer::with_strategy(strategy);
```

The layer expects a `std::net::SocketAddr` to be present in request extensions.
If the key is over quota, it returns `429 Too Many Requests`. If available,
`Retry-After` is included.

When the `axum` feature is enabled, the layer can also read peer addresses from
`axum::extract::ConnectInfo<std::net::SocketAddr>`.

```bash
cargo test --features axum
```

## Errors

`RateLimitError` has two cases:

```text
Limited { retry_after }
  The key is temporarily over budget. Waiting can help.

InsufficientCapacity
  The requested cost exceeds the limiter burst capacity. Waiting will not help
  unless the policy or requested cost changes.
```

## Current Limitations

Doorman currently does not provide:

```text
route classification
tiering
load shedding
trust tables
post-handler middleware accounting
```

The current HTTP layer uses default response bodies for rejected requests.
