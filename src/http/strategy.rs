use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use http::Request;
use ipnet::{IpNet, Ipv4Net, Ipv6Net};

use crate::http::extract::{ClientIpExtractor, peer_addr, split_nets};
use crate::http::layer::{RateLimitRejection, RateLimitStrategy};
use crate::{DurationBudgetLimiter, IpKey, RequestRateLimiter};

/// Elapsed-time budget accounting by client IP.
///
/// This built-in [`RateLimitStrategy`] extracts the real client IP before the
/// inner service runs, then charges the elapsed inner-service future duration
/// after it completes. The measured duration does not include full response body
/// streaming after the response future resolves.
#[derive(Clone)]
pub struct DurationBudgetByIp {
    limiter: Arc<DurationBudgetLimiter<IpKey>>,
    extractor: ClientIpExtractor,
    timeout: Option<Duration>,
}

impl DurationBudgetByIp {
    /// Creates an elapsed-time budget strategy keyed by client IP.
    pub fn new(
        limiter: Arc<DurationBudgetLimiter<IpKey>>,
        extractor: ClientIpExtractor,
        timeout: Option<Duration>,
    ) -> Self {
        Self {
            limiter,
            extractor,
            timeout,
        }
    }

    fn check_before_request<B>(&self, req: &mut Request<B>) -> Result<IpKey, RateLimitRejection> {
        let (_, key) = extract_ip_key(&self.extractor, req)?;

        req.extensions_mut().insert(key);
        Ok(key)
    }
}

impl<B> RateLimitStrategy<B> for DurationBudgetByIp {
    type State = IpKey;

    fn before_request(&self, req: &mut Request<B>) -> Result<Self::State, RateLimitRejection> {
        self.check_before_request(req)
    }

    fn after_response(
        &self,
        state: Self::State,
        elapsed: Duration,
    ) -> Result<(), RateLimitRejection> {
        self.limiter
            .consume_duration(&state, elapsed)
            .map_err(RateLimitRejection::Limited)
    }

    fn timeout(&self, _state: &Self::State) -> Option<Duration> {
        self.timeout
    }
}

/// Fixed-cost request limiting by client IP.
///
/// This is the built-in [`RateLimitStrategy`] for the common HTTP case: extract
/// the real client IP, optionally bypass whitelisted networks, consume one
/// request unit, and store the resulting [`IpKey`] in request extensions.
#[derive(Clone)]
pub struct RequestCountByIp {
    limiter: Arc<RequestRateLimiter<IpKey>>,
    extractor: ClientIpExtractor,
    whitelist_v4: Box<[Ipv4Net]>,
    whitelist_v6: Box<[Ipv6Net]>,
}

impl RequestCountByIp {
    /// Creates a request-counting strategy keyed by client IP.
    pub fn new(limiter: Arc<RequestRateLimiter<IpKey>>, extractor: ClientIpExtractor) -> Self {
        Self {
            limiter,
            extractor,
            whitelist_v4: Box::new([]),
            whitelist_v6: Box::new([]),
        }
    }

    /// Adds IP networks that bypass this strategy's request limiter.
    ///
    /// Whitelisting is scoped to this layer only.
    pub fn with_whitelist(mut self, whitelist: impl IntoIterator<Item = IpNet>) -> Self {
        let (whitelist_v4, whitelist_v6) = split_nets(whitelist);
        self.whitelist_v4 = whitelist_v4;
        self.whitelist_v6 = whitelist_v6;
        self
    }

    fn is_whitelisted(&self, ip: IpAddr) -> bool {
        match ip {
            IpAddr::V4(v4) => self.whitelist_v4.iter().any(|net| net.contains(&v4)),
            IpAddr::V6(v6) => self.whitelist_v6.iter().any(|net| net.contains(&v6)),
        }
    }

    fn check_before_request<B>(&self, req: &mut Request<B>) -> Result<(), RateLimitRejection> {
        let (ip, key) = extract_ip_key(&self.extractor, req)?;

        if self.is_whitelisted(ip) {
            req.extensions_mut().insert(key);
            return Ok(());
        }

        self.limiter
            .check_request(&key)
            .map_err(RateLimitRejection::Limited)?;

        req.extensions_mut().insert(key);
        Ok(())
    }
}

impl<B> RateLimitStrategy<B> for RequestCountByIp {
    type State = ();

    fn before_request(&self, req: &mut Request<B>) -> Result<Self::State, RateLimitRejection> {
        self.check_before_request(req)
    }
}

fn extract_ip_key<B>(
    extractor: &ClientIpExtractor,
    req: &mut Request<B>,
) -> Result<(IpAddr, IpKey), RateLimitRejection> {
    let Some(peer_addr) = peer_addr(req) else {
        return Err(RateLimitRejection::MissingPeer);
    };

    let ip = extractor
        .extract(peer_addr, req.headers())
        .map_err(|_| RateLimitRejection::ExtractFailed)?;

    let key = IpKey::from(ip);

    Ok((ip, key))
}
