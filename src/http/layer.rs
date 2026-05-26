use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::{Request, Response, StatusCode};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use tower::{Layer, Service};

use crate::http::extract::split_nets;
use crate::{IpKey, RateLimitError, RequestRateLimiter, http::ClientIpExtractor};

enum RateLimitRejection {
    Limited(RateLimitError),
    MissingPeer,
    ExtractFailed,
}

/// Fixed-cost request limiting by client IP.
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

    /// Adds IP networks that bypass this layer's request limiter.
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

    fn before_request<B>(&self, req: &mut Request<B>) -> Result<(), RateLimitRejection> {
        let Some(peer_addr) = peer_addr(&req) else {
            return Err(RateLimitRejection::MissingPeer);
        };

        let ip = self
            .extractor
            .extract(peer_addr, req.headers())
            .map_err(|_| RateLimitRejection::ExtractFailed)?;

        let key = IpKey::from(ip);

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

/// Tower layer that applies a rate-limit strategy before calling the inner service.
pub struct RateLimitLayer {
    strategy: RequestCountByIp,
}

pub struct RateLimitService<S> {
    inner: S,
    strategy: RequestCountByIp,
}

impl<S> Layer<S> for RateLimitLayer {
    type Service = RateLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimitService {
            inner,
            strategy: self.strategy.clone(),
        }
    }
}

impl RateLimitLayer {
    /// Creates a layer from a request-counting strategy.
    pub fn with_strategy(strategy: RequestCountByIp) -> Self {
        Self { strategy }
    }

    /// Creates a layer using fixed-cost request limiting by client IP.
    pub fn new(limiter: Arc<RequestRateLimiter<IpKey>>, extractor: ClientIpExtractor) -> Self {
        Self::with_strategy(RequestCountByIp::new(limiter, extractor))
    }

    /// Adds IP networks that bypass this layer's request-count strategy.
    pub fn with_whitelist(mut self, whitelist: impl IntoIterator<Item = IpNet>) -> Self {
        self.strategy = self.strategy.with_whitelist(whitelist);
        self
    }
}

impl<S, B> Service<Request<B>> for RateLimitService<S>
where
    S: Service<Request<B>, Response = Response<B>>,
    B: Default,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = RateLimitFuture<S::Future, B>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<B>) -> Self::Future {
        match self.strategy.before_request(&mut req) {
            Ok(()) => RateLimitFuture::Allowed(self.inner.call(req)),
            Err(RateLimitRejection::Limited(err)) => RateLimitFuture::Limited(err),
            Err(RateLimitRejection::MissingPeer) => RateLimitFuture::MissingPeer,
            Err(RateLimitRejection::ExtractFailed) => RateLimitFuture::ExtractFailed,
        }
    }
}

fn peer_addr<B>(req: &Request<B>) -> Option<SocketAddr> {
    if let Some(addr) = req.extensions().get::<SocketAddr>().copied() {
        return Some(addr);
    }

    #[cfg(feature = "axum")]
    {
        if let Some(axum::extract::ConnectInfo(addr)) = req
            .extensions()
            .get::<axum::extract::ConnectInfo<SocketAddr>>()
        {
            return Some(*addr);
        }
    }

    None
}

pub enum RateLimitFuture<F, B> {
    Allowed(F),
    Limited(RateLimitError),
    MissingPeer,
    ExtractFailed,
    _Body(std::marker::PhantomData<B>),
}

impl<F, B, E> Future for RateLimitFuture<F, B>
where
    F: Future<Output = Result<Response<B>, E>>,
    B: Default,
{
    type Output = Result<Response<B>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: We never move F out of self, and all other variants are trivially Unpin.
        match unsafe { self.get_unchecked_mut() } {
            RateLimitFuture::Allowed(fut) => unsafe { Pin::new_unchecked(fut) }.poll(cx),
            RateLimitFuture::Limited(err) => Poll::Ready(Ok(rate_limited_response(*err))),
            RateLimitFuture::MissingPeer | RateLimitFuture::ExtractFailed => {
                Poll::Ready(Ok(server_error_response()))
            }
            RateLimitFuture::_Body(_) => unreachable!(),
        }
    }
}

fn rate_limited_response<B: Default>(err: RateLimitError) -> Response<B> {
    use http::header::RETRY_AFTER;

    let mut response = Response::builder()
        .status(StatusCode::TOO_MANY_REQUESTS)
        .body(B::default())
        .expect("response builder with static status should not fail");

    if let Some(retry_after) = err.retry_after() {
        response
            .headers_mut()
            .insert(RETRY_AFTER, retry_after.as_secs().saturating_add(1).into());
    }

    response
}

fn server_error_response<B: Default>() -> Response<B> {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(B::default())
        .expect("response builder with static status should not fail")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Policy;
    use http::header::RETRY_AFTER;
    use ipnet::IpNet;
    use std::convert::Infallible;
    use std::num::NonZeroU32;
    use tower::ServiceExt;
    use tower::service_fn;

    fn layer() -> RateLimitLayer {
        let policy = Policy::per_second(NonZeroU32::new(1).unwrap(), NonZeroU32::new(1).unwrap());
        let limiter = Arc::new(RequestRateLimiter::new(policy));
        let extractor =
            ClientIpExtractor::with_trusted_proxies(["127.0.0.0/8".parse::<IpNet>().unwrap()]);
        RateLimitLayer::new(limiter, extractor)
    }

    fn whitelisted_layer() -> RateLimitLayer {
        layer().with_whitelist(["1.2.3.0/24".parse::<IpNet>().unwrap()])
    }

    fn request(peer: Option<&str>) -> Request<()> {
        let mut req = Request::new(());
        if let Some(peer) = peer {
            req.extensions_mut().insert(socket(peer));
        }
        req
    }

    fn socket(ip: &str) -> SocketAddr {
        SocketAddr::new(ip.parse().unwrap(), 12345)
    }

    fn ok_service() -> impl Service<Request<()>, Response = Response<()>, Error = Infallible> {
        service_fn(|_req: Request<()>| async {
            Ok::<_, Infallible>(Response::builder().status(StatusCode::OK).body(()).unwrap())
        })
    }

    fn require_ip_key_service(
        expected: IpKey,
    ) -> impl Service<Request<()>, Response = Response<()>, Error = Infallible> {
        service_fn(move |req: Request<()>| async move {
            let status = match req.extensions().get::<IpKey>() {
                Some(key) if *key == expected => StatusCode::OK,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };

            Ok::<_, Infallible>(Response::builder().status(status).body(()).unwrap())
        })
    }

    #[tokio::test]
    async fn allowed_request_calls_inner_service() {
        let mut service = layer().layer(ok_service());

        let response = service
            .ready()
            .await
            .unwrap()
            .call(request(Some("1.2.3.4")))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn allowed_request_inserts_ip_key_extension() {
        let expected = IpKey::from("1.2.3.4".parse::<std::net::IpAddr>().unwrap());
        let mut service = layer().layer(require_ip_key_service(expected));

        let response = service
            .ready()
            .await
            .unwrap()
            .call(request(Some("1.2.3.4")))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn second_request_from_same_ip_returns_429() {
        let mut service = layer().layer(ok_service());

        let first = service
            .ready()
            .await
            .unwrap()
            .call(request(Some("1.2.3.4")))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        let second = service
            .ready()
            .await
            .unwrap()
            .call(request(Some("1.2.3.4")))
            .await
            .unwrap();

        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            second
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
    }

    #[tokio::test]
    async fn whitelist_bypasses_only_the_configured_layer() {
        let mut action_service = whitelisted_layer().layer(ok_service());

        let first = action_service
            .ready()
            .await
            .unwrap()
            .call(request(Some("1.2.3.4")))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        let second = action_service
            .ready()
            .await
            .unwrap()
            .call(request(Some("1.2.3.4")))
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::OK);

        let mut general_service = layer().layer(ok_service());

        let first = general_service
            .ready()
            .await
            .unwrap()
            .call(request(Some("1.2.3.4")))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        let second = general_service
            .ready()
            .await
            .unwrap()
            .call(request(Some("1.2.3.4")))
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn missing_peer_extension_returns_500() {
        let mut service = layer().layer(ok_service());

        let response = service
            .ready()
            .await
            .unwrap()
            .call(request(None))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn trusted_proxy_header_is_used_as_key() {
        let mut service = layer().layer(ok_service());

        let mut first = request(Some("127.0.0.1"));
        first
            .headers_mut()
            .insert("x-forwarded-for", "1.1.1.1".parse().unwrap());

        let first = service.ready().await.unwrap().call(first).await.unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        let mut second = request(Some("127.0.0.1"));
        second
            .headers_mut()
            .insert("x-forwarded-for", "2.2.2.2".parse().unwrap());

        let second = service.ready().await.unwrap().call(second).await.unwrap();
        assert_eq!(second.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn trusted_proxy_forwarded_clients_have_independent_quota() {
        let mut service = layer().layer(ok_service());

        let mut first = request(Some("127.0.0.1"));
        first
            .headers_mut()
            .insert("x-forwarded-for", "1.1.1.1".parse().unwrap());

        let first = service.ready().await.unwrap().call(first).await.unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        let mut second = request(Some("127.0.0.1"));
        second
            .headers_mut()
            .insert("x-forwarded-for", "2.2.2.2".parse().unwrap());

        let second = service.ready().await.unwrap().call(second).await.unwrap();
        assert_eq!(second.status(), StatusCode::OK);

        let mut third = request(Some("127.0.0.1"));
        third
            .headers_mut()
            .insert("x-forwarded-for", "1.1.1.1".parse().unwrap());

        let third = service.ready().await.unwrap().call(third).await.unwrap();
        assert_eq!(third.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn untrusted_forwarded_header_is_ignored() {
        let mut service = layer().layer(ok_service());

        let mut first = request(Some("9.9.9.9"));
        first
            .headers_mut()
            .insert("x-forwarded-for", "1.1.1.1".parse().unwrap());

        let first = service.ready().await.unwrap().call(first).await.unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        let mut second = request(Some("9.9.9.9"));
        second
            .headers_mut()
            .insert("x-forwarded-for", "2.2.2.2".parse().unwrap());

        let second = service.ready().await.unwrap().call(second).await.unwrap();
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[cfg(feature = "axum")]
    #[tokio::test]
    async fn peer_addr_reads_axum_connect_info() {
        let mut service = layer().layer(ok_service());
        let mut req = Request::new(());
        req.extensions_mut()
            .insert(axum::extract::ConnectInfo(socket("1.2.3.4")));

        let response = service.ready().await.unwrap().call(req).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
