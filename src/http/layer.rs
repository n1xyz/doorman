use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use http::{Request, Response, StatusCode};
use tokio::time::{Sleep, sleep};
use tower::{Layer, Service};

use crate::RateLimitError;

#[derive(Debug, Clone, Copy)]
pub enum RateLimitOutcome {
    ResponseStatus(StatusCode),
    ServiceError,
    Timeout,
}

/// Reason a rate-limit strategy rejected a request before the inner service ran.
pub enum RateLimitRejection {
    /// The request was rejected by a limiter.
    Limited(RateLimitError),

    /// The request did not contain peer address information.
    MissingPeer,

    /// The strategy could not extract a usable client identity from the request.
    ExtractFailed,
}

/// Tower layer that applies a rate-limit strategy before calling the inner service.
///
/// The layer owns the Tower mechanics. The strategy owns the policy decision:
/// what key to extract, what cost to charge, and whether the request should be
/// rejected before the inner service runs.
#[derive(Clone)]
pub struct RateLimitLayer<T> {
    strategy: T,
}

/// Strategy hook run by [`RateLimitLayer`] before the inner service is called.
///
/// Implement this trait for custom pre-request policies, such as API-key
/// limits, user-account limits, or route-specific request costs.
///
/// If [`RateLimitStrategy::timeout`] returns a duration and that timeout fires,
/// the layer drops the inner service future and returns `429 Too Many Requests`.
/// Only async work that respects cancellation is actually stopped.
pub trait RateLimitStrategy<B>: Clone {
    type State;

    /// Checks and optionally mutates a request before the inner service runs.
    ///
    /// Returning `Ok(())` allows the request to continue. Returning
    /// [`RateLimitRejection`] causes [`RateLimitLayer`] to return the matching
    /// rejection response without calling the inner service.
    fn before_request(&self, req: &mut Request<B>) -> Result<Self::State, RateLimitRejection>;
    fn after_response(
        &self,
        state: Self::State,
        outcome: RateLimitOutcome,
        elapsed: Duration,
    ) -> Result<(), RateLimitRejection> {
        let _ = (state, outcome, elapsed);
        Ok(())
    }

    fn timeout(&self, state: &Self::State) -> Option<Duration> {
        let _ = state;
        None
    }
}

#[derive(Clone)]
pub struct RateLimitService<S, T> {
    inner: S,
    strategy: T,
}

impl<S, T> Layer<S> for RateLimitLayer<T>
where
    T: Clone,
{
    type Service = RateLimitService<S, T>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimitService {
            inner,
            strategy: self.strategy.clone(),
        }
    }
}

impl<T> RateLimitLayer<T> {
    /// Creates a layer from a rate-limit strategy.
    pub fn with_strategy(strategy: T) -> Self {
        Self { strategy }
    }
}

impl<S, B, T> Service<Request<B>> for RateLimitService<S, T>
where
    S: Service<Request<B>, Response = Response<B>>,
    T: RateLimitStrategy<B>,
    B: Default,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = RateLimitFuture<S::Future, B, T>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<B>) -> Self::Future {
        match self.strategy.before_request(&mut req) {
            Ok(state) => {
                let start = Instant::now();
                let timeout = self.strategy.timeout(&state);
                let timeout = timeout.map(|duration| Box::pin(sleep(duration)));

                RateLimitFuture::Allowed {
                    future: self.inner.call(req),
                    strategy: self.strategy.clone(),
                    state: Some(state),
                    start,
                    timeout,
                }
            }
            Err(RateLimitRejection::Limited(err)) => RateLimitFuture::Limited(err),
            Err(RateLimitRejection::MissingPeer) => RateLimitFuture::MissingPeer,
            Err(RateLimitRejection::ExtractFailed) => RateLimitFuture::ExtractFailed,
        }
    }
}

pub enum RateLimitFuture<F, B, T>
where
    T: RateLimitStrategy<B>,
{
    Allowed {
        future: F,
        strategy: T,
        state: Option<T::State>,
        start: Instant,
        timeout: Option<Pin<Box<Sleep>>>,
    },
    Limited(RateLimitError),
    MissingPeer,
    ExtractFailed,
    _Body(std::marker::PhantomData<B>),
}

impl<F, B, E, T> Future for RateLimitFuture<F, B, T>
where
    F: Future<Output = Result<Response<B>, E>>,
    B: Default,
    T: RateLimitStrategy<B>,
{
    type Output = Result<Response<B>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: We never move F out of self, and all other variants are trivially Unpin.
        match unsafe { self.get_unchecked_mut() } {
            RateLimitFuture::Allowed {
                future,
                strategy,
                state,
                start,
                timeout,
            } => {
                let poll = unsafe { Pin::new_unchecked(future) }.poll(cx);

                match poll {
                    Poll::Pending => {
                        if let Some(timeout) = timeout.as_mut() {
                            match timeout.as_mut().poll(cx) {
                                Poll::Pending => Poll::Pending,
                                Poll::Ready(()) => {
                                    let elapsed = start.elapsed();
                                    let state = state
                                        .take()
                                        .expect("state is present until response completes");
                                    let after = strategy.after_response(
                                        state,
                                        RateLimitOutcome::Timeout,
                                        elapsed,
                                    );

                                    match after {
                                        Ok(()) => Poll::Ready(Ok(rate_limited_response(
                                            RateLimitError::limited(Duration::ZERO), // 429
                                        ))),
                                        Err(RateLimitRejection::Limited(err)) => {
                                            Poll::Ready(Ok(rate_limited_response(err)))
                                        }
                                        Err(
                                            RateLimitRejection::MissingPeer
                                            | RateLimitRejection::ExtractFailed,
                                        ) => Poll::Ready(Ok(server_error_response())),
                                    }
                                }
                            }
                        } else {
                            Poll::Pending
                        }
                    }
                    Poll::Ready(result) => {
                        let elapsed = start.elapsed();
                        let state = state
                            .take()
                            .expect("state is present until response completes");

                        match result {
                            Ok(response) => {
                                let after = strategy.after_response(
                                    state,
                                    RateLimitOutcome::ResponseStatus(response.status()),
                                    elapsed,
                                );
                                match after {
                                    Ok(()) => Poll::Ready(Ok(response)),
                                    Err(RateLimitRejection::Limited(err)) => {
                                        Poll::Ready(Ok(rate_limited_response(err)))
                                    }
                                    Err(
                                        RateLimitRejection::MissingPeer
                                        | RateLimitRejection::ExtractFailed,
                                    ) => Poll::Ready(Ok(server_error_response())),
                                }
                            }
                            Err(err) => {
                                let after = strategy.after_response(
                                    state,
                                    RateLimitOutcome::ServiceError,
                                    elapsed,
                                );
                                match after {
                                    Ok(()) => Poll::Ready(Err(err)),
                                    Err(RateLimitRejection::Limited(err)) => {
                                        Poll::Ready(Ok(rate_limited_response(err)))
                                    }
                                    Err(
                                        RateLimitRejection::MissingPeer
                                        | RateLimitRejection::ExtractFailed,
                                    ) => Poll::Ready(Ok(server_error_response())),
                                }
                            }
                        }
                    }
                }
            }
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
    use crate::http::{ClientIpExtractor, DurationBudgetByIp, RequestCountByIp};
    use crate::{DurationBudgetLimiter, IpKey, Policy, RequestRateLimiter};
    use http::header::RETRY_AFTER;
    use ipnet::IpNet;
    use std::cell::Cell;
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use std::num::NonZeroU32;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::time::Duration;
    use tower::ServiceExt;
    use tower::service_fn;

    #[derive(Clone)]
    struct AlwaysAllow;

    impl<B> RateLimitStrategy<B> for AlwaysAllow {
        type State = ();
        fn before_request(&self, _req: &mut Request<B>) -> Result<(), RateLimitRejection> {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct AlwaysLimited;

    impl<B> RateLimitStrategy<B> for AlwaysLimited {
        type State = ();
        fn before_request(&self, _req: &mut Request<B>) -> Result<(), RateLimitRejection> {
            Err(RateLimitRejection::Limited(RateLimitError::limited(
                Duration::from_secs(2),
            )))
        }
    }

    #[derive(Clone)]
    struct RecordsAfterResponse {
        called: Rc<Cell<bool>>,
    }

    impl<B> RateLimitStrategy<B> for RecordsAfterResponse {
        type State = ();

        fn before_request(&self, _req: &mut Request<B>) -> Result<(), RateLimitRejection> {
            Ok(())
        }

        fn after_response(
            &self,
            _state: Self::State,
            _outcome: RateLimitOutcome,
            _elapsed: Duration,
        ) -> Result<(), RateLimitRejection> {
            self.called.set(true);
            Ok(())
        }
    }

    #[derive(Clone)]
    struct RejectsAfterResponse;

    impl<B> RateLimitStrategy<B> for RejectsAfterResponse {
        type State = ();

        fn before_request(&self, _req: &mut Request<B>) -> Result<(), RateLimitRejection> {
            Ok(())
        }

        fn after_response(
            &self,
            _state: Self::State,
            _outcome: RateLimitOutcome,
            _elapsed: Duration,
        ) -> Result<(), RateLimitRejection> {
            Err(RateLimitRejection::Limited(RateLimitError::limited(
                Duration::from_secs(2),
            )))
        }
    }

    #[derive(Clone)]
    struct TimeoutImmediately {
        called: Rc<Cell<bool>>,
    }

    impl<B> RateLimitStrategy<B> for TimeoutImmediately {
        type State = ();

        fn before_request(&self, _req: &mut Request<B>) -> Result<(), RateLimitRejection> {
            Ok(())
        }

        fn after_response(
            &self,
            _state: Self::State,
            _outcome: RateLimitOutcome,
            _elapsed: Duration,
        ) -> Result<(), RateLimitRejection> {
            self.called.set(true);
            Ok(())
        }

        fn timeout(&self, _state: &Self::State) -> Option<Duration> {
            Some(Duration::ZERO)
        }
    }

    #[derive(Clone)]
    struct OutcomeObserver {
        service_error_seen: Rc<Cell<bool>>,
        called: Rc<Cell<bool>>,
    }

    impl<B> RateLimitStrategy<B> for OutcomeObserver {
        type State = ();

        fn before_request(&self, _req: &mut Request<B>) -> Result<(), RateLimitRejection> {
            Ok(())
        }

        fn after_response(
            &self,
            _state: Self::State,
            outcome: RateLimitOutcome,
            _elapsed: Duration,
        ) -> Result<(), RateLimitRejection> {
            self.called.set(true);
            self.service_error_seen
                .set(matches!(outcome, RateLimitOutcome::ServiceError));
            Ok(())
        }
    }

    fn layer() -> RateLimitLayer<RequestCountByIp> {
        let policy = Policy::per_second(NonZeroU32::new(1).unwrap(), NonZeroU32::new(1).unwrap());
        let limiter = Arc::new(RequestRateLimiter::new(policy));
        let extractor =
            ClientIpExtractor::with_trusted_proxies(["127.0.0.0/8".parse::<IpNet>().unwrap()]);
        RateLimitLayer::with_strategy(RequestCountByIp::with_limiter(limiter, extractor))
    }

    fn whitelisted_layer() -> RateLimitLayer<RequestCountByIp> {
        let policy = Policy::per_second(NonZeroU32::new(1).unwrap(), NonZeroU32::new(1).unwrap());
        let limiter = Arc::new(RequestRateLimiter::new(policy));
        let extractor =
            ClientIpExtractor::with_trusted_proxies(["127.0.0.0/8".parse::<IpNet>().unwrap()]);
        let strategy = RequestCountByIp::with_limiter(limiter, extractor)
            .with_whitelist(["1.2.3.0/24".parse::<IpNet>().unwrap()]);
        RateLimitLayer::with_strategy(strategy)
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

    fn sleep_service(
        duration: Duration,
    ) -> impl Service<Request<()>, Response = Response<()>, Error = Infallible> {
        service_fn(move |_req: Request<()>| async move {
            tokio::time::sleep(duration).await;
            Ok::<_, Infallible>(Response::builder().status(StatusCode::OK).body(()).unwrap())
        })
    }

    fn pending_service() -> impl Service<Request<()>, Response = Response<()>, Error = Infallible> {
        service_fn(|_req: Request<()>| std::future::pending::<Result<Response<()>, Infallible>>())
    }

    #[derive(Clone, Copy, Debug)]
    struct ErrorServiceErr;

    impl std::fmt::Display for ErrorServiceErr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("error service")
        }
    }

    impl std::error::Error for ErrorServiceErr {}

    fn error_service() -> impl Service<Request<()>, Response = Response<()>, Error = ErrorServiceErr>
    {
        service_fn(|_req: Request<()>| async { Err(ErrorServiceErr) })
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
    async fn custom_strategy_can_allow_request() {
        let mut service = RateLimitLayer::with_strategy(AlwaysAllow).layer(ok_service());

        let response = service
            .ready()
            .await
            .unwrap()
            .call(request(None))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn custom_strategy_can_reject_request() {
        let mut service = RateLimitLayer::with_strategy(AlwaysLimited).layer(ok_service());

        let response = service
            .ready()
            .await
            .unwrap()
            .call(request(None))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("3")
        );
    }

    #[tokio::test]
    async fn custom_strategy_after_response_runs_after_inner_service() {
        let called = Rc::new(Cell::new(false));
        let strategy = RecordsAfterResponse {
            called: Rc::clone(&called),
        };
        let mut service = RateLimitLayer::with_strategy(strategy).layer(ok_service());

        let response = service
            .ready()
            .await
            .unwrap()
            .call(request(None))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(called.get());
    }

    #[tokio::test]
    async fn custom_strategy_after_response_can_reject_request() {
        let mut service = RateLimitLayer::with_strategy(RejectsAfterResponse).layer(ok_service());

        let response = service
            .ready()
            .await
            .unwrap()
            .call(request(None))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("3")
        );
    }

    #[tokio::test]
    async fn custom_strategy_timeout_returns_429_and_runs_after_response() {
        let called = Rc::new(Cell::new(false));
        let strategy = TimeoutImmediately {
            called: Rc::clone(&called),
        };
        let mut service = RateLimitLayer::with_strategy(strategy).layer(pending_service());

        let response = service
            .ready()
            .await
            .unwrap()
            .call(request(None))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(called.get());
        assert_eq!(
            response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
    }

    #[tokio::test]
    async fn custom_strategy_receives_service_error_outcome() {
        let service_error_seen = Rc::new(Cell::new(false));
        let called = Rc::new(Cell::new(false));
        let strategy = OutcomeObserver {
            service_error_seen: Rc::clone(&service_error_seen),
            called: Rc::clone(&called),
        };
        let mut service = RateLimitLayer::with_strategy(strategy).layer(error_service());

        let response = service.ready().await.unwrap().call(request(None)).await;
        assert!(response.is_err());
        assert!(called.get());
        assert!(service_error_seen.get());
    }

    #[tokio::test]
    async fn duration_budget_by_ip_charges_elapsed_time_after_response() {
        let policy = Policy::per_second(NonZeroU32::new(1).unwrap(), NonZeroU32::new(1).unwrap());
        let limiter = Arc::new(DurationBudgetLimiter::<IpKey>::new(policy));
        let extractor =
            ClientIpExtractor::with_trusted_proxies(["127.0.0.0/8".parse::<IpNet>().unwrap()]);
        let strategy = DurationBudgetByIp::with_limiter(limiter, extractor);
        let mut service =
            RateLimitLayer::with_strategy(strategy).layer(sleep_service(Duration::from_millis(2)));

        let response = service
            .ready()
            .await
            .unwrap()
            .call(request(Some("1.2.3.4")))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn duration_budget_by_ip_timeout_returns_429() {
        let policy = Policy::per_second(NonZeroU32::new(10).unwrap(), NonZeroU32::new(10).unwrap());
        let limiter = Arc::new(DurationBudgetLimiter::<IpKey>::new(policy));
        let extractor =
            ClientIpExtractor::with_trusted_proxies(["127.0.0.0/8".parse::<IpNet>().unwrap()]);
        let strategy =
            DurationBudgetByIp::with_limiter(limiter, extractor).with_timeout(Duration::ZERO);
        let mut service = RateLimitLayer::with_strategy(strategy).layer(pending_service());

        let response = service
            .ready()
            .await
            .unwrap()
            .call(request(Some("1.2.3.4")))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
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
