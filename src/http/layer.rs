use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::{Request, Response, StatusCode};
use tower::{Layer, Service};

use crate::{IpKey, RateLimitError, RequestRateLimiter, http::ClientIpExtractor};

pub struct RateLimitLayer {
    limiter: Arc<RequestRateLimiter<IpKey>>,
    extractor: ClientIpExtractor,
}

pub struct RateLimitService<S> {
    inner: S,
    limiter: Arc<RequestRateLimiter<IpKey>>,
    extractor: ClientIpExtractor,
}

impl<S> Layer<S> for RateLimitLayer {
    type Service = RateLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimitService {
            inner,
            limiter: Arc::clone(&self.limiter),
            extractor: self.extractor.clone(),
        }
    }
}

impl RateLimitLayer {
    pub fn new(limiter: Arc<RequestRateLimiter<IpKey>>, extractor: ClientIpExtractor) -> Self {
        Self { limiter, extractor }
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

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let Some(peer_addr) = req.extensions().get::<SocketAddr>().copied() else {
            return RateLimitFuture::MissingPeer;
        };

        let key = match self.extractor.extract_key(peer_addr, req.headers()) {
            Ok(key) => key,
            Err(_) => return RateLimitFuture::ExtractFailed,
        };

        match self.limiter.check_request(&key) {
            Ok(()) => RateLimitFuture::Allowed(self.inner.call(req)),
            Err(err) => RateLimitFuture::Limited(err),
        }
    }
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
