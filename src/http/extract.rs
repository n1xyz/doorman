use crate::IpKey;
use http::{HeaderMap, Request};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use std::net::{IpAddr, SocketAddr};

/// Extracts the real client IP from peer connection info and HTTP proxy headers.
///
/// Forwarding headers are trusted only when the peer IP is in the configured
/// trusted proxy networks.
#[derive(Clone, Debug)]
pub struct ClientIpExtractor {
    trusted_v4: Box<[Ipv4Net]>,
    trusted_v6: Box<[Ipv6Net]>,
}

/// Error returned when a client IP cannot be extracted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExtractClientIpError {
    /// The peer is trusted, but no usable forwarded client IP header was found.
    MissingForwardedClientIp,
}

impl ClientIpExtractor {
    /// Creates an extractor with the trusted proxy networks.
    pub fn with_trusted_proxies(trusted_proxies: impl IntoIterator<Item = IpNet>) -> Self {
        let (trusted_v4, trusted_v6) = split_nets(trusted_proxies);
        Self {
            trusted_v4,
            trusted_v6,
        }
    }

    /// Extracts the real client IP for a request.
    ///
    /// If `peer_addr` is trusted, this checks `X-Forwarded-For`, then
    /// `X-Real-IP`, then `Forwarded`. Otherwise forwarding headers are ignored.
    pub fn extract(
        &self,
        peer_addr: SocketAddr,
        headers: &HeaderMap,
    ) -> Result<IpAddr, ExtractClientIpError> {
        let peer_ip = peer_addr.ip();

        if self.is_trusted_proxy(peer_ip) {
            maybe_x_forwarded_for(headers)
                .or_else(|| maybe_x_real_ip(headers))
                .or_else(|| maybe_forwarded(headers))
                .ok_or(ExtractClientIpError::MissingForwardedClientIp)
        } else {
            Ok(peer_ip)
        }
    }

    /// Extracts the real client IP and converts it into an [`IpKey`].
    pub fn extract_key(
        &self,
        peer_addr: SocketAddr,
        headers: &HeaderMap,
    ) -> Result<IpKey, ExtractClientIpError> {
        self.extract(peer_addr, headers).map(IpKey::from)
    }

    /// Returns whether the IP is in the configured trusted proxy networks.
    pub fn is_trusted_proxy(&self, ip: IpAddr) -> bool {
        match ip {
            IpAddr::V4(v4) => self.trusted_v4.iter().any(|net| net.contains(&v4)),
            IpAddr::V6(v6) => self.trusted_v6.iter().any(|net| net.contains(&v6)),
        }
    }
}

pub(crate) fn split_nets(
    nets: impl IntoIterator<Item = IpNet>,
) -> (Box<[Ipv4Net]>, Box<[Ipv6Net]>) {
    let (mut v4, mut v6) = (Vec::new(), Vec::new());
    for net in nets {
        match net {
            IpNet::V4(n) => v4.push(n),
            IpNet::V6(n) => v6.push(n),
        }
    }
    (v4.into(), v6.into())
}

pub(crate) fn peer_addr<B>(req: &Request<B>) -> Option<SocketAddr> {
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

fn maybe_x_forwarded_for(headers: &HeaderMap) -> Option<IpAddr> {
    headers
        .get("x-forwarded-for")
        .and_then(|hv| hv.to_str().ok())
        .and_then(|s| {
            s.split(',')
                // The leftmost IP is the original client; proxies append to the right.
                .find_map(|s| s.trim().parse::<IpAddr>().ok())
        })
}

fn maybe_x_real_ip(headers: &HeaderMap) -> Option<IpAddr> {
    headers
        .get("x-real-ip")
        .and_then(|hv| hv.to_str().ok())
        .and_then(|s| s.parse::<IpAddr>().ok())
}

fn maybe_forwarded(headers: &HeaderMap) -> Option<IpAddr> {
    use forwarded_header_value::{ForwardedHeaderValue, Identifier};
    use http::header::FORWARDED;

    headers.get_all(FORWARDED).iter().find_map(|hv| {
        hv.to_str()
            .ok()
            .and_then(|s| ForwardedHeaderValue::from_forwarded(s).ok())
            .and_then(|f| {
                f.iter()
                    .filter_map(|fs| fs.forwarded_for.as_ref())
                    .find_map(|ff| match ff {
                        Identifier::SocketAddr(a) => Some(a.ip()),
                        Identifier::IpAddr(ip) => Some(*ip),
                        _ => None,
                    })
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::FORWARDED;

    fn extractor(trusted: &[&str]) -> ClientIpExtractor {
        ClientIpExtractor::with_trusted_proxies(
            trusted.iter().map(|net| net.parse::<IpNet>().unwrap()),
        )
    }

    fn socket(ip: &str) -> SocketAddr {
        SocketAddr::new(ip.parse().unwrap(), 12345)
    }

    #[test]
    fn untrusted_peer_ignores_forwarded_headers() {
        let extractor = extractor(&["127.0.0.0/8"]);
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "9.9.9.9".parse().unwrap());

        let ip = extractor.extract(socket("1.2.3.4"), &headers).unwrap();

        assert_eq!(ip, "1.2.3.4".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn trusted_proxy_uses_x_forwarded_for_first() {
        let extractor = extractor(&["127.0.0.0/8"]);
        let mut headers = HeaderMap::new();
        headers.insert("x-real-ip", "8.8.8.8".parse().unwrap());
        headers.insert("x-forwarded-for", "9.9.9.9, 127.0.0.1".parse().unwrap());

        let ip = extractor.extract(socket("127.0.0.1"), &headers).unwrap();

        assert_eq!(ip, "9.9.9.9".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn trusted_proxy_falls_back_to_x_real_ip() {
        let extractor = extractor(&["127.0.0.0/8"]);
        let mut headers = HeaderMap::new();
        headers.insert("x-real-ip", "8.8.8.8".parse().unwrap());

        let ip = extractor.extract(socket("127.0.0.1"), &headers).unwrap();

        assert_eq!(ip, "8.8.8.8".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn trusted_proxy_falls_back_to_forwarded() {
        let extractor = extractor(&["127.0.0.0/8"]);
        let mut headers = HeaderMap::new();
        headers.insert(FORWARDED, "for=8.8.4.4;proto=https".parse().unwrap());

        let ip = extractor.extract(socket("127.0.0.1"), &headers).unwrap();

        assert_eq!(ip, "8.8.4.4".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn trusted_proxy_without_client_header_errors() {
        let extractor = extractor(&["127.0.0.0/8"]);
        let headers = HeaderMap::new();

        let err = extractor
            .extract(socket("127.0.0.1"), &headers)
            .unwrap_err();

        assert_eq!(err, ExtractClientIpError::MissingForwardedClientIp);
    }
}
