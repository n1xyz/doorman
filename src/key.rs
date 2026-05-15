use std::net::IpAddr;

/// Key type for IP-based rate limiting.
///
/// IPv4 addresses are keyed exactly. IPv6 addresses are grouped by /56 prefix
/// to avoid treating every IPv6 address as an independent client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IpKey {
    /// Exact IPv4 address bits.
    V4(u32),

    /// IPv6 /56 prefix represented as a `u64`.
    V6(u64),
}

impl From<IpAddr> for IpKey {
    fn from(ip: IpAddr) -> Self {
        match ip {
            IpAddr::V4(v4) => Self::V4(ipv4_to_u32(v4)),
            IpAddr::V6(v6) => Self::V6(ipv6_to_u64(v6)),
        }
    }
}

fn ipv4_to_u32(ip: std::net::Ipv4Addr) -> u32 {
    ip.to_bits()
}

/// TODO: look to improve this?
///
/// Rate limiting IPv6 addresses is in an incredibly poor state.
///
/// We naively rate limit by /56 prefixes in that case. This may lead
/// to false positives, but the additional 256 requests is probably not
/// an issue for most users, given our generous rate limits. Thanks to
/// the /56, we can map the IPs to a single `u64` to use as the key.
///
/// http://essay.utwente.nl/96014/1/van%20Heijningen_BA_EEMCS.pdf
fn ipv6_to_u64(ip: std::net::Ipv6Addr) -> u64 {
    let ip = (ip.to_bits() >> 64) as u64;
    ip & 0xffff_ffff_ffff_ff00
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn ipv4_key_uses_exact_address_bits() {
        let ip = Ipv4Addr::new(1, 2, 3, 4);

        assert_eq!(IpKey::from(IpAddr::V4(ip)), IpKey::V4(ip.to_bits()));
    }

    #[test]
    fn ipv6_addresses_in_same_56_share_key() {
        let ip1: IpAddr = "2001:db8:1234:5600::1".parse().unwrap();
        let ip2: IpAddr = "2001:db8:1234:5600::ffff".parse().unwrap();

        assert_eq!(IpKey::from(ip1), IpKey::from(ip2));
    }

    #[test]
    fn ipv6_addresses_in_different_56_have_different_keys() {
        let ip1: IpAddr = "2001:db8:1234:5600::1".parse().unwrap();
        let ip2: IpAddr = "2001:db8:1234:5700::1".parse().unwrap();

        assert_ne!(IpKey::from(ip1), IpKey::from(ip2));
    }
}
