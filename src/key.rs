use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

const IPV4_SENTINEL_PREFIX: u64 = 0x2001_0db8_0000_0000;

/// Compact key type for IP-based rate limiting.
///
/// IPv4 addresses are keyed exactly by embedding their 32 bits into an internal
/// sentinel range. IPv6 addresses are grouped by /64 prefix.
///
/// This representation is compact, but it is not collision free for every
/// syntactically valid IPv6 address. The IPv4 sentinel uses 2001:db8::/32,
/// which is reserved for documentation and should not appear as real client traffic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct IpKey(u64);

impl IpKey {
    pub fn into_inner(self) -> u64 {
        self.0
    }
}

impl From<IpAddr> for IpKey {
    fn from(ip: IpAddr) -> Self {
        match ip {
            IpAddr::V4(v4) => Self(ipv4_to_u64(v4)),
            IpAddr::V6(v6) => Self(ipv6_to_u64(v6)),
        }
    }
}

fn ipv4_to_u64(ip: Ipv4Addr) -> u64 {
    IPV4_SENTINEL_PREFIX | u64::from(ip.to_bits())
}

/// Use the first 64 bits of the IPV6 address as the key:
///
/// This means that these share a key:
///
///   2600:db8:1234:5600::1
///   2600:db8:1234:5600::ffff
///
/// But these do not:
///
///   2600:db8:1234:5600::1
///   2600:db8:1234:5601::1
///
fn ipv6_to_u64(ip: Ipv6Addr) -> u64 {
    (ip.to_bits() >> 64) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn ipv4_key_uses_exact_address_bits() {
        let ip = Ipv4Addr::new(1, 2, 3, 4);

        assert_eq!(
            IpKey::from(IpAddr::V4(ip)).into_inner(),
            0x2001_0db8_0000_0000 | u64::from(ip.to_bits())
        );
    }

    #[test]
    fn ipv6_addresses_in_same_64_share_key() {
        let ip1: IpAddr = "2600:1f18:1234:5600::1".parse().unwrap();
        let ip2: IpAddr = "2600:1f18:1234:5600::ffff".parse().unwrap();

        assert_eq!(IpKey::from(ip1), IpKey::from(ip2));
    }

    #[test]
    fn ipv6_addresses_in_different_64_have_different_keys() {
        let ip1: IpAddr = "2600:1f18:1234:5600::1".parse().unwrap();
        let ip2: IpAddr = "2600:1f18:1234:5601::1".parse().unwrap();

        assert_ne!(IpKey::from(ip1), IpKey::from(ip2));
    }

    #[test]
    fn ip_key_is_one_u64() {
        assert_eq!(std::mem::size_of::<IpKey>(), std::mem::size_of::<u64>());
    }
}
