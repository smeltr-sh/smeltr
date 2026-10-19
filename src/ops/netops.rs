//! IP addresses as text: `isIp`, `ipVersion`, `ipNormalize`, `ipScope`, `inCidr` and `cidrNetwork`.
//!
//! An address is a `Str`. Each kernel parses it and writes text back. Text that is not an address is a null row, as
//! `parseInt` gives null for text that is not a number. Space around an address is ignored, since a parser leaves
//! it there.
//!
//! A CIDR block given as a literal meets the kernel as a constant column and is parsed once for the batch.

use std::net::{IpAddr, Ipv6Addr};

use ipnetwork::IpNetwork;

use super::helpers::{Operand, Side, alloc_with_merged_validity, short_circuit_empty, side};
use crate::core::bits;
use crate::core::column::{Column, DataType, StrBuilder, VarLen, VarType};
use crate::core::ctx::Ctx;
use crate::format::bytes_text::BytesForm;

/// An address as grok's `IP` reads one: an IPv4 octet has no leading zero, and an IPv6 address may carry a zone
/// (`fe80::1%eth0`). The zone names a link, not part of the address, so only `ipNormalize` writes it back.
pub(crate) fn parse_ip(text: &[u8]) -> Option<IpAddr> {
    split_zone(text).map(|(ip, _)| ip)
}

/// The address and its zone, with the `%`: `%eth0`, or empty for none.
fn split_zone(text: &[u8]) -> Option<(IpAddr, &str)> {
    let text = std::str::from_utf8(text).ok()?.trim();
    let (addr, zone) = text.split_at(text.find('%').unwrap_or(text.len()));
    let zone_ok = zone.is_empty()
        || zone.len() > 1
            && zone[1..]
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._~-".contains(&b));
    match addr.parse().ok()? {
        IpAddr::V4(_) if !zone.is_empty() => None,
        ip => zone_ok.then_some((ip, zone)),
    }
}

/// A block, or a bare address as the block holding only itself.
fn parse_net(text: &[u8]) -> Option<IpNetwork> {
    std::str::from_utf8(text).ok()?.trim().parse().ok()
}

/// `isIp(text)`: the text is an address. Text that is not one is `false`. Null stays null.
pub fn is_ip(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Bool, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let mut out = alloc_with_merged_validity(ctx, &DataType::Bool, len, &[Some(v.validity)]);
    {
        let o = out.bool_mut().unwrap();
        for i in 0..len {
            let hit = o.validity.is_valid(i) && parse_ip(v.get(i)).is_some();
            bits::set_row(o.values, i, hit);
        }
    }
    out
}

/// `ipVersion(text)`: 4 or 6.
pub fn ip_version(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::I64, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let mut out = alloc_with_merged_validity(ctx, &DataType::I64, len, &[Some(v.validity)]);
    {
        let o = out.i64_mut().unwrap();
        for i in 0..len {
            // Every row is written: a pooled buffer is not zeroed.
            match o.validity.is_valid(i).then(|| parse_ip(v.get(i))).flatten() {
                Some(IpAddr::V4(_)) => o.values[i] = 4,
                Some(IpAddr::V6(_)) => o.values[i] = 6,
                None => {
                    o.values[i] = 0;
                    o.validity.set_valid(i, false);
                }
            }
        }
        o.validity.recount();
    }
    out
}

/// `ipNormalize(text)`: the address in one spelling. Leading zeros go. IPv6 is lowercase with its longest run of
/// zero groups compressed, as RFC 5952 writes it. A zone stays as written.
pub fn ip_normalize(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Str, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
    for i in 0..len {
        match v.validity.is_valid(i).then(|| split_zone(v.get(i))).flatten() {
            Some((ip, zone)) => b.push_grow(ctx.pool, format!("{ip}{zone}").as_bytes()),
            None => b.push_null(),
        }
    }
    b.finish()
}

/// Where an address reaches. The first that fits wins, so `::1` is `loopback`.
fn scope(ip: IpAddr) -> &'static str {
    match ip {
        IpAddr::V4(a) => {
            if a.is_unspecified() {
                "unspecified"
            } else if a.is_loopback() {
                "loopback"
            } else if a.is_link_local() {
                "link-local"
            } else if a.is_multicast() {
                "multicast"
            } else if a.is_private() {
                "private"
            } else {
                "public"
            }
        }
        IpAddr::V6(a) => {
            if a.is_unspecified() {
                "unspecified"
            } else if a.is_loopback() {
                "loopback"
            } else if is_v6_link_local(a) {
                "link-local"
            } else if a.is_multicast() {
                "multicast"
            } else if is_v6_unique_local(a) {
                "private"
            } else {
                "public"
            }
        }
    }
}

/// `fe80::/10`.
fn is_v6_link_local(a: Ipv6Addr) -> bool {
    a.segments()[0] & 0xffc0 == 0xfe80
}

/// `fc00::/7`, the IPv6 counterpart of the RFC 1918 ranges.
fn is_v6_unique_local(a: Ipv6Addr) -> bool {
    a.segments()[0] & 0xfe00 == 0xfc00
}

/// `ipScope(text)`: `unspecified`, `loopback`, `link-local`, `multicast`, `private` or `public`.
pub fn ip_scope(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Str, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
    for i in 0..len {
        match v.validity.is_valid(i).then(|| parse_ip(v.get(i))).flatten() {
            Some(ip) => b.push_grow(ctx.pool, scope(ip).as_bytes()),
            None => b.push_null(),
        }
    }
    b.finish()
}

/// A block argument as the loop reads it: a constant parsed once, or rows parsed as they come.
enum Block<'a> {
    One(Option<IpNetwork>),
    Rows(&'a Side<'a, VarLen<'a>, &'a [u8]>),
}

impl Block<'_> {
    /// `None` for a null row, or text that is not a block.
    fn at(&self, i: usize) -> Option<IpNetwork> {
        match self {
            Block::One(net) => *net,
            Block::Rows(s) => s.is_valid(i).then(|| parse_net(s.at(i))).flatten(),
        }
    }
}

/// `inCidr(address, block, ...)`: the address is in one of the blocks. A block of the other family holds nothing.
/// A row whose address or any block does not parse is null.
pub fn in_cidr(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Bool, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let sides: Vec<_> = ins[1..].iter().map(|c| side::var(c).unwrap()).collect();
    let blocks: Vec<Block> = sides
        .iter()
        .map(|s| match s {
            Side::One(text) => Block::One(parse_net(text)),
            Side::Rows(..) => Block::Rows(s),
        })
        .collect();
    let mut bitmaps = vec![Some(v.validity)];
    bitmaps.extend(sides.iter().map(|s| s.validity()));
    let mut out = alloc_with_merged_validity(ctx, &DataType::Bool, len, &bitmaps);
    {
        let o = out.bool_mut().unwrap();
        for i in 0..len {
            let mut hit = false;
            if o.validity.is_valid(i) {
                match parse_ip(v.get(i)) {
                    None => o.validity.set_valid(i, false),
                    Some(ip) => {
                        for block in &blocks {
                            match block.at(i) {
                                Some(net) => hit |= net.contains(ip),
                                None => {
                                    o.validity.set_valid(i, false);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            bits::set_row(o.values, i, hit && o.validity.is_valid(i));
        }
        o.validity.recount();
    }
    out
}

/// `cidrNetwork(address, prefix)`: the address masked to `prefix` bits, written as a block: `10.1.2.0/24`. A prefix
/// past the width of the address is null.
pub fn cidr_network(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Str, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let prefix = side::i64(ins[1]).unwrap();
    let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
    for i in 0..len {
        if !v.validity.is_valid(i) || !prefix.is_valid(i) {
            b.push_null();
            continue;
        }
        let net = parse_ip(v.get(i))
            .zip(u8::try_from(prefix.at(i)).ok())
            .and_then(|(ip, bits)| IpNetwork::new(ip, bits).ok());
        match net {
            Some(net) => b.push_grow(ctx.pool, format!("{}/{}", net.network(), net.prefix()).as_bytes()),
            None => b.push_null(),
        }
    }
    b.finish()
}

/// `communityId(srcIp, srcPort, dstIp, dstPort, transport)` and `communityId(…, seed)`: the flow's Community ID, version
/// 1 (`github.com/corelight/community-id-spec`), as Elastic's `community_id` computes it. `transport` is a protocol
/// number or a name, `tcp`, `udp`, `sctp`, `icmp`, `icmpv6`, `igmp`, `gre`, `eigrp`, `ospf` or `pim`. For ICMP, the
/// ports are the type and the code. A row is null where an address or the transport is null or does not parse, or
/// where TCP, UDP or SCTP has a port missing or outside 1-65535.
pub fn community_id(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(&[ins[0], ins[2], ins[4]], &DataType::Str, len) {
        return e;
    }
    let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
    for i in 0..len {
        let text = |c| Column::is_valid(c, i).then(|| text_at(c, i)).flatten();
        let int = |c| Column::is_valid(c, i).then(|| int_at(c, i)).flatten();
        let (Some(src), Some(dst)) = (text(ins[0]), text(ins[2])) else {
            b.push_null();
            continue;
        };
        if !ins[4].is_valid(i) {
            b.push_null();
            continue;
        }
        let transport = match ins[4].data_type() {
            DataType::I64 => int(ins[4]).and_then(Transport::from_number),
            _ => text(ins[4]).and_then(Transport::from_text),
        };
        let seed = match ins.get(5) {
            Some(c) => int(c).and_then(|s| u16::try_from(s).ok()),
            None => Some(0),
        };
        let flow = (|| {
            let (src, dst) = (parse_ip(src)?, parse_ip(dst)?);
            // Both endpoints of a flow are one family: the hash's address fields are both 4 bytes or both 16.
            if src.is_ipv4() != dst.is_ipv4() {
                return None;
            }
            let transport = transport?;
            let (sp, dp) = (int(ins[1]), int(ins[3]));
            let ports = match transport {
                Transport::Tcp | Transport::Udp | Transport::Sctp => {
                    let port = |p: Option<i64>| p.filter(|p| (1..=65535).contains(p)).map(|p| p as u16);
                    Some((port(sp)?, port(dp)?))
                }
                Transport::Icmp | Transport::Icmp6 => {
                    let code = |p: Option<i64>| u16::try_from(p.unwrap_or(0)).ok();
                    Some((code(sp)?, code(dp)?))
                }
                Transport::Other(_) => None,
            };
            Some(flow_id(seed?, src, dst, transport, ports))
        })();
        match flow {
            Some(id) => b.push_grow(ctx.pool, id.as_bytes()),
            None => b.push_null(),
        }
    }
    b.finish()
}

fn text_at(c: &Column, i: usize) -> Option<&[u8]> {
    match c.constant_value() {
        Some(crate::core::column::Scalar::Str(s)) => Some(s.as_bytes()),
        _ => c.var().ok().map(|v| v.get(i)),
    }
}

fn int_at(c: &Column, i: usize) -> Option<i64> {
    match c.constant_value() {
        Some(crate::core::column::Scalar::I64(x)) => Some(*x),
        _ => c.i64().ok().map(|v| v.values[i]),
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
enum Transport {
    Icmp,
    Icmp6,
    Tcp,
    Udp,
    Sctp,
    Other(u8),
}

impl Transport {
    /// An IANA protocol number.
    fn from_number(n: i64) -> Option<Transport> {
        Some(match u8::try_from(n).ok()? {
            1 => Transport::Icmp,
            6 => Transport::Tcp,
            17 => Transport::Udp,
            58 => Transport::Icmp6,
            132 => Transport::Sctp,
            n => Transport::Other(n),
        })
    }

    fn from_text(text: &[u8]) -> Option<Transport> {
        let name = std::str::from_utf8(text).ok()?.trim().to_ascii_lowercase();
        Some(match name.as_str() {
            "icmp" => Transport::Icmp,
            "igmp" => Transport::Other(2),
            "tcp" => Transport::Tcp,
            "udp" => Transport::Udp,
            "gre" => Transport::Other(47),
            "ipv6-icmp" | "icmpv6" => Transport::Icmp6,
            "eigrp" => Transport::Other(88),
            "ospf" => Transport::Other(89),
            "pim" => Transport::Other(103),
            "sctp" => Transport::Sctp,
            other => return Transport::from_number(other.parse().ok()?),
        })
    }

    fn number(self) -> u8 {
        match self {
            Transport::Icmp => 1,
            Transport::Tcp => 6,
            Transport::Udp => 17,
            Transport::Icmp6 => 58,
            Transport::Sctp => 132,
            Transport::Other(n) => n,
        }
    }
}

/// The ICMP type a reply to `ty` carries, for the types that have one: a request and its reply are one flow.
fn icmp_counterpart(ty: u16, v6: bool) -> Option<u16> {
    let pairs: &[(u16, u16)] = if v6 {
        &[
            (128, 129),
            (129, 128),
            (133, 134),
            (134, 133),
            (135, 136),
            (136, 135),
            (130, 131),
            (139, 140),
            (140, 139),
            (144, 145),
            (145, 144),
        ]
    } else {
        &[
            (8, 0),
            (0, 8),
            (13, 14),
            (14, 13),
            (15, 16),
            (10, 9),
            (9, 10),
            (17, 18),
            (18, 17),
        ]
    };
    pairs.iter().find(|(t, _)| *t == ty).map(|(_, r)| *r)
}

/// `1:` and the base64 SHA-1 of the seed, the ordered endpoints, the protocol and its padding byte, and the ports.
fn flow_id(seed: u16, src: IpAddr, dst: IpAddr, transport: Transport, ports: Option<(u16, u16)>) -> String {
    let octets = |ip: IpAddr| match ip {
        IpAddr::V4(a) => a.octets().to_vec(),
        IpAddr::V6(a) => a.octets().to_vec(),
    };
    let (src, dst) = (octets(src), octets(dst));
    let mut one_way = false;
    let ports = ports.map(|(sp, dp)| match transport {
        Transport::Icmp | Transport::Icmp6 => {
            let counterpart = icmp_counterpart(sp, transport == Transport::Icmp6);
            one_way = counterpart.is_none();
            (sp, counterpart.unwrap_or(dp))
        }
        _ => (sp, dp),
    });
    let (sp, dp) = ports.unwrap_or((0, 0));
    // The addresses compare as unsigned numbers, so an IPv4 one meets an IPv6 one by value.
    let number = |a: &[u8]| {
        let a = &a[a.iter().position(|&b| b != 0).unwrap_or(a.len())..];
        (a.len(), a.to_vec())
    };
    let ordered = match number(&src).cmp(&number(&dst)) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Equal => sp < dp,
        std::cmp::Ordering::Greater => false,
    };
    let keep = ordered || one_way;
    let (a, b, pa, pb) = if keep {
        (&src, &dst, sp, dp)
    } else {
        (&dst, &src, dp, sp)
    };
    let mut msg = Vec::with_capacity(2 + 32 + 2 + 4);
    msg.extend_from_slice(&seed.to_be_bytes());
    msg.extend_from_slice(a);
    msg.extend_from_slice(b);
    msg.extend_from_slice(&[transport.number(), 0]);
    if ports.is_some() {
        msg.extend_from_slice(&pa.to_be_bytes());
        msg.extend_from_slice(&pb.to_be_bytes());
    }
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA1_FOR_LEGACY_USE_ONLY, &msg);
    format!("1:{}", BytesForm::Base64.text(digest.as_ref()))
}
