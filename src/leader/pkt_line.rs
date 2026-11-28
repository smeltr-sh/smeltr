//! git's pkt-line framing, which every message of git's protocols is made of: the leader's pushes to a git host
//! (`remote`) and the fetches it serves workers (`upload_pack`).

/// The most a pkt-line carries: git's 65520-byte packet less its four length digits.
pub(crate) const MAX_PAYLOAD: usize = 65516;

/// One packet: a line of data, or one of the markers that frame a message.
#[derive(Debug, PartialEq)]
pub(crate) enum Packet<'a> {
    Data(&'a [u8]),
    /// `0000`: the end of a message.
    Flush,
    /// `0001`: the end of a section of a protocol v2 message.
    Delim,
    /// `0002`: the end of a protocol v2 response over a stateless connection.
    ResponseEnd,
}

/// `payload` as one pkt-line: its length, in four hex digits counting themselves, then the bytes. `payload` is at
/// most [`MAX_PAYLOAD`].
pub(crate) fn line(payload: &[u8]) -> Vec<u8> {
    debug_assert!(payload.len() <= MAX_PAYLOAD);
    let mut line = format!("{:04x}", payload.len() + 4).into_bytes();
    line.extend_from_slice(payload);
    line
}

/// The first packet of `bytes`, and what follows it.
pub(crate) fn next(bytes: &[u8]) -> Result<(Packet<'_>, &[u8]), String> {
    let length = bytes
        .get(..4)
        .filter(|hex| hex.iter().all(u8::is_ascii_hexdigit))
        .and_then(|hex| usize::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok())
        .ok_or("a pkt-line without its length")?;
    let packet = match length {
        0 => Packet::Flush,
        1 => Packet::Delim,
        2 => Packet::ResponseEnd,
        3 => return Err("a pkt-line of 3 bytes, too few to hold its own length".to_string()),
        _ => Packet::Data(
            bytes
                .get(4..length)
                .ok_or_else(|| format!("a pkt-line of {length} bytes in {} left", bytes.len()))?,
        ),
    };
    let rest = match packet {
        Packet::Data(_) => &bytes[length..],
        _ => &bytes[4..],
    };
    Ok((packet, rest))
}
