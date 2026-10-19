//! A count of bytes as a bundle states it: `"4 GiB"`, `"250 MB"`, or a bare number of bytes.
//!
//! Binary units are what every budget here is stated in, so the file is written back in them: the largest unit
//! that divides the value exactly, else bytes. Decimal units are accepted because people type them.

use serde::Serializer;
use serde::de::{self, Deserializer, Visitor};

const BINARY: &[(&str, u64)] = &[("TiB", 1 << 40), ("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)];
const DECIMAL: &[(&str, u64)] = &[
    ("TB", 1_000_000_000_000),
    ("GB", 1_000_000_000),
    ("MB", 1_000_000),
    ("KB", 1_000),
];

/// `"4 GiB"`, `"4GB"`, `"1.5 MiB"`, `"512"` or `"512 B"` to bytes. Case does not matter. A fraction must come out
/// whole: half a byte is refused.
pub fn parse(text: &str) -> Result<usize, String> {
    let text = text.trim();
    let split = text
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '_' || c == ','))
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let number: String = number.chars().filter(|c| *c != '_' && *c != ',').collect();
    let unit = unit.trim();
    if number.is_empty() {
        return Err(format!("{text:?} is not a size: it needs a number"));
    }
    let scale = if unit.is_empty() || unit.eq_ignore_ascii_case("B") {
        1
    } else {
        BINARY
            .iter()
            .chain(DECIMAL)
            .find(|(name, _)| name.eq_ignore_ascii_case(unit))
            .map(|(_, scale)| *scale)
            .ok_or_else(|| {
                format!("{text:?} is not a size: the unit must be one of B, KB, MB, GB, TB, KiB, MiB, GiB, TiB")
            })?
    };
    if let Ok(whole) = number.parse::<u64>() {
        return whole
            .checked_mul(scale)
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| format!("{text:?} is too large a size"));
    }
    let fraction: f64 = number
        .parse()
        .map_err(|_| format!("{text:?} is not a size: {number:?} is not a number"))?;
    let bytes = fraction * scale as f64;
    if bytes < 0.0 || bytes.fract() != 0.0 {
        return Err(format!("{text:?} is not a whole number of bytes"));
    }
    // `usize::MAX as f64` rounds up to 2^64, one past the largest size, so equal is already too large.
    if bytes >= usize::MAX as f64 {
        return Err(format!("{text:?} is too large a size"));
    }
    Ok(bytes as usize)
}

/// `bytes` in the largest binary unit that divides it exactly, else as bytes: `64 MiB`, `1 GiB`, `4000000 B`. What the
/// bundle carries, and what an error prints, so the two read the same.
pub fn exact(bytes: usize) -> String {
    let n = bytes as u64;
    if n > 0 {
        for (name, scale) in BINARY {
            if n.is_multiple_of(*scale) {
                return format!("{} {name}", n / scale);
            }
        }
    }
    format!("{n} B")
}

/// Serializes a byte count as [`exact`] text.
pub fn serialize<S: Serializer>(bytes: &usize, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&exact(*bytes))
}

/// Deserializes a byte count from a number of bytes or [`parse`] text.
pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<usize, D::Error> {
    struct ByteCount;
    impl<'de> Visitor<'de> for ByteCount {
        type Value = usize;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a number of bytes, or a size such as \"64 MiB\"")
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<usize, E> {
            usize::try_from(v).map_err(|_| E::custom("too large a size"))
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<usize, E> {
            if v < 0 {
                return Err(E::custom("a size cannot be negative"));
            }
            usize::try_from(v).map_err(|_| E::custom("too large a size"))
        }
        fn visit_f64<E: de::Error>(self, v: f64) -> Result<usize, E> {
            if v < 0.0 || v.fract() != 0.0 || v > usize::MAX as f64 {
                return Err(E::custom("a size is a whole number of bytes"));
            }
            Ok(v as usize)
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<usize, E> {
            parse(v).map_err(E::custom)
        }
    }
    deserializer.deserialize_any(ByteCount)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_size_reads_in_either_unit_family_or_as_bare_bytes() {
        assert_eq!(parse("4 GiB"), Ok(4 << 30));
        assert_eq!(parse("4GB"), Ok(4_000_000_000));
        assert_eq!(parse("4 gb"), Ok(4_000_000_000));
        assert_eq!(parse("64 MiB"), Ok(64 << 20));
        assert_eq!(parse("1.5 MiB"), Ok(3 << 19));
        assert_eq!(parse("512"), Ok(512));
        assert_eq!(parse(" 512 B "), Ok(512));
        assert_eq!(parse("1,000 KB"), Ok(1_000_000));
    }

    #[test]
    fn what_is_not_a_size_is_refused_by_name() {
        assert!(parse("").unwrap_err().contains("needs a number"));
        assert!(parse("GB").unwrap_err().contains("needs a number"));
        assert!(parse("4 parsecs").unwrap_err().contains("unit must be"));
        assert!(parse("1.5 B").unwrap_err().contains("whole number"));
        assert!(parse("99999999999 TiB").unwrap_err().contains("too large"));
        assert!(parse("18446744073709551616.0 B").unwrap_err().contains("too large"));
    }

    #[test]
    fn the_file_carries_the_largest_binary_unit_that_divides_exactly() {
        assert_eq!(exact(64 << 20), "64 MiB");
        assert_eq!(exact(1 << 30), "1 GiB");
        assert_eq!(exact(3 << 19), "1536 KiB");
        assert_eq!(exact(4_000_000), "4000000 B");
        assert_eq!(exact(0), "0 B");
    }

    /// A field the way the DTOs carry one.
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(transparent)]
    struct ByteCount(#[serde(with = "super")] usize);

    #[test]
    fn a_count_survives_the_file_in_text_form() {
        let text = serde_json::to_string(&ByteCount(64 << 20)).unwrap();
        assert_eq!(text, r#""64 MiB""#);
        assert_eq!(serde_json::from_str::<ByteCount>(&text).unwrap(), ByteCount(64 << 20));
        assert_eq!(
            serde_json::from_str::<ByteCount>("67108864").unwrap(),
            ByteCount(64 << 20)
        );
        assert_eq!(
            serde_json::from_str::<ByteCount>(r#""4 GB""#).unwrap(),
            ByteCount(4_000_000_000)
        );
        assert!(serde_json::from_str::<ByteCount>(r#""soon""#).is_err());
    }
}
