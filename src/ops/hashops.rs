//! Digests of text: `md5`, `sha1`, `sha256` and `sha512`, each written as lowercase hex.
//!
//! The digest is of the text's bytes, UTF-8 for a `str`. AWS-LC computes the SHAs, as it does every other hash this
//! process takes; `md-5` computes MD5, which AWS-LC's Rust API does not offer (`Cargo.toml`).

use aws_lc_rs::digest;
use md5::Digest;

use super::helpers::short_circuit_empty;
use crate::core::column::{Column, DataType, StrBuilder, VarType};
use crate::core::ctx::Ctx;

pub fn md5(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    hex_digest(ctx, ins, |text, hex| to_hex(&md5::Md5::digest(text), hex))
}

pub fn sha1(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    hex_digest(ctx, ins, |text, hex| {
        to_hex(digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, text).as_ref(), hex)
    })
}

pub fn sha256(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    hex_digest(ctx, ins, |text, hex| {
        to_hex(digest::digest(&digest::SHA256, text).as_ref(), hex)
    })
}

pub fn sha512(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    hex_digest(ctx, ins, |text, hex| {
        to_hex(digest::digest(&digest::SHA512, text).as_ref(), hex)
    })
}

fn hex_digest(ctx: &mut Ctx, ins: &[&Column], digest_hex: impl Fn(&[u8], &mut Vec<u8>)) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Str, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
    let mut hex = Vec::with_capacity(2 * digest::MAX_OUTPUT_LEN);
    for i in 0..len {
        if !v.validity.is_valid(i) {
            b.push_null();
            continue;
        }
        hex.clear();
        digest_hex(v.get(i), &mut hex);
        b.push_grow(ctx.pool, &hex);
    }
    b.finish()
}

/// `bytes` as lowercase hex, appended to `out`.
pub(crate) fn to_hex(bytes: &[u8], out: &mut Vec<u8>) {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    for &byte in bytes {
        out.push(DIGITS[usize::from(byte >> 4)]);
        out.push(DIGITS[usize::from(byte & 0xf)]);
    }
}
