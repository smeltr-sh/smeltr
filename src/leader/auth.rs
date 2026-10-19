//! Worker enrollment: an operator mints a one-time code, a worker exchanges it exactly once for a Bearer token it
//! persists and sends on every request. Only hashes are stored, so a database dump alone hands out no working token.
//!
//! A credential says "this is an enrolled worker", not which group it may act on. Who a person is, and how their
//! password is verified, is `accounts`: a token is 256 random bits and a password is not, so they are hashed by
//! different functions for different reasons.

use aws_lc_rs::rand::SecureRandom;

/// 32 bytes of OS-backed randomness, hex-encoded. The shape of both an enrollment code and a token.
pub fn generate_secret() -> String {
    hex_encode(&random_bytes(32))
}

/// `n` bytes from the OS. Every unpredictable value in the leader comes from here: tokens, account ids, generated
/// passwords and session material, so there is one answer to where the randomness came from.
pub fn random_bytes(n: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; n];
    aws_lc_rs::rand::SystemRandom::new()
        .fill(&mut bytes)
        .expect("the OS random number generator");
    bytes
}

/// SHA-256 of `secret`, hex-encoded: the stored form.
pub fn hash_secret(secret: &str) -> String {
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, secret.as_bytes());
    hex_encode(digest.as_ref())
}

pub fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(out, "{b:02x}").unwrap();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_are_64_hex_chars_and_distinct() {
        let (a, b) = (generate_secret(), generate_secret());
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
    }

    #[test]
    fn hashing_is_deterministic_and_never_the_identity() {
        let s = generate_secret();
        assert_eq!(hash_secret(&s), hash_secret(&s));
        assert_ne!(hash_secret(&s), s);
        assert_ne!(hash_secret("a"), hash_secret("b"));
    }
}
