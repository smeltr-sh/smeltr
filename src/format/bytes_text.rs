//! Bytes as text. Bytes carry no meaning of their own, so writing them as text picks a form that spells every byte:
//! base64 wherever nothing names one (the JSON writer, `toString`, a field's text), hex or binary where a call does.
//! Reading text back as bytes takes the same forms. Reading bytes *as* text in some charset is `format::encoding`.

use base64::Engine as _;

/// How bytes are spelled as text.
#[derive(Copy, Clone, PartialEq, Eq, Debug, Default)]
pub enum BytesForm {
    /// Standard base64, padded when written; read padded or not.
    #[default]
    Base64,
    /// Two lowercase hex digits per byte; read in either case.
    Hex,
    /// Eight `0`s and `1`s per byte, high bit first.
    Binary,
}

/// The engine base64 is read with: the standard alphabet, padded or not.
const BASE64_READ: base64::engine::GeneralPurpose = base64::engine::GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    base64::engine::GeneralPurposeConfig::new()
        .with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent),
);

impl BytesForm {
    /// `"base64"`, `"hex"` or `"binary"`, in any case. `None` for any other name.
    pub fn for_label(label: &[u8]) -> Option<BytesForm> {
        [
            ("base64", BytesForm::Base64),
            ("hex", BytesForm::Hex),
            ("binary", BytesForm::Binary),
        ]
        .into_iter()
        .find(|(name, _)| label.eq_ignore_ascii_case(name.as_bytes()))
        .map(|(_, form)| form)
    }

    /// How many bytes of text `n` bytes take in this form.
    pub fn text_len(self, n: usize) -> usize {
        match self {
            BytesForm::Base64 => base64::encoded_len(n, true).expect("a value in memory has a base64 length that fits"),
            BytesForm::Hex => 2 * n,
            BytesForm::Binary => 8 * n,
        }
    }

    /// Append `bytes` in this form.
    pub fn write(self, bytes: &[u8], out: &mut Vec<u8>) {
        let start = out.len();
        out.resize(start + self.text_len(bytes.len()), 0);
        self.write_to(bytes, &mut out[start..]);
    }

    /// Write `bytes` in this form over `out`, which is [`text_len`](Self::text_len) long.
    pub fn write_to(self, bytes: &[u8], out: &mut [u8]) {
        debug_assert_eq!(out.len(), self.text_len(bytes.len()));
        match self {
            BytesForm::Base64 => {
                base64::engine::general_purpose::STANDARD
                    .encode_slice(bytes, out)
                    .expect("sized by text_len");
            }
            BytesForm::Hex => {
                const DIGITS: &[u8; 16] = b"0123456789abcdef";
                for (b, pair) in bytes.iter().zip(out.as_chunks_mut::<2>().0) {
                    *pair = [DIGITS[usize::from(b >> 4)], DIGITS[usize::from(b & 0xf)]];
                }
            }
            BytesForm::Binary => {
                for (b, bits) in bytes.iter().zip(out.as_chunks_mut::<8>().0) {
                    for (bit, c) in (0..8).rev().zip(bits) {
                        *c = if b >> bit & 1 == 1 { b'1' } else { b'0' };
                    }
                }
            }
        }
    }

    /// `bytes` in this form, as a `String`.
    pub fn text(self, bytes: &[u8]) -> String {
        let mut out = Vec::new();
        self.write(bytes, &mut out);
        // Every form's alphabet is ASCII.
        String::from_utf8(out).expect("ASCII")
    }

    /// Append the bytes `text` spells in this form. False, with `out` as it was, where `text` is not this form.
    pub fn read(self, text: &[u8], out: &mut Vec<u8>) -> bool {
        let start = out.len();
        let ok = match self {
            BytesForm::Base64 => BASE64_READ.decode_vec(text, out).is_ok(),
            BytesForm::Hex => {
                let (pairs, rest) = text.as_chunks::<2>();
                rest.is_empty()
                    && pairs.iter().all(|&[hi, lo]| {
                        let digit = |c: u8| char::from(c).to_digit(16);
                        match (digit(hi), digit(lo)) {
                            (Some(hi), Some(lo)) => {
                                out.push((hi << 4 | lo) as u8);
                                true
                            }
                            _ => false,
                        }
                    })
            }
            BytesForm::Binary => {
                let (bytes, rest) = text.as_chunks::<8>();
                rest.is_empty()
                    && bytes.iter().all(|bits| {
                        let mut byte = 0u8;
                        for &c in bits {
                            match c {
                                b'0' => byte <<= 1,
                                b'1' => byte = byte << 1 | 1,
                                _ => return false,
                            }
                        }
                        out.push(byte);
                        true
                    })
            }
        };
        if !ok {
            out.truncate(start);
        }
        ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FORMS: [BytesForm; 3] = [BytesForm::Base64, BytesForm::Hex, BytesForm::Binary];

    #[test]
    fn every_form_spells_every_byte_and_reads_it_back() {
        let bytes: Vec<u8> = (0..=255).collect();
        for form in FORMS {
            let text = form.text(&bytes);
            let mut back = Vec::new();
            assert!(form.read(text.as_bytes(), &mut back), "{form:?}");
            assert_eq!(back, bytes, "{form:?}");
        }
    }

    #[test]
    fn each_form_writes_as_documented() {
        assert_eq!(BytesForm::Base64.text(b"caf\xe9"), "Y2Fm6Q==");
        assert_eq!(BytesForm::Hex.text(b"caf\xe9"), "636166e9");
        assert_eq!(BytesForm::Binary.text(b"\x81"), "10000001");
    }

    #[test]
    fn text_not_in_the_form_reads_as_nothing() {
        let mut out = b"kept".to_vec();
        for (form, text) in [
            (BytesForm::Base64, &b"not base64!"[..]),
            (BytesForm::Hex, b"abc"),
            (BytesForm::Hex, b"zz"),
            (BytesForm::Binary, b"0101"),
            (BytesForm::Binary, b"01010102"),
        ] {
            assert!(!form.read(text, &mut out), "{form:?} {text:?}");
            assert_eq!(out, b"kept");
        }
    }

    #[test]
    fn base64_reads_unpadded_and_hex_either_case() {
        let mut out = Vec::new();
        assert!(BytesForm::Base64.read(b"Y2Fm6Q", &mut out));
        assert_eq!(out, b"caf\xe9");
        out.clear();
        assert!(BytesForm::Hex.read(b"E9e9", &mut out));
        assert_eq!(out, b"\xe9\xe9");
    }

    #[test]
    fn a_form_is_named_in_any_case() {
        assert_eq!(BytesForm::for_label(b"HEX"), Some(BytesForm::Hex));
        assert_eq!(BytesForm::for_label(b"base32"), None);
    }
}
