//! Text decoding at ingest.
//!
//! `_raw` is a `Str`, so a record's bytes are decoded exactly once — here — and every downstream string function works
//! on valid UTF-8 without revalidating. A source declares its `encoding`; the default, `auto`, detects per record,
//! and `none` decodes nothing, so the source's record has no `_raw`.
//!
//! Detection is structural, never statistical: a BOM, UTF-8 validity, or NUL bytes at one index parity. Frequency
//! analysis would make the same bytes decode differently depending on which records shared a batch, and a log stream
//! is not a document — a short record carries too little evidence to guess from.

use std::borrow::Cow;

/// One of encoding_rs' static encodings.
pub type Charset = &'static encoding_rs::Encoding;

/// What `auto` falls back to when the bytes are neither UTF-8 nor UTF-16. Every byte maps to a character, so the
/// fallback never fails — it can only be wrong, which is why it is marked.
pub const FALLBACK: Charset = encoding_rs::WINDOWS_1252;

/// A source's declared encoding.
#[derive(Copy, Clone, PartialEq, Eq, Debug, Default)]
pub enum Encoding {
    /// Detect per record.
    #[default]
    Auto,
    /// Declared. No detection: bytes that do not decode become U+FFFD.
    Fixed(Charset),
    /// The bytes are not text. The source's record has no `_raw`; its payload is `__source.bytes` alone.
    None,
}

impl Encoding {
    /// `"auto"`, `"none"` or a WHATWG label (`"utf-8"`, `"utf-16le"`, `"shift_jis"`, `"latin1"`, …). `None` for a name
    /// encoding_rs does not know, so a config file can be rejected at load with the name in the message.
    ///
    /// `_no_replacement` is the load-bearing half: WHATWG maps a handful of labels it will not implement
    /// (`hz-gb-2312`, the ISO-2022 variants) to the `replacement` encoding, which decodes *any* input to one U+FFFD.
    /// Accepting one would load clean and then destroy every record.
    pub fn for_label(label: &str) -> Option<Encoding> {
        if label.eq_ignore_ascii_case("auto") {
            return Some(Encoding::Auto);
        }
        if label.eq_ignore_ascii_case("none") {
            return Some(Encoding::None);
        }
        encoding_rs::Encoding::for_label_no_replacement(label.as_bytes()).map(Encoding::Fixed)
    }

    /// The canonical name, which is what a saved config file holds. Not always the label that was written: WHATWG
    /// folds aliases, so `"latin1"` saves back as `"windows-1252"`.
    pub fn label(&self) -> &'static str {
        match self {
            Encoding::Auto => "auto",
            Encoding::Fixed(cs) => cs.name(),
            Encoding::None => "none",
        }
    }

    /// Decode one record. The `Cow` borrows for the common case: UTF-8 input, valid. `None` reads no text, so its
    /// record decodes to nothing.
    pub fn decode<'b>(&self, bytes: &'b [u8]) -> Decoded<'b> {
        match self {
            Encoding::None => Decoded::borrowed(""),
            Encoding::Auto => decode_auto(bytes),
            Encoding::Fixed(cs) => {
                let (text, replaced) = cs.decode_with_bom_removal(bytes);
                Decoded {
                    text,
                    replaced,
                    guessed: None,
                }
            }
        }
    }
}

/// A decoded record, and what the decoder had to do to get it. Both flags become exceptions, which mark the row.
pub struct Decoded<'b> {
    pub text: Cow<'b, str>,
    /// Bytes that did not decode were replaced with U+FFFD.
    pub replaced: bool,
    /// `auto` found no BOM, invalid UTF-8, and no UTF-16 pattern, and fell back to this encoding.
    pub guessed: Option<Charset>,
}

impl<'b> Decoded<'b> {
    /// The common case: valid UTF-8, borrowed, nothing to report.
    fn borrowed(text: &'b str) -> Decoded<'b> {
        Decoded {
            text: Cow::Borrowed(text),
            replaced: false,
            guessed: None,
        }
    }
}

fn decode_auto(bytes: &[u8]) -> Decoded<'_> {
    // A BOM outranks content: UTF-8 with a BOM is valid UTF-8, but the BOM is not part of the record.
    if let Some((cs, bom_len)) = encoding_rs::Encoding::for_bom(bytes) {
        let (text, replaced) = cs.decode_without_bom_handling(&bytes[bom_len..]);
        return Decoded {
            text,
            replaced,
            guessed: None,
        };
    }
    // ASCII text in UTF-16 is also valid UTF-8 -- every other byte is NUL -- so validity alone does not settle it. A
    // NUL does not occur in text, so a record without one that validates is UTF-8, decided.
    let utf8 = std::str::from_utf8(bytes);
    if let Ok(text) = utf8
        && !bytes.contains(&0)
    {
        return Decoded::borrowed(text);
    }
    if let Some(cs) = utf16_by_parity(bytes) {
        let (text, replaced) = cs.decode_without_bom_handling(bytes);
        return Decoded {
            text,
            replaced,
            guessed: None,
        };
    }
    // NULs, but in no UTF-16 pattern. Valid UTF-8 is still the better reading than a fallback.
    if let Ok(text) = utf8 {
        return Decoded::borrowed(text);
    }
    let (text, replaced) = FALLBACK.decode_without_bom_handling(bytes);
    Decoded {
        text,
        replaced,
        guessed: Some(FALLBACK),
    }
}

/// UTF-16 without a BOM, by where the NUL bytes sit.
///
/// Text has no NUL, so a NUL is a code unit's zero half; all of them at even indices means that half comes first,
/// which is big-endian.
fn utf16_by_parity(bytes: &[u8]) -> Option<Charset> {
    if bytes.len() < 2 || !bytes.len().is_multiple_of(2) {
        return None;
    }
    let mut even = false;
    let mut odd = false;
    for (i, b) in bytes.iter().enumerate() {
        if *b == 0 {
            if i % 2 == 0 {
                even = true;
            } else {
                odd = true;
            }
        }
    }
    match (even, odd) {
        (true, false) => Some(encoding_rs::UTF_16BE),
        (false, true) => Some(encoding_rs::UTF_16LE),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utf16le(s: &str) -> Vec<u8> {
        s.encode_utf16().flat_map(|u| u.to_le_bytes()).collect()
    }

    fn utf16be(s: &str) -> Vec<u8> {
        s.encode_utf16().flat_map(|u| u.to_be_bytes()).collect()
    }

    fn auto(bytes: &[u8]) -> Decoded<'_> {
        Encoding::Auto.decode(bytes)
    }

    #[test]
    fn utf8_is_borrowed_and_silent() {
        let d = auto(b"level=info msg=\"ready\"");
        assert!(matches!(d.text, Cow::Borrowed(_)));
        assert_eq!(d.text, "level=info msg=\"ready\"");
        assert!(!d.replaced && d.guessed.is_none());
    }

    #[test]
    fn multibyte_utf8_needs_no_guess() {
        let d = auto("日本語 ok".as_bytes());
        assert_eq!(d.text, "日本語 ok");
        assert!(!d.replaced && d.guessed.is_none());
    }

    #[test]
    fn boms_are_stripped() {
        let mut le = vec![0xFF, 0xFE];
        le.extend(utf16le("hello"));
        assert_eq!(auto(&le).text, "hello");

        let mut be = vec![0xFE, 0xFF];
        be.extend(utf16be("hello"));
        assert_eq!(auto(&be).text, "hello");

        let mut u8bom = vec![0xEF, 0xBB, 0xBF];
        u8bom.extend_from_slice(b"hello");
        let d = auto(&u8bom);
        assert_eq!(d.text, "hello");
        assert!(d.guessed.is_none());
    }

    #[test]
    fn utf16_without_bom_by_parity() {
        let le = utf16le("kernel: eth0 up");
        let d = auto(&le);
        assert_eq!(d.text, "kernel: eth0 up");
        assert!(d.guessed.is_none());

        let be = utf16be("kernel: eth0 up");
        let d = auto(&be);
        assert_eq!(d.text, "kernel: eth0 up");
        assert!(d.guessed.is_none());
    }

    #[test]
    fn mixed_script_utf16_still_detected() {
        // The CJK units have no zero half; the ASCII ones do, all at one parity.
        let bytes = utf16le("msg=日本語 done");
        let d = auto(&bytes);
        assert_eq!(d.text, "msg=日本語 done");
    }

    #[test]
    fn nul_bearing_utf8_is_kept_as_utf8() {
        // Valid UTF-8 with NULs that form no UTF-16 pattern: no fallback, no replacement.
        let d = auto(b"a\x00\x00b");
        assert_eq!(d.text, "a\0\0b");
        assert!(!d.replaced && d.guessed.is_none());
    }

    #[test]
    fn latin1_falls_back_and_is_marked() {
        // 0xE9 alone is not valid UTF-8; in windows-1252 it is 'é'.
        let d = auto(b"caf\xE9 ferm\xE9");
        assert_eq!(d.text, "café fermé");
        assert_eq!(d.guessed, Some(FALLBACK));
        assert!(!d.replaced);
    }

    #[test]
    fn nuls_at_both_parities_are_not_utf16() {
        let d = auto(&[0x00, 0x00, 0xE9, 0x41]);
        assert_eq!(d.guessed, Some(FALLBACK));
    }

    #[test]
    fn odd_length_is_not_utf16() {
        let d = auto(&[0x41, 0x00, 0xE9]);
        assert_eq!(d.guessed, Some(FALLBACK));
    }

    #[test]
    fn empty_input_decodes_to_empty() {
        let d = auto(b"");
        assert_eq!(d.text, "");
        assert!(!d.replaced && d.guessed.is_none());
    }

    #[test]
    fn declared_encoding_skips_detection() {
        // Valid UTF-8 that means something else in windows-1252.
        let latin = Encoding::for_label("latin1").unwrap();
        let d = latin.decode("é".as_bytes());
        assert_eq!(d.text, "Ã©");
        assert!(d.guessed.is_none());

        let le = Encoding::for_label("utf-16le").unwrap();
        let bytes = utf16le("hi");
        assert_eq!(le.decode(&bytes).text, "hi");
    }

    #[test]
    fn declared_utf8_replaces_and_marks() {
        let u8enc = Encoding::for_label("utf-8").unwrap();
        let d = u8enc.decode(b"caf\xE9");
        assert_eq!(d.text, "caf\u{FFFD}");
        assert!(d.replaced);
        assert!(d.guessed.is_none());
    }

    #[test]
    fn declared_encoding_removes_its_own_bom() {
        let le = Encoding::for_label("utf-16le").unwrap();
        let mut bytes = vec![0xFF, 0xFE];
        bytes.extend(utf16le("hi"));
        assert_eq!(le.decode(&bytes).text, "hi");
    }

    /// WHATWG maps a few labels it will not implement to the `replacement` encoding, which decodes any input to one
    /// U+FFFD. Accepting one would load clean and then destroy every record, so they are refused as names.
    #[test]
    fn a_replacement_encoding_label_is_refused() {
        for label in ["hz-gb-2312", "iso-2022-cn", "iso-2022-kr", "csiso2022kr", "replacement"] {
            assert_eq!(Encoding::for_label(label), None, "{label}");
        }
        // Not a blanket ban on ISO-2022: the Japanese one is implemented.
        assert_eq!(Encoding::for_label("iso-2022-jp").unwrap().label(), "ISO-2022-JP");
    }

    #[test]
    fn labels_are_whatwg_and_canonical() {
        assert_eq!(Encoding::for_label("auto"), Some(Encoding::Auto));
        assert_eq!(Encoding::Auto.label(), "auto");
        assert_eq!(Encoding::for_label("NONE"), Some(Encoding::None));
        assert_eq!(Encoding::None.label(), "none");
        assert_eq!(Encoding::for_label("latin1").unwrap().label(), "windows-1252");
        assert_eq!(Encoding::for_label("UTF8").unwrap().label(), "UTF-8");
        assert_eq!(Encoding::for_label("shift_jis").unwrap().label(), "Shift_JIS");
        assert_eq!(Encoding::for_label("klingon"), None);
    }
}
