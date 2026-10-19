//! Word-at-a-time bitmap primitives over `&[u64]`, 64 rows per iteration.
//!
//! **Invariant: bits past `len` are always 0.** Every function that writes a whole word re-masks the tail; a hot
//! trailing bit makes `popcount` over-count and shows up much later as a wrong null count.

pub const fn words_for(rows: usize) -> usize {
    rows.div_ceil(u64::BITS as usize)
}

/// Bytes needed to store `rows` bits, rounded up to whole 64-bit words.
pub const fn bytes_for(rows: usize) -> usize {
    words_for(rows) * size_of::<u64>()
}

/// Mask for the final partial word: bits [0, len % 64) set, or `!0` when `len` is a whole multiple of 64.
#[inline]
const fn tail_mask(len: usize) -> u64 {
    let r = len % 64;
    if r == 0 { !0u64 } else { (1u64 << r) - 1 }
}

/// Zero every bit at or past `len` in the final word. The single point where the trailing-zero invariant is restored.
#[inline]
pub fn mask_tail(bits: &mut [u64], len: usize) {
    let words = words_for(len);
    if words == 0 {
        return;
    }
    bits[words - 1] &= tail_mask(len);
}

pub fn fill_valid(bits: &mut [u64], len: usize) {
    let words = words_for(len);
    bits[..words].fill(!0u64);
    mask_tail(&mut bits[..words], len);
}

pub fn fill_invalid(bits: &mut [u64]) {
    bits.fill(0u64);
}

pub fn popcount(bits: &[u64]) -> usize {
    bits.iter().map(|w| w.count_ones() as usize).sum()
}

#[inline]
pub fn get_row(bits: &[u64], row: usize) -> bool {
    bits[row / 64] & (1u64 << (row % 64)) != 0
}

/// Returns true if the bit changed.
#[inline]
pub fn set_row(bits: &mut [u64], row: usize, value: bool) -> bool {
    let (w, m) = (row / 64, 1u64 << (row % 64));
    let was = bits[w] & m != 0;
    if value {
        bits[w] |= m
    } else {
        bits[w] &= !m
    }
    was != value
}

// -- binary ops: `dst` cannot alias `a` or `b` (borrow rules) ---------------

#[inline]
pub fn and_into(dst: &mut [u64], a: &[u64], b: &[u64], len: usize) {
    let w = words_for(len);
    for i in 0..w {
        dst[i] = a[i] & b[i];
    }
    mask_tail(&mut dst[..w], len);
}

#[inline]
pub fn or_into(dst: &mut [u64], a: &[u64], b: &[u64], len: usize) {
    let w = words_for(len);
    for i in 0..w {
        dst[i] = a[i] | b[i];
    }
    mask_tail(&mut dst[..w], len);
}

#[inline]
pub fn xor_into(dst: &mut [u64], a: &[u64], b: &[u64], len: usize) {
    let w = words_for(len);
    for i in 0..w {
        dst[i] = a[i] ^ b[i];
    }
    mask_tail(&mut dst[..w], len);
}

/// `dst = a & !b`: the Drop kernel, `live' = live & !dropped`.
#[inline]
pub fn and_not_into(dst: &mut [u64], a: &[u64], b: &[u64], len: usize) {
    let w = words_for(len);
    for i in 0..w {
        dst[i] = a[i] & !b[i];
    }
    mask_tail(&mut dst[..w], len);
}

/// `dst = !a`. NOT turns the zero tail hot, so it must re-mask.
#[inline]
pub fn not_into(dst: &mut [u64], a: &[u64], len: usize) {
    let w = words_for(len);
    for i in 0..w {
        dst[i] = !a[i];
    }
    mask_tail(&mut dst[..w], len);
}

// -- in-place forms ---------------------------------------------------------

#[inline]
pub fn and_assign(dst: &mut [u64], src: &[u64], len: usize) {
    let w = words_for(len);
    for i in 0..w {
        dst[i] &= src[i];
    }
}

#[inline]
pub fn or_assign(dst: &mut [u64], src: &[u64], len: usize) {
    let w = words_for(len);
    for i in 0..w {
        dst[i] |= src[i];
    }
    mask_tail(&mut dst[..w], len);
}

#[inline]
pub fn and_not_assign(dst: &mut [u64], src: &[u64], len: usize) {
    let w = words_for(len);
    for i in 0..w {
        dst[i] &= !src[i];
    }
}

/// True when every bit in [0, len) is set. Bits past `len` are ignored.
#[inline]
pub fn all_set(bits: &[u64], len: usize) -> bool {
    let w = words_for(len);
    if w == 0 {
        return true;
    }
    if bits[..w - 1].iter().any(|&x| x != !0u64) {
        return false;
    }
    let m = tail_mask(len);
    bits[w - 1] & m == m
}

#[inline]
pub fn any_set(bits: &[u64], len: usize) -> bool {
    bits[..words_for(len)].iter().any(|&w| w != 0)
}

/// Visit the indices of set bits, ascending. O(popcount), not O(len).
pub fn for_each_set<F: FnMut(usize)>(bits: &[u64], len: usize, mut f: F) {
    for (wi, &word) in bits[..words_for(len)].iter().enumerate() {
        let mut w = word;
        while w != 0 {
            let bit = w.trailing_zeros() as usize;
            f(wi * 64 + bit);
            w &= w - 1;
        }
    }
}

/// For each row, the index of the mask in `masks` whose bit it is, or `None`. The masks are disjoint. One pass over
/// the set bits, not one probe per row per mask.
pub fn index_of_set(masks: &[&[u64]], len: usize) -> Vec<Option<u32>> {
    let mut out = vec![None; len];
    for (k, m) in masks.iter().enumerate() {
        for_each_set(m, len, |i| out[i] = Some(k as u32));
    }
    out
}

/// Collect set-bit indices into a gather list: how a selection becomes the `indices` argument to `Batch::gather`.
pub fn to_indices(bits: &[u64], len: usize, out: &mut Vec<u32>) {
    out.clear();
    out.reserve(popcount(&bits[..words_for(len)]));
    for_each_set(bits, len, |i| out.push(i as u32));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn from_rows(len: usize, set: &[usize]) -> Vec<u64> {
        let mut v = vec![0u64; words_for(len)];
        for &r in set {
            set_row(&mut v, r, true);
        }
        v
    }

    #[test]
    fn words_and_bytes() {
        assert_eq!(words_for(0), 0);
        assert_eq!(words_for(1), 1);
        assert_eq!(words_for(64), 1);
        assert_eq!(words_for(65), 2);
        assert_eq!(bytes_for(65), 16);
    }

    #[test]
    fn fill_valid_masks_the_tail() {
        let mut v = vec![0u64; 2];
        fill_valid(&mut v, 65);
        assert_eq!(popcount(&v), 65);
        assert_eq!(v[1], 1);
    }

    #[test]
    fn fill_valid_whole_word_boundary() {
        let mut v = vec![0u64; 1];
        fill_valid(&mut v, 64);
        assert_eq!(v[0], !0u64);
        assert_eq!(popcount(&v), 64);
    }

    #[test]
    fn not_into_does_not_leak_trailing_bits() {
        let a = from_rows(4, &[0, 1, 2, 3]);
        let mut d = vec![0u64; 1];
        not_into(&mut d, &a, 4);
        assert_eq!(popcount(&d), 0);
    }

    #[test]
    fn and_not_is_the_drop_kernel() {
        let live = from_rows(8, &[0, 1, 2, 3, 4, 5, 6, 7]);
        let dropped = from_rows(8, &[1, 3]);
        let mut out = vec![0u64; 1];
        and_not_into(&mut out, &live, &dropped, 8);
        assert_eq!(popcount(&out), 6);
        assert!(!get_row(&out, 1));
        assert!(get_row(&out, 2));
    }

    #[test]
    fn boolean_ops_across_word_boundary() {
        let a = from_rows(130, &[0, 63, 64, 129]);
        let b = from_rows(130, &[63, 64, 100]);
        let mut d = vec![0u64; 3];

        and_into(&mut d, &a, &b, 130);
        assert_eq!(popcount(&d), 2); // 63, 64

        or_into(&mut d, &a, &b, 130);
        assert_eq!(popcount(&d), 5); // 0,63,64,100,129

        xor_into(&mut d, &a, &b, 130);
        assert_eq!(popcount(&d), 3); // 0,100,129
    }

    #[test]
    fn assign_forms_match_into_forms() {
        let a = from_rows(70, &[1, 65]);
        let b = from_rows(70, &[65, 69]);
        let mut into = vec![0u64; 2];
        and_into(&mut into, &a, &b, 70);
        let mut assign = a.clone();
        and_assign(&mut assign, &b, 70);
        assert_eq!(into, assign);
    }

    #[test]
    fn all_set_respects_len() {
        let mut v = vec![0u64; 2];
        fill_valid(&mut v, 70);
        assert!(all_set(&v, 70));
        set_row(&mut v, 69, false);
        assert!(!all_set(&v, 70));
    }

    #[test]
    fn all_set_ignores_bits_past_len() {
        let v = vec![!0u64];
        assert!(all_set(&v, 4));
    }

    #[test]
    fn all_set_on_empty() {
        assert!(all_set(&[], 0));
    }

    #[test]
    fn any_set_basic() {
        assert!(!any_set(&[0u64, 0u64], 128));
        assert!(any_set(&from_rows(128, &[127]), 128));
    }

    #[test]
    fn for_each_set_visits_ascending() {
        let v = from_rows(200, &[0, 5, 64, 130, 199]);
        let mut seen = Vec::new();
        for_each_set(&v, 200, |i| seen.push(i));
        assert_eq!(seen, vec![0, 5, 64, 130, 199]);
    }

    #[test]
    fn to_indices_builds_a_gather_list() {
        let v = from_rows(100, &[3, 7, 99]);
        let mut out = vec![9999u32]; // pre-dirtied; must be cleared
        to_indices(&v, 100, &mut out);
        assert_eq!(out, vec![3, 7, 99]);
    }

    #[test]
    fn for_each_set_on_full_word() {
        let v = vec![!0u64];
        let mut n = 0;
        for_each_set(&v, 64, |_| n += 1);
        assert_eq!(n, 64);
    }

    #[test]
    fn zero_len_is_a_noop_everywhere() {
        let mut d: Vec<u64> = vec![];
        not_into(&mut d, &[], 0);
        and_into(&mut d, &[], &[], 0);
        mask_tail(&mut d, 0);
        assert_eq!(popcount(&d), 0);
    }
}
