use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

/// Past this many entries a key is found by a map of them; up to it a scan, which costs less than the map.
const SCAN: usize = 32;

/// An object's keys and their values, in the order they arrived. A record's keys are data, so every place that finds one
/// key among an object's keys finds it here, and a row costs time linear in its number of keys. A key taken out leaves
/// a hole where it was, so the others keep their positions.
pub(crate) struct Keys<K, V> {
    entries: Vec<Option<(K, V)>>,
    /// Each held key's position, once there are more than `SCAN` entries.
    index: HashMap<K, usize>,
    live: usize,
}

impl<K, V> Default for Keys<K, V> {
    fn default() -> Self {
        Keys {
            entries: Vec::new(),
            index: HashMap::new(),
            live: 0,
        }
    }
}

impl<K: Borrow<str> + Hash + Eq + Clone, V> Keys<K, V> {
    /// The position of `key`.
    #[inline(always)]
    pub(crate) fn find(&self, key: &str) -> Option<usize> {
        if self.entries.len() > SCAN {
            return self.index.get(key).copied();
        }
        self.entries
            .iter()
            .position(|e| e.as_ref().is_some_and(|(k, _)| k.borrow() == key))
    }

    pub(crate) fn get(&self, key: &str) -> Option<&V> {
        self.find(key).and_then(|i| self.at(i)).map(|(_, v)| v)
    }

    /// The entry at `i`, `None` at a hole.
    pub(crate) fn at(&self, i: usize) -> Option<&(K, V)> {
        self.entries[i].as_ref()
    }

    pub(crate) fn at_mut(&mut self, i: usize) -> Option<&mut (K, V)> {
        self.entries[i].as_mut()
    }

    /// Append `key`, which is not held, with `value`. Its position.
    #[inline(always)]
    pub(crate) fn push(&mut self, key: K, value: V) -> usize {
        let at = self.entries.len();
        if at >= SCAN {
            self.index_with(&key, at);
        }
        self.entries.push(Some((key, value)));
        self.live += 1;
        at
    }

    /// Put `key` at `at` in the index, made from the held keys at the first key past `SCAN`.
    #[cold]
    fn index_with(&mut self, key: &K, at: usize) {
        if at == SCAN {
            let held = self
                .entries
                .iter()
                .enumerate()
                .filter_map(|(i, e)| Some((e.as_ref()?.0.clone(), i)));
            self.index.extend(held);
        }
        self.index.insert(key.clone(), at);
    }

    /// Push `key` with `value` where it is not held: its new position, else the position that holds it.
    pub(crate) fn insert(&mut self, key: K, value: V) -> Result<usize, usize> {
        match self.find(key.borrow()) {
            Some(i) => Err(i),
            None => Ok(self.push(key, value)),
        }
    }

    /// Take out the entry at `i`, leaving a hole.
    pub(crate) fn take(&mut self, i: usize) -> Option<(K, V)> {
        let taken = self.entries[i].take()?;
        self.index.remove(taken.0.borrow());
        self.live -= 1;
        Some(taken)
    }

    #[inline(always)]
    pub(crate) fn clear(&mut self) {
        if self.entries.len() > SCAN {
            self.index.clear();
        }
        self.entries.clear();
        self.live = 0;
    }

    /// How many keys are held.
    #[inline(always)]
    pub(crate) fn len(&self) -> usize {
        self.live
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &(K, V)> {
        self.entries.iter().flatten()
    }
}

/// A key that comes again keeps its first value.
impl<K: Borrow<str> + Hash + Eq + Clone, V> FromIterator<(K, V)> for Keys<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(entries: I) -> Self {
        let mut keys = Keys::default();
        entries.into_iter().for_each(|(k, v)| _ = keys.insert(k, v));
        keys
    }
}

impl<K, V> IntoIterator for Keys<K, V> {
    type Item = (K, V);
    type IntoIter = std::iter::Flatten<std::vec::IntoIter<Option<(K, V)>>>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter().flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_is_found_where_it_was_pushed_on_either_side_of_the_switch() {
        for n in [3, SCAN, SCAN + 1, 3 * SCAN] {
            let mut keys = Keys::default();
            for i in 0..n {
                assert_eq!(keys.push(i.to_string(), i), i);
            }
            for i in 0..n {
                assert_eq!(keys.find(&i.to_string()), Some(i), "{n} keys");
            }
            assert_eq!(keys.find("x"), None);
            assert_eq!(keys.len(), n);
        }
    }

    #[test]
    fn a_taken_key_leaves_a_hole_and_comes_back_at_the_end() {
        for n in [3, SCAN + 1, 3 * SCAN] {
            let mut keys = Keys::default();
            for i in 0..n {
                keys.push(i.to_string(), i);
            }
            assert_eq!(keys.take(1), Some(("1".to_string(), 1)));
            assert_eq!(keys.find("1"), None);
            assert_eq!(keys.find("2"), Some(2));
            assert_eq!(keys.push("1".to_string(), 10), n);
            assert_eq!(keys.find("1"), Some(n));
            assert_eq!(keys.len(), n);
            let order: Vec<usize> = keys.into_iter().map(|(_, v)| v).collect();
            assert_eq!(order[..2], [0, 2]);
            assert_eq!(order.last(), Some(&10));
        }
    }

    #[test]
    fn a_cleared_index_finds_nothing_and_fills_again() {
        let mut keys = Keys::default();
        for i in 0..2 * SCAN {
            keys.push(i.to_string(), ());
        }
        keys.clear();
        assert_eq!(keys.len(), 0);
        assert_eq!(keys.find("40"), None);
        keys.push("40".to_string(), ());
        assert_eq!(keys.find("40"), Some(0));
    }
}
