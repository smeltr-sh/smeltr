use std::marker::PhantomData;
use std::ops::{Deref, Index, IndexMut};

/// What numbers the entries of a [`ById`].
pub trait Id: Copy {
    /// Its place in a list that holds one entry per id.
    fn index(self) -> usize;

    /// The id of the entry at `index`.
    fn at(index: usize) -> Self;
}

/// A list with one entry per id of one kind. Only that kind of id indexes it, so a row number, a count or another
/// kind of id cannot stand in for one. `as_slice` reads it as a slice of every entry in id order, the one way to
/// index by position.
#[derive(Clone, PartialEq, Eq)]
pub struct ById<I, T> {
    entries: Vec<T>,
    read_by: PhantomData<fn(I)>,
}

impl<I: Id, T> ById<I, T> {
    pub const fn new() -> Self {
        ById {
            entries: Vec::new(),
            read_by: PhantomData,
        }
    }

    /// Adds `entry` and gives the id that reads it.
    pub fn push(&mut self, entry: T) -> I {
        self.entries.push(entry);
        I::at(self.entries.len() - 1)
    }

    /// The id the next entry pushed will have.
    pub fn next_id(&self) -> I {
        I::at(self.entries.len())
    }

    pub fn get(&self, id: I) -> Option<&T> {
        self.entries.get(id.index())
    }

    pub fn as_slice(&self) -> &[T] {
        &self.entries
    }

    /// The entries from the one `id` reads, on, in order.
    pub fn from_id(&self, id: I) -> &[T] {
        &self.entries[id.index()..]
    }

    /// Every id that reads an entry, in order.
    pub fn ids(&self) -> impl DoubleEndedIterator<Item = I> + ExactSizeIterator + use<I, T> {
        (0..self.entries.len()).map(I::at)
    }

    /// Each entry with the id that reads it, in order.
    pub fn entries(&self) -> impl DoubleEndedIterator<Item = (I, &T)> + ExactSizeIterator {
        self.entries
            .iter()
            .enumerate()
            .map(|(index, entry)| (I::at(index), entry))
    }
}

impl<I, T> IntoIterator for ById<I, T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl<'a, I, T> IntoIterator for &'a ById<I, T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

impl<I: Id, T> Default for ById<I, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I, T> From<Vec<T>> for ById<I, T> {
    fn from(entries: Vec<T>) -> Self {
        ById {
            entries,
            read_by: PhantomData,
        }
    }
}

impl<I, T> FromIterator<T> for ById<I, T> {
    fn from_iter<A: IntoIterator<Item = T>>(entries: A) -> Self {
        Self::from(entries.into_iter().collect::<Vec<T>>())
    }
}

impl<I, T> Deref for ById<I, T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        &self.entries
    }
}

impl<I: Id, T> Index<I> for ById<I, T> {
    type Output = T;

    fn index(&self, id: I) -> &T {
        &self.entries[id.index()]
    }
}

impl<I: Id, T> IndexMut<I> for ById<I, T> {
    fn index_mut(&mut self, id: I) -> &mut T {
        &mut self.entries[id.index()]
    }
}

impl<I, T: std::fmt::Debug> std::fmt::Debug for ById<I, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.entries.fmt(f)
    }
}

/// Makes `$name` a number of its own kind, so one kind of id cannot be passed for another.
macro_rules! id {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
        #[derive(serde::Serialize, serde::Deserialize)]
        #[serde(transparent)]
        pub struct $name(u32);

        impl $name {
            pub const fn new(n: u32) -> Self {
                Self(n)
            }
        }

        impl Id for $name {
            fn index(self) -> usize {
                self.0 as usize
            }

            fn at(index: usize) -> Self {
                Self::try_from(index).expect("the entries of a list read by id fit a u32")
            }
        }

        impl TryFrom<usize> for $name {
            type Error = std::num::TryFromIntError;

            fn try_from(n: usize) -> Result<Self, Self::Error> {
                u32::try_from(n).map(Self)
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

id!(
    /// A destination, by its number among those the loaded configuration holds.
    DestId
);
id!(
    /// A source, by its number among those the loaded configuration holds.
    SourceId
);
id!(
    /// A step of a pipeline, by its number among that pipeline's steps.
    FunctionId
);
id!(
    /// A regex literal, by its number among those a plan holds.
    PatternId
);
id!(
    /// A slot: a column of a batch, by its number in the plan's layout.
    SlotId
);

/// A list with one entry per slot.
pub type Slots<T> = ById<SlotId, T>;
