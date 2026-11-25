pub type SlotId = usize;
pub type PatternId = usize;
pub type FunctionId = u32;

/// Makes `$name` a number of its own kind, so one kind of id cannot be passed for another.
macro_rules! id {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
        #[derive(serde::Serialize, serde::Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub u32);

        impl $name {
            pub const MAX: Self = Self(u32::MAX);

            /// Its place in a list that holds one entry per id.
            pub fn index(self) -> usize {
                self.0 as usize
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
