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

impl SlotId {
    /// The slot at `index` of a layout.
    pub fn at(index: usize) -> SlotId {
        SlotId::try_from(index).expect("the slots of one plan fit a u32")
    }
}
