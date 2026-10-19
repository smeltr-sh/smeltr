//! The types a column holds: the storage types, the compile-time `DataType`, and how types meet.

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Fixed-width, 8-byte-backed types. All the same width, so they share one pool bucket and can reuse each other's
/// buffers.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum FixedType {
    /// No U64: an integer outside i64 fails its row rather than adding a second integer width -- narrow in practice,
    /// since the large integers in log data are string ids.
    I64,
    F64,
    /// Microseconds since the Unix epoch.
    DateTime,
    /// Microseconds, so `DateTime - DateTime` is a plain subtraction.
    Duration,
}

impl FixedType {
    pub const fn size_of(&self) -> usize {
        8
    }
}

///What kind of bytes a variable-length column holds
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum VarType {
    /// Valid UTF-8, validated where Bytes becomes Str.
    Str,
    /// Arbitrary bytes. `_raw` before parsing.
    Bytes,
    /// One variant per row, as `core::variant` encodes it.
    Variant,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum DataType {
    /// The type of a value with nothing in it: `null`, the elements of `[]`, what no row fills. It belongs to every
    /// type, so it meets any type as that type (`DataType::unify`). No declaration names it, and a column carries
    /// it only as `Empty`.
    Null,
    Bool,
    I64,
    F64,
    DateTime,
    Duration,
    Str,
    Bytes,
    Array(Box<DataType>),
    /// An object: its named fields, and whether it is open, `{a: i64, ...}`: it may have unclaimed fields, and a read
    /// reaches them, each value a variant. A closed object has none.
    Struct(Vec<(String, DataType)>, bool),
    /// A value whose shape only the row knows: the language's variant. Its column holds one encoded value per row
    /// (`core::variant`).
    Variant,
}

impl std::fmt::Display for DataType {
    /// How a type reads in prose: lowercase, the spelling the pipeline editor's declared-type list uses. A
    /// `Struct` reads as `struct`, since its fields are the shape rather than the name.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Null => f.write_str("null"),
            DataType::Bool => f.write_str("bool"),
            DataType::I64 => f.write_str("i64"),
            DataType::F64 => f.write_str("f64"),
            DataType::DateTime => f.write_str("datetime"),
            DataType::Duration => f.write_str("duration"),
            DataType::Str => f.write_str("str"),
            DataType::Bytes => f.write_str("bytes"),
            DataType::Array(inner) => write!(f, "array({inner})"),
            DataType::Struct(..) => f.write_str("struct"),
            DataType::Variant => f.write_str("variant"),
        }
    }
}

impl DataType {
    pub fn is_numeric(&self) -> bool {
        matches!(self, DataType::I64 | DataType::F64)
    }

    /// Where two values of these types meet: equal types unify to themselves, `null` takes the other's type, and
    /// an `I64` beside an `F64` widens to `F64`, inside an array's element and a struct's field too. Two structs
    /// unify field by field, and a field one side lacks is null there. They meet open where either is open, since the
    /// value may then have unclaimed fields. `Str` and `Bytes` do not unify, since `Bytes` carries no UTF-8 guarantee. A variant meets
    /// anything as a variant: it holds any value as it is.
    pub fn unify(&self, other: &DataType) -> Option<DataType> {
        if self == other {
            return Some(self.clone());
        }
        match (self, other) {
            (DataType::Null, t) | (t, DataType::Null) => Some(t.clone()),
            (DataType::Variant, _) | (_, DataType::Variant) => Some(DataType::Variant),
            (DataType::Array(x), DataType::Array(y)) => x.unify(y).map(|e| DataType::Array(Box::new(e))),
            (DataType::Struct(fa, oa), DataType::Struct(fb, ob)) => {
                let mut fields: Vec<(String, DataType)> = Vec::with_capacity(fa.len() + fb.len());
                for (name, x) in fa {
                    let t = match fb.iter().find(|(n, _)| n == name) {
                        Some((_, y)) => x.unify(y)?,
                        None => x.clone(),
                    };
                    fields.push((name.clone(), t));
                }
                for (name, y) in fb {
                    if !fa.iter().any(|(n, _)| n == name) {
                        fields.push((name.clone(), y.clone()));
                    }
                }
                Some(DataType::Struct(fields, *oa || *ob))
            }
            _ if self.is_numeric() && other.is_numeric() => Some(DataType::F64),
            _ => None,
        }
    }

    /// The type as a declaration spells it: `array<i64>`, `{ip: str, port: i64}`, `{ip: str, ...}`. For an
    /// error about a type, where the shape is the point; `Display` names only the kind.
    pub fn spell(&self) -> String {
        match self {
            DataType::Array(e) => format!("array<{}>", e.spell()),
            DataType::Struct(fields, open) => {
                // A key that is not an identifier is a string, as a type literal reads it.
                let ident = |n: &str| {
                    n.bytes().next().is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
                        && n.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                };
                let key = |n: &str| match ident(n) {
                    true => n.to_string(),
                    false => crate::plan::tree::spell_quoted(n),
                };
                let mut parts: Vec<String> = fields
                    .iter()
                    .map(|(n, t)| format!("{}: {}", key(n), t.spell()))
                    .collect();
                if *open {
                    parts.push("...".to_string());
                }
                format!("{{{}}}", parts.join(", "))
            }
            other => other.to_string(),
        }
    }

    /// Two types share a pool bucket when their storage shape and per-row width match. This, not type equality, is what
    /// makes buffer reuse legal: an I64 buffer can back an F64 output, a Str buffer cannot.
    pub fn same_storage_shape(&self, other: &DataType) -> bool {
        use DataType::*;
        matches!(
            (self, other),
            (Bool, Bool)
                | (I64 | F64 | DateTime | Duration, I64 | F64 | DateTime | Duration)
                | (Str | Bytes | Variant, Str | Bytes | Variant)
                | (Array(_), Array(_))
        )
    }
}

impl From<FixedType> for DataType {
    fn from(t: FixedType) -> Self {
        match t {
            FixedType::I64 => DataType::I64,
            FixedType::F64 => DataType::F64,
            FixedType::DateTime => DataType::DateTime,
            FixedType::Duration => DataType::Duration,
        }
    }
}

impl From<VarType> for DataType {
    fn from(t: VarType) -> Self {
        match t {
            VarType::Bytes => DataType::Bytes,
            VarType::Str => DataType::Str,
            VarType::Variant => DataType::Variant,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TypeError {
    pub expected: &'static str,
    pub found: DataType,
}

impl std::fmt::Display for TypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "expected {}, found {:?}", self.expected, self.found)
    }
}
impl std::error::Error for TypeError {}
