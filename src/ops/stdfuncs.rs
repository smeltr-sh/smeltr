//! The stdlib table: one row per signature, from which the name lookup (`by_name`), the type judgment
//! (`result_type`), the dispatcher (`kernel`) and the catalog (`ROWS`) are all generated -- the shape `ops::table`
//! gives operators.
//!
//! A function is its rows: `parseInt` is `[Str]` and `[Str, I64]` the way `+` is `I64, I64` and `F64, F64`. A
//! parameter is a [`Param`] pattern and a result a [`Ret`] rule; `result_type` takes the first row whose arity and
//! patterns fit the arguments.
//!
//! Calls null-propagate the way operators do: a null argument fits any parameter and types as if it were that
//! parameter, and a call whose every argument is null is itself null. A variant
//! argument fits none: `isNull` alone reads one, and `plan::resolve` handles it ahead of the table. `null_safe` names
//! the functions that answer on null -- `isNull` tests it, `push` keeps it as a position -- and keep their result type.
//!
//! A parameter is declared by kind. `Pattern` is a regex literal, interned at compile time and read off the IR by
//! `expr::eval`; it never travels as a column, so a row with one has no kernel. A row with no parameters (`now`)
//! takes its row count from `Ctx::rows`.

use crate::core::bits;
use crate::core::column::{Column, DataType, StrBuilder, VarType};
use crate::core::ctx::{CompiledPattern, Ctx};
use crate::ops::helpers::{Operand, side};
use crate::ops::{OpKernel, arith, arrayops, convert, datetime, hashops, logic, netops, strops, useragent, webops};

/// What a parameter accepts. `null` fits every pattern; `Variant` fits none. An `I64` fits where an `F64` does, and
/// `plan::resolve` widens it.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Param {
    /// Anything, nested types included.
    Any,
    /// `Str` or `Bytes`: what a kernel reads as bytes, whatever they mean.
    Data,
    /// `Str` only: parsing reads characters, not bytes.
    Str,
    /// `Bytes` only: what has no text until something says how to read it.
    Bytes,
    I64,
    F64,
    Bool,
    /// `I64` or `F64`.
    Num,
    DateTime,
    Duration,
    /// `Str`, `Bytes` or any `Array`: what has a length.
    Sized,
    /// An `Array` whose elements are scalars ([`is_pushable_scalar`]) -- what a kernel can build element-wise.
    /// `length` takes any array, through `Sized`.
    Array,
    /// Any `Array`: what `arrayops::insert_at` builds into, which nests as deep as the element type.
    List,
    /// A value a `List` may hold: any but a variant.
    Element,
    /// `I64`, `F64`, `Bool`, `Str`, `Bytes`, `DateTime` or `Duration`: what `toString` renders.
    Printable,
    /// A regex literal: consumed at compile time, never a column.
    Pattern,
}

/// What a call site hands `result_type` for one argument.
#[derive(Clone, PartialEq, Debug)]
pub enum Arg {
    Ty(DataType),
    /// A regex literal, already interned.
    Pattern,
}

impl Param {
    pub(crate) fn accepts(self, a: &Arg) -> bool {
        let t = match (self, a) {
            (Param::Pattern, Arg::Pattern) => return true,
            (Param::Pattern, Arg::Ty(_)) | (_, Arg::Pattern) => return false,
            (_, Arg::Ty(t)) => t,
        };
        let d = match t {
            DataType::Null => return true,
            DataType::Variant => return false,
            d => d,
        };
        match self {
            Param::Any => true,
            Param::Data => matches!(d, DataType::Str | DataType::Bytes),
            Param::Str => *d == DataType::Str,
            Param::Bytes => *d == DataType::Bytes,
            Param::I64 => *d == DataType::I64,
            Param::F64 => matches!(d, DataType::F64 | DataType::I64),
            Param::Bool => *d == DataType::Bool,
            Param::Num => d.is_numeric(),
            Param::DateTime => *d == DataType::DateTime,
            Param::Duration => *d == DataType::Duration,
            Param::Sized => matches!(d, DataType::Str | DataType::Bytes | DataType::Array(_)),
            Param::Array => matches!(d, DataType::Array(e) if is_pushable_scalar(e)),
            Param::List => matches!(d, DataType::Array(_)),
            Param::Element => true,
            Param::Printable => matches!(
                d,
                DataType::I64
                    | DataType::F64
                    | DataType::Bool
                    | DataType::Str
                    | DataType::Bytes
                    | DataType::DateTime
                    | DataType::Duration
            ),
            Param::Pattern => false,
        }
    }

    /// How the parameter reads in a type error: the engine's names.
    pub fn describe(self) -> &'static str {
        match self {
            Param::Any => "any",
            Param::Data => "str/bytes",
            Param::Str => "str",
            Param::Bytes => "bytes",
            Param::I64 => "i64",
            Param::F64 => "f64",
            Param::Bool => "bool",
            Param::Num => "i64/f64",
            Param::DateTime => "datetime",
            Param::Duration => "duration",
            Param::Sized => "str/bytes/array",
            Param::Array => "array of scalars",
            Param::List => "array",
            Param::Element => "value",
            Param::Printable => "i64/f64/bool/str/bytes/datetime/duration",
            Param::Pattern => "/pattern/",
        }
    }
}

/// How a row's result type is decided from the arguments.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Ret {
    Fixed(DataType),
    /// An `Array` of a scalar.
    ArrayOf(DataType),
    /// Argument `i`'s type.
    Same(usize),
    /// `I64` when every argument is `I64`, else `F64`.
    NumericJoin,
    /// `push`: the array's element type met with every value's, as two ternary branches meet (`DataType::unify`):
    /// `I64` widens to `F64`, `null` takes the other's type, and anything else must match exactly. A null
    /// array is an empty one. The `usize` is where the values start -- `1` for `push`, `2` for `insert`, whose second
    /// argument is the position.
    Pushed(usize),
    /// Argument `i`'s element type; `null` when the array is null.
    Element(usize),
    /// A fixed type, provided argument `1` is an element of argument `0`'s array (or either is null).
    ForElement(DataType),
    /// Every argument's array type, met as one (`DataType::unify`); a null argument is an empty array.
    SameArray,
    /// A closed object of these fields, as a function written in the language returns one.
    Object(&'static [(&'static str, Field)]),
}

/// A field of an object a built-in returns. A table row is a static, and a `DataType::Struct` cannot be one.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Field {
    Leaf(DataType),
    Object(&'static [(&'static str, Field)]),
}

impl Field {
    pub fn data_type(&self) -> DataType {
        match self {
            Field::Leaf(t) => t.clone(),
            Field::Object(fields) => object_type(fields),
        }
    }
}

/// The closed object type `fields` describe.
pub fn object_type(fields: &[(&str, Field)]) -> DataType {
    DataType::Struct(
        fields.iter().map(|(n, f)| (n.to_string(), f.data_type())).collect(),
        false,
    )
}

impl Ret {
    fn resolve(&self, args: &[Arg]) -> Option<DataType> {
        // `None` for a variant argument, which no parameter accepts.
        let ty = |i: usize| match &args[i] {
            Arg::Ty(DataType::Variant) => None,
            Arg::Ty(t) => Some(t.clone()),
            Arg::Pattern => Some(DataType::Null),
        };
        Some(match self {
            Ret::Fixed(t) => t.clone(),
            Ret::ArrayOf(t) => DataType::Array(Box::new(t.clone())),
            Ret::Same(i) => ty(*i)?,
            Ret::NumericJoin => {
                let all = |t: DataType| {
                    args.iter().all(|a| match a {
                        Arg::Ty(DataType::Null) => true,
                        Arg::Ty(k) => *k == t,
                        _ => false,
                    })
                };
                if all(DataType::I64) {
                    DataType::I64
                } else {
                    DataType::F64
                }
            }
            // Every value is an element of the same array, so the tail has to agree with itself as well as with the
            // array: `push(xs, "a", 1)` has no common element type, but `push(xs, 1, 1.5)` is an array of numbers.
            Ret::Pushed(first) => {
                let mut elem = match ty(0)? {
                    DataType::Array(e) => *e,
                    DataType::Null => DataType::Null,
                    _ => return None,
                };
                for i in *first..args.len() {
                    elem = elem.unify(&ty(i)?)?;
                }
                DataType::Array(Box::new(elem))
            }
            Ret::Element(i) => match ty(*i)? {
                DataType::Array(elem) => *elem,
                DataType::Null => DataType::Null,
                _ => return None,
            },
            // The value must be one the array's elements meet: the same type, or an `i64` beside an `f64`, which
            // the kernel compares by value.
            Ret::ForElement(t) => match (ty(0), ty(1)) {
                (Some(DataType::Array(elem)), Some(v)) if elem.unify(&v).is_none() => {
                    return None;
                }
                _ => t.clone(),
            },
            // Every array's elements meet as one type, as a ternary's branches do.
            Ret::SameArray => {
                let mut elem = DataType::Null;
                for i in 0..args.len() {
                    match ty(i)? {
                        DataType::Null => {}
                        DataType::Array(a) => elem = elem.unify(&a)?,
                        _ => return None,
                    }
                }
                DataType::Array(Box::new(elem))
            }
            Ret::Object(fields) => object_type(fields),
        })
    }
}

/// A type as the reference spells it: the engine's names. `Param::describe` uses the same spellings.
fn type_name(t: &DataType) -> String {
    t.to_string()
}

impl Ret {
    /// How the result type reads in the reference, for a row taking `params`.
    pub fn describe(&self, params: &[Param]) -> String {
        match self {
            Ret::Fixed(t) | Ret::ForElement(t) => type_name(t),
            Ret::ArrayOf(t) => format!("array({})", type_name(t)),
            Ret::Same(i) if params.get(*i) == Some(&Param::Array) => "the array's type".into(),
            Ret::Same(0) => "the value's type".into(),
            Ret::Same(i) => format!("argument {}'s type", i + 1),
            Ret::NumericJoin => "i64 when every argument is, else f64".into(),
            Ret::Pushed(_) => "the array's type".into(),
            Ret::Element(_) => "the element type".into(),
            Ret::SameArray => "the array's type".into(),
            Ret::Object(fields) => object_type(fields).spell(),
        }
    }
}

/// One signature of one function.
#[derive(Debug)]
pub struct Row {
    pub func: StdFunc,
    pub name: &'static str,
    pub params: &'static [Param],
    pub ret: Ret,
}

impl Row {
    /// The parameter list as it reads in a type error: `(Str/Bytes, /pattern/)`.
    pub fn describe(&self) -> String {
        let params: Vec<&str> = self.params.iter().map(|p| p.describe()).collect();
        format!("({})", params.join(", "))
    }

    /// The same, for a function whose last parameter repeats: `(Array of scalars, element, ...)`.
    pub fn describe_variadic(&self) -> String {
        let params: Vec<&str> = self.params.iter().map(|p| p.describe()).collect();
        format!("({}, ...)", params.join(", "))
    }

    /// Whether every argument fits, the ones past this row's length filling its last parameter.
    fn accepts(&self, args: &[Arg]) -> bool {
        let Some(last) = self.params.last() else {
            return args.is_empty();
        };
        args.iter()
            .enumerate()
            .all(|(i, a)| self.params.get(i).unwrap_or(last).accepts(a))
    }
}

macro_rules! ret {
    (same $i:literal) => {
        Ret::Same($i)
    };
    (numeric_join) => {
        Ret::NumericJoin
    };
    (pushed) => {
        Ret::Pushed(1)
    };
    (pushed_at $i:literal) => {
        Ret::Pushed($i)
    };
    (array $t:ident) => {
        Ret::ArrayOf(DataType::$t)
    };
    (element $i:literal) => {
        Ret::Element($i)
    };
    (for_element $t:ident) => {
        Ret::ForElement(DataType::$t)
    };
    (same_array) => {
        Ret::SameArray
    };
    (object $fields:path) => {
        Ret::Object($fields)
    };
    ($t:ident) => {
        Ret::Fixed(DataType::$t)
    };
}

/// `null_safe` names the functions that answer on null rather than propagating it; `variadic` those whose last
/// parameter repeats.
macro_rules! stdfuncs {
    (
        null_safe: [ $($nfunc:ident),* ];
        variadic: [ $($vfunc:ident),* ];
        $( $func:ident $name:literal = $kern:expr ; $( [ $($p:ident),* ] => ( $($r:tt)+ ) ),+ ; )*
    ) => {
        #[derive(Copy, Clone, PartialEq, Eq, Debug)]
        pub enum StdFunc {
            $($func,)*
        }

        /// Every signature, one row per overload, in declaration order.
        pub static ROWS: &[Row] = &[
            $($(Row { func: StdFunc::$func, name: $name, params: &[$(Param::$p),*], ret: ret!($($r)+) },)+)*
        ];

        impl StdFunc {
            /// Sees only the plain function name: method-call syntax has already been desugared by the parser or
            /// `plan::resolve::resolve_call`.
            pub fn by_name(name: &str) -> Option<StdFunc> {
                Some(match name {
                    $($name => StdFunc::$func,)*
                    _ => return None,
                })
            }

            pub fn name(self) -> &'static str {
                match self {
                    $(StdFunc::$func => $name,)*
                }
            }

            /// Whether a call whose arguments are all null is itself null.
            pub fn propagates_null(self) -> bool {
                !matches!(self, $(StdFunc::$nfunc)|*)
            }

            /// Whether the last parameter of every row repeats: `push(xs, a, b, c)`, `min(a, b, c)`. One call is one
            /// pass, where a chain of two-argument calls is one pass and one intermediate column each.
            pub fn is_variadic(self) -> bool {
                matches!(self, $(StdFunc::$vfunc)|*)
            }

            /// `None` for a function with a `Pattern` parameter, whose arguments do not all travel as columns.
            pub fn kernel(self) -> Option<OpKernel> {
                match self {
                    $(StdFunc::$func => $kern,)*
                }
            }
        }
    };
}

fn some(k: OpKernel) -> Option<OpKernel> {
    Some(k)
}

stdfuncs! {
    null_safe: [IsNull, Push, Insert];
    variadic: [Push, Insert, Concat, Min, Max, InCidr];

    IsNull      "isNull"      = some(logic::is_null);          [Any] => (Bool);
    Length      "length"      = some(length_any);              [Sized] => (I64);
    ToString    "toString"    = some(|c, i| convert::convert_or_refuse(c, i, convert::to_string));   [Printable] => (Str), [Bytes, Str] => (Str);
    Decode      "decode"      = some(|c, i| convert::convert_or_refuse(c, i, convert::decode));      [Bytes, Str] => (Str);
    Encode      "encode"      = some(|c, i| convert::convert_or_refuse(c, i, convert::encode));      [Str, Str] => (Bytes);
    ParseBytes  "parseBytes"  = some(|c, i| convert::convert_or_refuse(c, i, convert::parse_bytes)); [Str] => (Bytes), [Str, Str] => (Bytes);
    ParseInt    "parseInt"    = some(|c, i| convert::read_or_refuse(c, i, convert::parse_int));      [Str] => (I64), [Str, I64] => (I64);
    ParseHex    "parseHex"    = some(|c, i| convert::read_or_refuse(c, i, convert::parse_hex));      [Str] => (I64);
    ParseFloat  "parseFloat"  = some(|c, i| convert::read_or_refuse(c, i, convert::parse_float));    [Str] => (F64);
    Round       "round"       = some(arith::round_f64);        [F64] => (I64);
    Floor       "floor"       = some(arith::floor_f64);        [F64] => (I64);
    Ceil        "ceil"        = some(arith::ceil_f64);         [F64] => (I64);
    Trunc       "trunc"       = some(arith::trunc_f64);        [F64] => (I64);
    // Integer division truncating toward zero, the pair of `%`.
    Idiv        "idiv"        = some(arith::idiv_num);         [Num, Num] => (numeric_join);
    Sign        "sign"        = some(arith::sign_num);         [Num] => (I64);
    Abs         "abs"         = some(arith::abs_num);          [Num] => (same 0);
    Min         "min"         = some(arith::min_num);          [Num, Num] => (numeric_join);
    Max         "max"         = some(arith::max_num);          [Num, Num] => (numeric_join);
    Clamp       "clamp"       = some(arith::clamp_num);        [Num, Num, Num] => (numeric_join);
    Slice       "slice"       = some(slice_any);               [Str, I64] => (Str), [Str, I64, I64] => (Str), [Array, I64] => (same 0), [Array, I64, I64] => (same 0);
    IndexOf     "indexOf"     = some(index_of_any);            [Str, Str] => (I64), [Array, Element] => (for_element I64);
    ToUpperCase "toUpperCase" = some(strops::str_to_upper);    [Str] => (Str);
    ToLowerCase "toLowerCase" = some(strops::str_to_lower);    [Str] => (Str);
    Trim        "trim"        = some(strops::str_trim);        [Str] => (Str);
    StartsWith  "startsWith"  = some(strops::str_starts_with); [Str, Str] => (Bool);
    EndsWith    "endsWith"    = some(strops::str_ends_with);   [Str, Str] => (Bool);
    Includes    "includes"    = some(includes_any);            [Str, Str] => (Bool), [Array, Element] => (for_element Bool);
    TrimStart   "trimStart"   = some(strops::str_trim_start);  [Str] => (Str);
    TrimEnd     "trimEnd"     = some(strops::str_trim_end);    [Str] => (Str);
    PadStart    "padStart"    = some(strops::str_pad_start);   [Str, I64] => (Str), [Str, I64, Str] => (Str);
    PadEnd      "padEnd"      = some(strops::str_pad_end);     [Str, I64] => (Str), [Str, I64, Str] => (Str);
    Repeat      "repeat"      = some(strops::str_repeat);      [Str, I64] => (Str);
    LastIndexOf "lastIndexOf" = some(strops::str_last_index_of); [Str, Str] => (I64);
    At          "at"          = some(at_any);                  [Str, I64] => (Str), [Array, I64] => (element 0);
    ParseTime   "parseTime"   = some(|c, i| convert::read_or_refuse(c, i, convert::parse_time));     [Str] => (DateTime), [Str, Str] => (DateTime), [Str, DateTime] => (DateTime), [Str, Str, DateTime] => (DateTime);
    ParseDuration "parseDuration" = some(|c, i| convert::read_or_refuse(c, i, convert::parse_duration)); [Str] => (Duration);
    Year        "year"        = some(datetime::dt_year);       [DateTime] => (I64);
    Month       "month"       = some(datetime::dt_month);      [DateTime] => (I64);
    Day         "day"         = some(datetime::dt_day);        [DateTime] => (I64);
    Hour        "hour"        = some(datetime::dt_hour);       [DateTime] => (I64);
    Minute      "minute"      = some(datetime::dt_minute);     [DateTime] => (I64);
    Second      "second"      = some(datetime::dt_second);     [DateTime] => (I64);
    DayOfWeek   "dayOfWeek"   = some(datetime::dt_day_of_week); [DateTime] => (I64);
    Millisecond "millisecond" = some(datetime::dt_millisecond); [DateTime] => (I64);
    EpochMillis "epochMillis" = some(datetime::dt_epoch_millis); [DateTime] => (I64);
    FromEpochMillis "fromEpochMillis" = some(datetime::from_epoch_millis); [I64] => (DateTime);
    EpochSeconds "epochSeconds" = some(datetime::dt_epoch_seconds); [DateTime] => (I64);
    FromEpochSeconds "fromEpochSeconds" = some(datetime::from_epoch_seconds); [I64] => (DateTime);
    Duration    "duration"    = some(datetime::duration_from); [Num, Str] => (Duration);
    CountIn     "countIn"     = some(datetime::duration_count_in); [Duration, Str] => (F64);
    Split       "split"       = some(arrayops::str_split);     [Str, Str] => (array Str), [Str, Pattern] => (array Str);
    Push        "push"        = some(arrayops::array_push);    [List, Element] => (pushed);
    Insert      "insert"      = some(arrayops::array_insert);  [List, I64, Element] => (pushed_at 2);
    Remove      "remove"      = some(arrayops::array_remove);  [Array, I64] => (same 0);
    Resize      "resize"      = some(arrayops::array_resize);  [Array, I64] => (same 0);
    Join        "join"        = some(arrayops::array_join);    [Array, Str] => (Str);
    Concat      "concat"      = some(arrayops::array_concat);  [Array, Array] => (same_array);
    Match       "match"       = None;                          [Str, Pattern] => (array Str);
    Test        "test"        = None;                          [Str, Pattern] => (Bool);
    ReplaceFirst "replaceFirst" = None;                        [Str, Pattern, Str] => (Str);
    ReplaceAll  "replaceAll"  = None;                          [Str, Pattern, Str] => (Str);
    Now         "now"         = some(datetime::now);           [] => (DateTime);
    IsIp        "isIp"        = some(netops::is_ip);           [Str] => (Bool);
    IpVersion   "ipVersion"   = some(netops::ip_version);      [Str] => (I64);
    IpNormalize "ipNormalize" = some(netops::ip_normalize);    [Str] => (Str);
    IpScope     "ipScope"     = some(netops::ip_scope);        [Str] => (Str);
    InCidr      "inCidr"      = some(netops::in_cidr);         [Str, Str] => (Bool);
    CidrNetwork "cidrNetwork" = some(netops::cidr_network);    [Str, I64] => (Str);
    CommunityId "communityId" = some(netops::community_id);    [Str, I64, Str, I64, Str] => (Str), [Str, I64, Str, I64, I64] => (Str), [Str, I64, Str, I64, Str, I64] => (Str), [Str, I64, Str, I64, I64, I64] => (Str);
    Sort        "sort"        = some(arrayops::array_sort);    [Array] => (same 0), [Array, Bool] => (same 0);
    Md5         "md5"         = some(hashops::md5);            [Data] => (Str);
    Sha1        "sha1"        = some(hashops::sha1);           [Data] => (Str);
    Sha256      "sha256"      = some(hashops::sha256);         [Data] => (Str);
    Sha512      "sha512"      = some(hashops::sha512);         [Data] => (Str);
    UrlDecode   "urlDecode"   = some(webops::url_decode);      [Str] => (Str);
    HtmlStrip   "htmlStrip"   = some(webops::html_strip);      [Str] => (Str);
    ParseUrl    "parseUrl"    = some(webops::parse_url);       [Str] => (object webops::URL_PARTS);
    RegisteredDomain "registeredDomain" = some(webops::registered_domain); [Str] => (object webops::DOMAIN_PARTS);
    ParseUserAgent "parseUserAgent" = some(useragent::parse_user_agent); [Str] => (object useragent::USER_AGENT);
}

impl StdFunc {
    /// This function's rows.
    pub fn rows(self) -> impl Iterator<Item = &'static Row> {
        ROWS.iter().filter(move |r| r.func == self)
    }

    /// Whether argument `i` is a regex literal rather than a value, in any row.
    pub fn takes_pattern_at(self, i: usize) -> bool {
        self.rows().any(|row| row.params.get(i) == Some(&Param::Pattern))
    }

    /// The row `result_type` picks for `args`: what `plan::resolve` reads to widen an argument to the type the row
    /// gives it. `None` where no row fits.
    pub fn row_for(self, args: &[Arg]) -> Option<&'static Row> {
        self.rows()
            .find(|row| self.row_fits(row, args.len()) && row.accepts(args))
    }

    /// Whether argument `i` is a value, in any row. `split` takes both at `1`.
    pub fn takes_value_at(self, i: usize) -> bool {
        self.rows()
            .any(|row| matches!(row.params.get(i), Some(p) if *p != Param::Pattern))
    }

    /// Whether argument `i` may come as a constant column: the kernel reads it as one value. A position outside the
    /// set is given rows by `expr::eval`. `min`, `max` and `clamp` read every argument the same way; the rest read a
    /// needle, count, index, unit or format beside the value they work on.
    pub fn takes_constant_at(self, i: usize) -> bool {
        use StdFunc::*;
        match self {
            Min | Max | Clamp | Idiv | IsNull | CommunityId | Decode | Encode => true,
            StartsWith | EndsWith | Includes | IndexOf | LastIndexOf | Split | Join | Repeat | At | Insert | Remove
            | Resize | Duration | CountIn | ParseInt | CidrNetwork | ToString | ParseBytes => i == 1,
            Slice | PadStart | PadEnd | ParseTime => i == 1 || i == 2,
            InCidr => i >= 1,
            Sort => i == 1,
            _ => false,
        }
    }

    /// Whether `row` can take `n` arguments. A variadic row takes its own length or more, the extras all filling its
    /// last parameter.
    pub(crate) fn row_fits(self, row: &Row, n: usize) -> bool {
        if self.is_variadic() {
            n >= row.params.len()
        } else {
            n == row.params.len()
        }
    }

    /// The arities a call may have. For a variadic function this is the minimum -- `call_type_error` renders it as
    /// "2 or more".
    pub fn arities(self) -> Vec<usize> {
        let mut out: Vec<usize> = self.rows().map(|row| row.params.len()).collect();
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Every row as the reference prints it: the parameter list and the result type.
    pub fn typed_rows(self) -> Vec<(String, String)> {
        let describe = if self.is_variadic() {
            Row::describe_variadic
        } else {
            Row::describe
        };
        self.rows()
            .map(|row| (describe(row), row.ret.describe(row.params)))
            .collect()
    }

    /// Every signature as it reads in a type error: `(Str) or (Str, I64)`.
    pub fn signatures_text(self) -> String {
        let describe = if self.is_variadic() {
            Row::describe_variadic
        } else {
            Row::describe
        };
        let rows: Vec<String> = self.rows().map(describe).collect();
        rows.join(" or ")
    }

    /// `None`: these argument types or this arity do not type-check. `null` when every value argument is null and the
    /// function propagates it; a `Pattern` is not a value and does not count.
    pub fn result_type(self, args: &[Arg]) -> Option<DataType> {
        let mut values = args.iter().filter_map(|a| match a {
            Arg::Ty(t) => Some(t),
            Arg::Pattern => None,
        });
        let null = DataType::Null;
        let all_null = values
            .next()
            .is_some_and(|first| *first == null && values.all(|t| *t == null));
        if all_null && self.propagates_null() {
            return self.rows().any(|row| self.row_fits(row, args.len())).then_some(null);
        }
        self.row_for(args).and_then(|row| row.ret.resolve(args))
    }
}

/// The element types an `Array` built at runtime may hold. `null` is one: an element may hold nothing.
pub(crate) fn is_pushable_scalar(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Null
            | DataType::Bool
            | DataType::I64
            | DataType::F64
            | DataType::DateTime
            | DataType::Duration
            | DataType::Str
            | DataType::Bytes
    )
}

/// `length` dispatches on the runtime type: `Array` or text. So do `slice`, `at`, `includes` and `indexOf`, which JS
/// defines on both.
pub fn length_any(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    if matches!(ins[0].data_type(), DataType::Array(_)) {
        arrayops::array_length(ctx, ins)
    } else {
        strops::str_length(ctx, ins)
    }
}

macro_rules! text_or_array {
    ($name:ident, $text:path, $array:path) => {
        pub fn $name(ctx: &mut Ctx, ins: &[&Column]) -> Column {
            if matches!(ins[0].data_type(), DataType::Array(_)) {
                $array(ctx, ins)
            } else {
                $text(ctx, ins)
            }
        }
    };
}

text_or_array!(slice_any, strops::str_slice, arrayops::array_slice);
text_or_array!(at_any, strops::str_at, arrayops::array_at);
text_or_array!(includes_any, strops::str_includes, arrayops::array_includes);
text_or_array!(index_of_any, strops::str_index_of, arrayops::array_index_of);

/// `test(value, /pattern/)` -> `Bool`. Null propagates.
pub fn regex_test(ctx: &mut Ctx, value: &Column, pattern: &CompiledPattern) -> Column {
    let len = value.len();
    let Ok(v) = value.var() else {
        return Column::empty(DataType::Bool, len);
    };
    let mut out = crate::ops::helpers::alloc_with_merged_validity(ctx, &DataType::Bool, len, &[Some(v.validity), None]);
    {
        let o = out.bool_mut().unwrap();
        for i in 0..len {
            // Every row's bit is written: a pooled buffer is not zeroed.
            let hit = v.validity.is_valid(i) && pattern.regex.is_match(&String::from_utf8_lossy(v.get(i)));
            bits::set_row(o.values, i, hit);
        }
    }
    out
}

/// JS spells a named group `$<name>` and the whole match `$&`; the regex crate spells them `${name}` and `${0}`.
/// Rewritten only when a JS spelling is present, so the common case costs a scan. `$$` is left alone.
fn js_replacement(repl: &str) -> std::borrow::Cow<'_, str> {
    if !repl.contains("$<") && !repl.contains("$&") {
        return std::borrow::Cow::Borrowed(repl);
    }
    let mut out = String::with_capacity(repl.len() + 8);
    let mut rest = repl;
    while let Some(i) = rest.find('$') {
        out.push_str(&rest[..i]);
        let tail = &rest[i..];
        if let Some(after) = tail.strip_prefix("$$") {
            out.push_str("$$");
            rest = after;
        } else if let Some(after) = tail.strip_prefix("$&") {
            out.push_str("${0}");
            rest = after;
        } else if let Some(after) = tail.strip_prefix("$<")
            && let Some(close) = after.find('>')
            && close > 0
        {
            out.push_str("${");
            out.push_str(&after[..close]);
            out.push('}');
            rest = &after[close + 1..];
        } else {
            out.push('$');
            rest = &tail[1..];
        }
    }
    out.push_str(rest);
    std::borrow::Cow::Owned(out)
}

/// `replaceFirst`/`replaceAll(value, /pattern/, replacement)` -> `Str`. `all` is which of the two was called, so the
/// choice is in the name at the call site rather than a flag on the pattern. `replacement` takes `$1`, `${name}`, and
/// JS's `$<name>` and `$&`, and is read as one value when it is a constant.
pub fn regex_replace(
    ctx: &mut Ctx,
    value: &Column,
    pattern: &CompiledPattern,
    replacement: &Column,
    all: bool,
) -> Column {
    let len = value.len();
    let (Ok(v), Some(r)) = (value.var(), side::var(replacement)) else {
        return Column::empty(DataType::Str, len);
    };
    let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
    for i in 0..len {
        if !v.validity.is_valid(i) || !r.is_valid(i) {
            b.push_null();
            continue;
        }
        let text = String::from_utf8_lossy(v.get(i));
        let repl = String::from_utf8_lossy(r.at(i));
        let repl = js_replacement(&repl);
        let out = if all {
            pattern.regex.replace_all(&text, repl.as_ref())
        } else {
            pattern.regex.replace(&text, repl.as_ref())
        };
        b.push_grow(ctx.pool, out.as_bytes());
    }
    b.finish()
}

#[cfg(test)]
mod typing {
    use super::*;

    fn k(t: DataType) -> Arg {
        Arg::Ty(t)
    }

    fn null() -> Arg {
        Arg::Ty(DataType::Null)
    }

    fn known(t: DataType) -> Option<DataType> {
        Some(t)
    }

    fn arr(t: DataType) -> DataType {
        DataType::Array(Box::new(t))
    }

    /// The table's array parameter is an array of scalars, so an array of structs or of arrays is refused at the
    /// type judgment, where it is written, and never reaches a kernel.
    #[test]
    fn an_array_of_non_scalars_is_not_an_array_argument() {
        let nested = arr(arr(DataType::Str));
        let structs = arr(DataType::Struct(vec![("k".to_string(), DataType::Str)], false));
        for bad in [nested, structs] {
            for func in [StdFunc::Slice, StdFunc::At, StdFunc::IndexOf, StdFunc::Includes] {
                assert_eq!(
                    func.result_type(&[k(bad.clone()), k(DataType::I64)]),
                    None,
                    "{func:?} accepted {bad:?}"
                );
            }
            assert_eq!(StdFunc::Concat.result_type(&[k(bad.clone()), k(bad.clone())]), None);
            assert_eq!(StdFunc::Join.result_type(&[k(bad.clone()), k(DataType::Str)]), None);
            // `length` reads offsets only, so it still answers for any array.
            assert_eq!(StdFunc::Length.result_type(&[k(bad)]), known(DataType::I64));
        }
        // The scalar case the rule must not disturb.
        assert_eq!(
            StdFunc::At.result_type(&[k(arr(DataType::Str)), k(DataType::I64)]),
            known(DataType::Str)
        );
    }

    #[test]
    fn a_null_argument_beside_a_known_one_types_as_the_known_side() {
        assert_eq!(
            StdFunc::Min.result_type(&[null(), k(DataType::F64)]),
            known(DataType::F64)
        );
        assert_eq!(
            StdFunc::Min.result_type(&[k(DataType::I64), null()]),
            known(DataType::I64)
        );
        assert_eq!(
            StdFunc::ParseInt.result_type(&[null(), k(DataType::I64)]),
            known(DataType::I64)
        );
        assert_eq!(
            StdFunc::Slice.result_type(&[k(DataType::Str), null()]),
            known(DataType::Str)
        );
    }

    /// The operator rule: `null + null` is `null`, and so is `min(null, null)`.
    #[test]
    fn a_call_on_nothing_but_null_is_null() {
        for (f, n) in [
            (StdFunc::Min, 2),
            (StdFunc::Abs, 1),
            (StdFunc::Year, 1),
            (StdFunc::Trim, 1),
            (StdFunc::ParseTime, 2),
            (StdFunc::ToString, 1),
        ] {
            assert_eq!(f.result_type(&vec![null(); n]), known(DataType::Null), "{f:?}");
        }
    }

    #[test]
    fn a_call_on_nothing_but_null_still_needs_a_real_arity() {
        assert_eq!(StdFunc::Min.result_type(&[null()]), None);
        assert_eq!(StdFunc::Trim.result_type(&[null(), null()]), None);
    }

    /// `isNull` answers on null and `push` keeps it as a position: neither result is null.
    #[test]
    fn the_null_safe_functions_keep_their_type_on_null() {
        assert_eq!(StdFunc::IsNull.result_type(&[null()]), known(DataType::Bool));
        assert_eq!(
            StdFunc::Push.result_type(&[null(), k(DataType::Str)]),
            known(arr(DataType::Str))
        );
        assert_eq!(
            StdFunc::Push.result_type(&[k(arr(DataType::I64)), null()]),
            known(arr(DataType::I64))
        );
        // Where nothing decides an element type, the result is `[]`: an array of null.
        assert_eq!(StdFunc::Push.result_type(&[null(), null()]), known(arr(DataType::Null)));
    }

    /// `[]` is an array of null, whose element type the other arguments decide, and stays `[]` where none does.
    #[test]
    fn an_empty_array_takes_the_element_type_it_meets() {
        let empty = k(arr(DataType::Null));
        assert_eq!(
            StdFunc::Push.result_type(&[empty.clone(), null()]),
            known(arr(DataType::Null))
        );
        assert_eq!(
            StdFunc::Push.result_type(&[empty.clone(), k(DataType::Str)]),
            known(arr(DataType::Str))
        );
        assert_eq!(
            StdFunc::Concat.result_type(&[empty.clone(), k(arr(DataType::Str))]),
            known(arr(DataType::Str))
        );
        assert_eq!(
            StdFunc::Concat.result_type(&[empty.clone(), empty.clone()]),
            known(arr(DataType::Null))
        );
        assert_eq!(
            StdFunc::Length.result_type(std::slice::from_ref(&empty)),
            known(DataType::I64)
        );
        assert_eq!(
            StdFunc::At.result_type(&[empty, k(DataType::I64)]),
            known(DataType::Null)
        );
    }

    /// `push` widens `I64` to `F64`, as `DataType::unify` does everywhere else.
    #[test]
    fn push_widens_i64_and_f64_like_unify_but_nothing_else() {
        assert_eq!(
            StdFunc::Push.result_type(&[k(arr(DataType::I64)), k(DataType::F64)]),
            known(arr(DataType::F64))
        );
        assert_eq!(
            StdFunc::Push.result_type(&[k(arr(DataType::F64)), k(DataType::I64)]),
            known(arr(DataType::F64))
        );
        assert_eq!(
            StdFunc::Push.result_type(&[k(arr(DataType::Str)), k(DataType::Bytes)]),
            None
        );
    }

    #[test]
    fn a_type_outside_the_pattern_is_refused() {
        assert_eq!(StdFunc::Trim.result_type(&[k(DataType::I64)]), None);
        assert_eq!(StdFunc::ParseInt.result_type(&[k(DataType::Bytes)]), None);
        assert_eq!(
            StdFunc::Push.result_type(&[k(arr(DataType::I64)), k(DataType::Str)]),
            None
        );
        assert_eq!(
            StdFunc::Min.result_type(&[k(DataType::Duration), k(DataType::Duration)]),
            None
        );
    }

    #[test]
    fn to_string_renders_a_datetime_and_a_duration() {
        assert_eq!(
            StdFunc::ToString.result_type(&[k(DataType::DateTime)]),
            known(DataType::Str)
        );
        assert_eq!(
            StdFunc::ToString.result_type(&[k(DataType::Duration)]),
            known(DataType::Str)
        );
        assert_eq!(
            StdFunc::ToString.result_type(&[k(DataType::Bytes)]),
            known(DataType::Str)
        );
        assert_eq!(
            StdFunc::ToString.result_type(&[k(DataType::Bytes), k(DataType::Str)]),
            known(DataType::Str)
        );
        assert_eq!(
            StdFunc::ToString.result_type(&[k(DataType::I64), k(DataType::Str)]),
            None
        );
    }

    #[test]
    fn an_optional_argument_is_a_second_row() {
        assert_eq!(StdFunc::ParseInt.result_type(&[k(DataType::Str)]), known(DataType::I64));
        assert_eq!(
            StdFunc::ParseInt.result_type(&[k(DataType::Str), k(DataType::I64)]),
            known(DataType::I64)
        );
        assert_eq!(
            StdFunc::ParseInt.result_type(&[k(DataType::Str), k(DataType::I64), k(DataType::I64)]),
            None
        );
        assert_eq!(StdFunc::ParseInt.arities(), vec![1, 2]);
    }

    /// A `Pattern` parameter fits only a regex literal, and a regex literal fits only a `Pattern` parameter.
    #[test]
    fn a_pattern_argument_is_declared_by_the_row() {
        assert_eq!(
            StdFunc::Match.result_type(&[k(DataType::Str), Arg::Pattern]),
            known(arr(DataType::Str))
        );
        assert_eq!(StdFunc::Match.result_type(&[k(DataType::Str), k(DataType::Str)]), None);
        assert_eq!(
            StdFunc::Test.result_type(&[k(DataType::Str), Arg::Pattern]),
            known(DataType::Bool)
        );
        assert_eq!(StdFunc::Trim.result_type(&[Arg::Pattern]), None);
        assert_eq!(
            StdFunc::ReplaceAll.result_type(&[k(DataType::Str), Arg::Pattern, k(DataType::Str)]),
            known(DataType::Str)
        );
        assert_eq!(
            StdFunc::ReplaceAll.result_type(&[k(DataType::Bytes), Arg::Pattern, k(DataType::Str)]),
            None
        );
        assert!(StdFunc::Match.takes_pattern_at(1));
        assert!(!StdFunc::Match.takes_pattern_at(0));
        assert!(!StdFunc::Trim.takes_pattern_at(0));
    }

    /// `split` takes a string or a regex in the same position: two rows, and the call site picks by what it sees.
    #[test]
    fn a_position_may_take_a_value_in_one_row_and_a_pattern_in_another() {
        assert_eq!(
            StdFunc::Split.result_type(&[k(DataType::Str), k(DataType::Str)]),
            known(arr(DataType::Str))
        );
        assert_eq!(
            StdFunc::Split.result_type(&[k(DataType::Str), Arg::Pattern]),
            known(arr(DataType::Str))
        );
        assert!(StdFunc::Split.takes_pattern_at(1) && StdFunc::Split.takes_value_at(1));
        assert!(StdFunc::Match.takes_pattern_at(1) && !StdFunc::Match.takes_value_at(1));
    }

    /// The pattern is not a value: `match(null, /x/)` is a call on nothing but null.
    #[test]
    fn a_pattern_does_not_count_toward_null_propagation() {
        assert_eq!(
            StdFunc::Match.result_type(&[null(), Arg::Pattern]),
            known(DataType::Null)
        );
        assert_eq!(
            StdFunc::ReplaceAll.result_type(&[k(DataType::Str), Arg::Pattern, null()]),
            known(DataType::Str)
        );
    }

    #[test]
    fn a_row_with_no_parameters_types_on_no_arguments() {
        assert_eq!(StdFunc::Now.result_type(&[]), known(DataType::DateTime));
        assert_eq!(StdFunc::Now.result_type(&[k(DataType::I64)]), None);
        assert_eq!(StdFunc::Now.arities(), vec![0]);
    }

    #[test]
    fn signatures_read_as_the_error_will_show_them() {
        assert_eq!(StdFunc::Match.signatures_text(), "(str, /pattern/)");
        assert_eq!(StdFunc::Md5.signatures_text(), "(str/bytes)");
        assert_eq!(StdFunc::ParseInt.signatures_text(), "(str) or (str, i64)");
        assert_eq!(StdFunc::Now.signatures_text(), "()");
    }

    /// `slice`, `at`, `includes` and `indexOf` take an array or text in position 0, as `length` does; an element
    /// argument has to be one of that array's, and `at` yields one.
    #[test]
    fn the_array_rows_type_against_the_element() {
        let ints = arr(DataType::I64);
        assert_eq!(
            StdFunc::Includes.result_type(&[k(ints.clone()), k(DataType::I64)]),
            known(DataType::Bool)
        );
        assert_eq!(
            StdFunc::Includes.result_type(&[k(ints.clone()), k(DataType::Str)]),
            None
        );
        assert_eq!(
            StdFunc::IndexOf.result_type(&[k(ints.clone()), null()]),
            known(DataType::I64)
        );
        assert_eq!(
            StdFunc::At.result_type(&[k(ints.clone()), k(DataType::I64)]),
            known(DataType::I64)
        );
        // A null first argument fits the first row, the text one; the kernel dispatches on the runtime type anyway.
        assert_eq!(
            StdFunc::At.result_type(&[null(), k(DataType::I64)]),
            known(DataType::Str)
        );
        assert_eq!(
            StdFunc::Slice.result_type(&[k(ints.clone()), k(DataType::I64)]),
            known(ints.clone())
        );
        assert_eq!(
            StdFunc::Concat.result_type(&[k(ints.clone()), k(ints.clone())]),
            known(ints.clone())
        );
        assert_eq!(
            StdFunc::Concat.result_type(&[k(ints.clone()), k(arr(DataType::Str))]),
            None
        );
        assert_eq!(
            StdFunc::Join.result_type(&[k(ints), k(DataType::Str)]),
            known(DataType::Str)
        );
    }
}

/// The GUI's autocomplete (`gui-frontend/src/stdlib.ts`) reads the functions from the website's reference, which
/// `tests/website-reference-tests.rs` holds to this table. It keeps one list of its own, the functions a condition
/// cannot call; this keeps it equal to the engine's.
#[cfg(test)]
mod gui_stdlib_sync {
    use std::collections::BTreeSet;

    const SOURCE: &str = include_str!("../../gui-frontend/src/stdlib.ts");

    fn quoted(text: &str, pattern: &str) -> BTreeSet<String> {
        let re = regex::Regex::new(pattern).unwrap();
        re.captures_iter(text).map(|c| c[1].to_string()).collect()
    }

    fn set(names: impl IntoIterator<Item = &'static str>) -> BTreeSet<String> {
        names.into_iter().map(str::to_string).collect()
    }

    #[test]
    fn the_functions_kept_from_conditions_are_the_engines() {
        let list = quoted(SOURCE, r#"NOT_IN_CONDITIONS = \[([^\]]*)\]"#);
        let have = quoted(list.iter().next().expect("NOT_IN_CONDITIONS"), r#"'([A-Za-z]+)'"#);
        assert_eq!(
            have,
            set(crate::expr::inline::PRELUDE_FUNCTIONS),
            "stdlib.ts's NOT_IN_CONDITIONS"
        );
    }
}

#[cfg(test)]
mod constants {
    use super::*;
    use crate::core::buffer::BufferPool;
    use crate::core::error::ExceptionKind;
    use crate::ops::testing::{one_of, rows_of, run};

    /// The types a parameter is tried with. `Num` and `Data` both ways; an array over `elem`, its `Element` alike.
    fn types_for(p: Param, elem: &DataType) -> Vec<DataType> {
        use DataType::*;
        match p {
            Param::Any | Param::I64 | Param::Printable => vec![I64],
            Param::Num => vec![I64, F64],
            Param::Data => vec![Str, Bytes],
            Param::Str | Param::Sized => vec![Str],
            Param::Bytes => vec![Bytes],
            Param::F64 => vec![F64],
            Param::DateTime => vec![DateTime],
            Param::Duration => vec![Duration],
            Param::Bool => vec![Bool],
            Param::Array | Param::List => vec![Array(Box::new(elem.clone()))],
            Param::Element => vec![elem.clone()],
            Param::Pattern => vec![],
        }
    }

    /// Every way to type a row's parameters, one type per position. A row with an array is tried over each element
    /// type an array may hold.
    fn assignments(params: &[Param]) -> Vec<Vec<DataType>> {
        use DataType::*;
        let elems: &[DataType] = if params.contains(&Param::Array) || params.contains(&Param::List) {
            &[I64, F64, Str, Bool]
        } else {
            &[I64]
        };
        let mut out = Vec::new();
        for elem in elems {
            let mut acc: Vec<Vec<DataType>> = vec![vec![]];
            for p in params {
                acc = acc
                    .iter()
                    .flat_map(|prefix| {
                        types_for(*p, elem).into_iter().map(|t| {
                            let mut v = prefix.clone();
                            v.push(t);
                            v
                        })
                    })
                    .collect();
            }
            out.extend(acc);
        }
        out
    }

    /// Rows at every position, and at each `one` position the one value: a constant column, or that value filled in.
    fn columns(p: &mut BufferPool, tys: &[DataType], one: &dyn Fn(usize) -> bool, constant: bool) -> Vec<Column> {
        tys.iter()
            .enumerate()
            .map(|(j, t)| {
                if !one(j) {
                    rows_of(p, t)
                } else if constant {
                    Column::constant(one_of(t), 4)
                } else {
                    Column::filled(p, &one_of(t), 4)
                }
            })
            .collect()
    }

    type Output = (Vec<Option<String>>, DataType, Vec<ExceptionKind>);

    /// `k` over `tys`, the `one` positions constant or filled. `None` when the kernel refuses the columns.
    fn outcome(k: OpKernel, tys: &[DataType], one: &dyn Fn(usize) -> bool, constant: bool) -> Option<Output> {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut p = BufferPool::new();
            let cols = columns(&mut p, tys, one, constant);
            let refs: Vec<&Column> = cols.iter().collect();
            let out = run(&mut p, k, &refs);
            for c in cols {
                c.reclaim(&mut p);
            }
            out
        }))
        .ok()
    }

    /// `takes_constant_at` names exactly the positions whose kernel reads a constant as the value in every row: the
    /// same rows, nulls, type and exceptions as that value filled in. A position outside the set refuses the column
    /// or answers differently. The refusals are panics, caught here and printed with the rest; the hook is
    /// process-wide and stays put.
    #[test]
    fn takes_constant_at_names_the_positions_that_take_one() {
        for row in ROWS.iter() {
            let Some(k) = row.func.kernel() else { continue };
            for tys in assignments(row.params) {
                // The compiler widens the arguments a number join meets to one type.
                let mixed = tys.contains(&DataType::I64) && tys.contains(&DataType::F64);
                if tys.is_empty() || (mixed && matches!(row.ret, Ret::NumericJoin)) {
                    continue;
                }
                for i in 0..tys.len() {
                    let takes = row.func.takes_constant_at(i);
                    if matches!(tys[i], DataType::Array(_)) {
                        assert!(!takes, "{}: an array is never a constant", row.name);
                        continue;
                    }
                    let what = format!("{}{tys:?} at {i}", row.name);
                    let filled = outcome(k, &tys, &|j| j == i, false).expect(&what);
                    let constant = outcome(k, &tys, &|j| j == i, true);
                    if takes {
                        assert_eq!(
                            constant.as_ref(),
                            Some(&filled),
                            "{what}: refuses a constant, or reads it wrong"
                        );
                    } else {
                        assert_ne!(
                            constant.as_ref(),
                            Some(&filled),
                            "{what}: takes a constant; the fact must say so"
                        );
                    }
                }
                if (0..tys.len()).all(|i| row.func.takes_constant_at(i)) {
                    let what = format!("{}{tys:?}, every argument constant", row.name);
                    let filled = outcome(k, &tys, &|_| true, false).expect(&what);
                    assert_eq!(outcome(k, &tys, &|_| true, true).as_ref(), Some(&filled), "{what}");
                }
            }
        }
    }

    /// A kernel given a column of a type its row does not name, at a position that tolerates null, answers null there;
    /// it does not panic.
    #[test]
    fn a_wrong_typed_argument_at_a_null_tolerant_position_is_null() {
        use DataType::*;
        let cases: [(OpKernel, &[DataType]); 6] = [
            (strops::str_slice, &[Str, F64]),
            (strops::str_slice, &[Str, I64, Str]),
            (strops::str_pad_start, &[Str, I64, I64]),
            (arrayops::array_slice, &[Array(Box::new(I64)), I64, Str]),
            (convert::parse_int, &[Str, Str]),
            (convert::parse_time, &[Str, I64]),
        ];
        for (k, tys) in cases {
            let (rows, _, _) = outcome(k, tys, &|_| false, false).unwrap_or_else(|| panic!("{tys:?} panicked"));
            assert!(rows.iter().all(Option::is_none), "{tys:?}: {rows:?}");
        }
        let mut p = BufferPool::new();
        let pattern = CompiledPattern {
            regex: regex::Regex::new("a").unwrap(),
        };
        let value = rows_of(&mut p, &Str);
        let number = rows_of(&mut p, &I64);
        let mut errors = crate::core::error::ErrorSink::new();
        let out = {
            let mut ctx = Ctx::new(&mut p, &mut errors);
            regex_replace(&mut ctx, &value, &pattern, &number, true)
        };
        assert!(out.is_unbound());
        for c in [value, number, out] {
            c.reclaim(&mut p);
        }
    }

    /// `replaceAll`'s replacement is dispatched around the table, and reads a constant the same way.
    #[test]
    fn regex_replace_reads_a_constant_replacement() {
        let mut p = BufferPool::new();
        let pattern = CompiledPattern {
            regex: regex::Regex::new("a").unwrap(),
        };
        let value = rows_of(&mut p, &DataType::Str);
        let one = Column::constant(one_of(&DataType::Str), 4);
        let filled = Column::filled(&mut p, &one_of(&DataType::Str), 4);
        let mut errors = crate::core::error::ErrorSink::new();
        let (from_one, from_filled) = {
            let mut ctx = Ctx::new(&mut p, &mut errors);
            (
                regex_replace(&mut ctx, &value, &pattern, &one, true),
                regex_replace(&mut ctx, &value, &pattern, &filled, true),
            )
        };
        let text = |c: &Column| {
            (0..4)
                .map(|i| crate::format::json::scalar_text(c, i))
                .collect::<Vec<_>>()
        };
        assert_eq!(text(&from_one), text(&from_filled));
        assert_eq!(
            text(&from_one),
            [Some("s".into()), None, Some("".into()), Some("s".into())]
        );
        for c in [value, one, filled, from_one, from_filled] {
            c.reclaim(&mut p);
        }
    }
}
