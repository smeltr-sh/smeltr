//! The intermediate representation: `Expr` with names resolved to slots, regexes to pattern ids, and the checker's own
//! judgment of its type cached on every node.
//!
//! Expression intermediates never get slots; they live inside one tree walk.

use crate::core::column::DataType;
use crate::core::id::{PatternId, SlotId};
use crate::expr::ast::{Span, Value};
use crate::ops::objectops::ObjectOp;
use crate::ops::stdfuncs::StdFunc;
use crate::ops::{BinOp, UnOp};

#[derive(Clone, Debug)]
pub struct Ir {
    /// The *expected* type, as the checker judged it. A value whose shape only the row knows is `DataType::Variant`,
    /// and the runtime allocates a variant column for it.
    ///
    /// A numeric node yields exactly this type at runtime: an `I64` that overflows fails its row and stays `I64`.
    pub ty: DataType,
    pub span: Span,
    pub kind: IrKind,
}

impl Ir {
    pub fn new(ty: DataType, span: Span, kind: IrKind) -> Ir {
        Ir { ty, span, kind }
    }
}

#[derive(Clone, Debug)]
pub enum IrKind {
    Literal(Value),
    Field(SlotId),
    Binary {
        op: BinOp,
        lhs: Box<Ir>,
        rhs: Box<Ir>,
    },
    Unary {
        op: UnOp,
        operand: Box<Ir>,
    },
    Ternary {
        cond: Box<Ir>,
        then: Box<Ir>,
        else_: Box<Ir>,
    },
    Call {
        func: StdFunc,
        args: Vec<Ir>,
    },
    Index {
        base: Box<Ir>,
        index: Box<Ir>,
    },
    Array(Vec<Ir>),
    /// An object built from its fields, and the unclaimed fields it carries where it has some: a struct column.
    Object {
        fields: Vec<(String, Ir)>,
        unclaimed: Option<Box<Ir>>,
    },
    /// The unclaimed fields `value` holds, a variant object per row: a struct's own, or a variant's value where it is
    /// an object, null where it is not.
    Unclaimed(Box<Ir>),
    /// Unclaimed fields built in layers, each laid over the last: a spread lays down every key of the object its value
    /// holds on the row, and a key lays down one key whole. A variant.
    Merge(Vec<Layer>),
    Regex(PatternId),
    /// A function over an object's keys (`ops::objectops`), typed by `plan::resolve`.
    ObjectOp {
        op: ObjectOp,
        args: Vec<Ir>,
    },
    /// `value` coerced to `to` by the coercion table: the node a declaration puts where a value meets its type. A
    /// column already of the type passes through untouched.
    Coerce {
        to: DataType,
        value: Box<Ir>,
    },
    /// `array` with position `index` set to `value` (`ops::arrayops::array_set`).
    SetElement {
        array: Box<Ir>,
        index: Box<Ir>,
        value: Box<Ir>,
    },

    /// N-ary forms produced by `flatten`.
    ConcatN(Vec<Ir>),
    AndN(Vec<Ir>),
    OrN(Vec<Ir>),
    CoalesceN(Vec<Ir>),
}

/// One layer of an `IrKind::Merge`.
#[derive(Clone, Debug)]
pub enum Layer {
    /// `...value`: each key of the value's object replaces the key of that name, on the rows where the value holds one.
    Spread(Ir),
    /// `key: value`: the value, whole, at the key, a `str`. A key only the row knows lays nothing on a row where it
    /// is null. A null value takes the key out.
    Key(Ir, Ir),
}

impl Layer {
    pub fn value(&self) -> &Ir {
        match self {
            Layer::Spread(v) | Layer::Key(_, v) => v,
        }
    }

    /// A key's name, where it is written in the code.
    pub fn name(&self) -> Option<&str> {
        match self {
            Layer::Key(
                Ir {
                    kind: IrKind::Literal(Value::Str(name)),
                    ..
                },
                _,
            ) => Some(name),
            _ => None,
        }
    }

    /// The key, where it is one, then the value.
    pub fn children(&self) -> impl Iterator<Item = &Ir> {
        let key = match self {
            Layer::Key(k, _) => Some(k),
            Layer::Spread(_) => None,
        };
        key.into_iter().chain(std::iter::once(self.value()))
    }

    fn map(self, f: &mut impl FnMut(Ir) -> Ir) -> Layer {
        match self {
            Layer::Spread(v) => Layer::Spread(f(v)),
            Layer::Key(k, v) => Layer::Key(f(k), f(v)),
        }
    }
}

/// `kind` with `f` applied to each direct child.
pub fn map_children(kind: IrKind, f: &mut impl FnMut(Ir) -> Ir) -> IrKind {
    let mut b = |x: Box<Ir>| Box::new(f(*x));
    match kind {
        IrKind::Literal(_) | IrKind::Field(_) | IrKind::Regex(_) => kind,
        IrKind::Binary { op, lhs, rhs } => IrKind::Binary {
            op,
            lhs: b(lhs),
            rhs: b(rhs),
        },
        IrKind::Unary { op, operand } => IrKind::Unary {
            op,
            operand: b(operand),
        },
        IrKind::Ternary { cond, then, else_ } => IrKind::Ternary {
            cond: b(cond),
            then: b(then),
            else_: b(else_),
        },
        IrKind::Index { base, index } => IrKind::Index {
            base: b(base),
            index: b(index),
        },
        IrKind::Coerce { to, value } => IrKind::Coerce { to, value: b(value) },
        IrKind::Unclaimed(value) => IrKind::Unclaimed(b(value)),
        IrKind::SetElement { array, index, value } => IrKind::SetElement {
            array: b(array),
            index: b(index),
            value: b(value),
        },
        IrKind::Call { func, args } => IrKind::Call {
            func,
            args: args.into_iter().map(&mut *f).collect(),
        },
        IrKind::ObjectOp { op, args } => IrKind::ObjectOp {
            op,
            args: args.into_iter().map(&mut *f).collect(),
        },
        IrKind::Array(v) => IrKind::Array(v.into_iter().map(&mut *f).collect()),
        IrKind::ConcatN(v) => IrKind::ConcatN(v.into_iter().map(&mut *f).collect()),
        IrKind::AndN(v) => IrKind::AndN(v.into_iter().map(&mut *f).collect()),
        IrKind::OrN(v) => IrKind::OrN(v.into_iter().map(&mut *f).collect()),
        IrKind::CoalesceN(v) => IrKind::CoalesceN(v.into_iter().map(&mut *f).collect()),
        IrKind::Object { fields, unclaimed } => IrKind::Object {
            fields: fields.into_iter().map(|(n, e)| (n, *b(Box::new(e)))).collect(),
            unclaimed: unclaimed.map(b),
        },
        IrKind::Merge(layers) => IrKind::Merge(layers.into_iter().map(|l| l.map(f)).collect()),
    }
}

/// Collapse left-nested chains of associative operators into n-ary nodes.
///
/// Runs on `Ir` because legality needs types: `Add` is associative on `Str` and on `Array`, and on neither number.
/// Float addition is not associative at all, and reassociating it would change results; an integer chain is
/// associative until a step overflows, which fails the row, and folding it would hide which step that was.
pub fn flatten(ir: Ir) -> Ir {
    let Ir { ty, span, kind } = ir;
    let kind = match kind {
        IrKind::Binary { op, lhs, rhs } => {
            let lhs = flatten(*lhs);
            let rhs = flatten(*rhs);

            if let Some(v) = fold_int(op, &ty, &lhs, &rhs) {
                return Ir::new(ty, span, IrKind::Literal(v));
            }

            let legal = match op {
                BinOp::Add => matches!(ty, DataType::Str | DataType::Array(_)),
                BinOp::And | BinOp::Or | BinOp::Coalesce => true,
                _ => false,
            };

            if !legal {
                return Ir::new(
                    ty,
                    span,
                    IrKind::Binary {
                        op,
                        lhs: Box::new(lhs),
                        rhs: Box::new(rhs),
                    },
                );
            }

            let mut operands = Vec::new();
            splice(op, lhs, &mut operands);
            splice(op, rhs, &mut operands);

            match op {
                BinOp::Add => match fold_text(&ty, &operands) {
                    Some(v) => IrKind::Literal(v),
                    None => IrKind::ConcatN(operands),
                },
                BinOp::And => IrKind::AndN(operands),
                BinOp::Or => IrKind::OrN(operands),
                BinOp::Coalesce => IrKind::CoalesceN(operands),
                _ => unreachable!(),
            }
        }
        IrKind::Unary { op, operand } => IrKind::Unary {
            op,
            operand: Box::new(flatten(*operand)),
        },
        IrKind::Ternary { cond, then, else_ } => IrKind::Ternary {
            cond: Box::new(flatten(*cond)),
            then: Box::new(flatten(*then)),
            else_: Box::new(flatten(*else_)),
        },
        IrKind::Call { func, args } => IrKind::Call {
            func,
            args: args.into_iter().map(flatten).collect(),
        },
        IrKind::Index { base, index } => IrKind::Index {
            base: Box::new(flatten(*base)),
            index: Box::new(flatten(*index)),
        },
        IrKind::Coerce { to, value } => IrKind::Coerce {
            to,
            value: Box::new(flatten(*value)),
        },
        IrKind::Array(items) => IrKind::Array(items.into_iter().map(flatten).collect()),
        IrKind::Merge(layers) => IrKind::Merge(layers.into_iter().map(|l| l.map(&mut flatten)).collect()),
        kind @ (IrKind::Object { .. } | IrKind::Unclaimed(_) | IrKind::ObjectOp { .. }) => {
            map_children(kind, &mut flatten)
        }
        other => other,
    };
    Ir::new(ty, span, kind)
}

/// The answer for two literal integers, where the kernel's answer is certain. Add, subtract and multiply fold when
/// they do not overflow. An overflow is left to the kernel, which fails the row rather than wrapping, and a literal
/// here cannot say that.
fn fold_int(op: BinOp, ty: &DataType, lhs: &Ir, rhs: &Ir) -> Option<Value> {
    if *ty != DataType::I64 {
        return None;
    }
    let (IrKind::Literal(Value::I64(a)), IrKind::Literal(Value::I64(b))) = (&lhs.kind, &rhs.kind) else {
        return None;
    };
    match op {
        BinOp::Add => a.checked_add(*b),
        BinOp::Sub => a.checked_sub(*b),
        BinOp::Mul => a.checked_mul(*b),
        _ => None,
    }
    .map(Value::I64)
}

/// Text joined end to end, when every operand is a literal string. The chain is one node by the time this is asked,
/// so it folds whole or not at all.
fn fold_text(ty: &DataType, operands: &[Ir]) -> Option<Value> {
    if *ty != DataType::Str {
        return None;
    }
    let mut joined = String::new();
    for o in operands {
        let IrKind::Literal(Value::Str(part)) = &o.kind else {
            return None;
        };
        joined.push_str(part);
    }
    Some(Value::Str(joined))
}

/// Append `child` to `out`, splicing its operands in if it is already the same n-ary node.
fn splice(op: BinOp, child: Ir, out: &mut Vec<Ir>) {
    let matches = matches!(
        (op, &child.kind),
        (BinOp::Add, IrKind::ConcatN(_))
            | (BinOp::And, IrKind::AndN(_))
            | (BinOp::Or, IrKind::OrN(_))
            | (BinOp::Coalesce, IrKind::CoalesceN(_))
    );
    if matches {
        match child.kind {
            IrKind::ConcatN(v) | IrKind::AndN(v) | IrKind::OrN(v) | IrKind::CoalesceN(v) => out.extend(v),
            _ => unreachable!(),
        }
    } else {
        out.push(child);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::column::DataType::*;

    fn field(slot: SlotId, ty: DataType) -> Ir {
        Ir::new(ty, Span::default(), IrKind::Field(slot))
    }

    fn bin(op: BinOp, ty: DataType, l: Ir, r: Ir) -> Ir {
        Ir::new(
            ty,
            Span::default(),
            IrKind::Binary {
                op,
                lhs: Box::new(l),
                rhs: Box::new(r),
            },
        )
    }

    fn chain(op: BinOp, ty: DataType, n: usize) -> Ir {
        let mut e = field(0, ty.clone());
        for i in 1..n {
            e = bin(op, ty.clone(), e, field(i, ty.clone()));
        }
        e
    }

    #[test]
    fn associative_chains_flatten_completely() {
        assert!(matches!(chain(BinOp::Add, Str, 5).flattened(), IrKind::ConcatN(v) if v.len() == 5));
        // `+` joins arrays too, so a chain of them is one pass rather than three with two intermediates.
        let arrays = Array(Box::new(Str));
        assert!(matches!(chain(BinOp::Add, arrays, 4).flattened(), IrKind::ConcatN(v) if v.len() == 4));
        assert!(matches!(chain(BinOp::And, Bool, 3).flattened(), IrKind::AndN(v) if v.len() == 3));
        assert!(matches!(chain(BinOp::Coalesce, Str, 3).flattened(), IrKind::CoalesceN(v) if v.len() == 3));
    }

    #[test]
    fn numeric_add_does_not_flatten() {
        assert!(matches!(chain(BinOp::Add, F64, 3).flattened(), IrKind::Binary { .. }));
        assert!(matches!(chain(BinOp::Add, I64, 3).flattened(), IrKind::Binary { .. }));
    }

    #[test]
    fn different_operators_do_not_merge() {
        let inner = bin(BinOp::And, Bool, field(0, Bool), field(1, Bool));
        let outer = bin(BinOp::Or, Bool, inner, field(2, Bool));
        match flatten(outer).kind {
            IrKind::OrN(v) => {
                assert_eq!(v.len(), 2);
                assert!(matches!(v[0].kind, IrKind::AndN(_)));
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn flatten_recurses_under_ternary_and_keeps_the_type() {
        let t = Ir::new(
            Str,
            Span::default(),
            IrKind::Ternary {
                cond: Box::new(field(9, Bool)),
                then: Box::new(chain(BinOp::Add, Str, 3)),
                else_: Box::new(field(3, Str)),
            },
        );
        let f = flatten(t);
        assert_eq!(f.ty, Str);
        match f.kind {
            IrKind::Ternary { then, .. } => assert!(matches!(then.kind, IrKind::ConcatN(_))),
            other => panic!("got {other:?}"),
        }
    }

    impl Ir {
        fn flattened(self) -> IrKind {
            flatten(self).kind
        }
    }
}
