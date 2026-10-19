use super::*;

/// `null` meets any type as that type at any depth, as it does at the top: inside an array's element and a
/// struct's field, and under both at once.
#[test]
fn null_meets_any_type_at_any_depth() {
    use DataType::*;
    let arr = |t: DataType| Array(Box::new(t));
    let obj = |t: DataType| Struct(vec![("a".into(), t)], false);
    assert_eq!(Null.unify(&Str), Some(Str));
    assert_eq!(arr(Null).unify(&arr(Str)), Some(arr(Str)));
    assert_eq!(arr(arr(Null)).unify(&arr(arr(Str))), Some(arr(arr(Str))));
    assert_eq!(obj(Null).unify(&obj(Str)), Some(obj(Str)));
    assert_eq!(obj(arr(Null)).unify(&obj(arr(Str))), Some(obj(arr(Str))));
    assert_eq!(arr(obj(Null)).unify(&arr(obj(I64))), Some(arr(obj(I64))));
    assert_eq!(Null.unify(&obj(Str)), Some(obj(Str)));
    assert_eq!(arr(Null).unify(&Str), None, "[] meets nothing but an array");
    assert_eq!(obj(Null).unify(&arr(Null)), None);
    assert_eq!(obj(arr(Null)).spell(), "{a: array<null>}");
}

#[test]
fn unify_widens_i64_to_f64_and_refuses_bytes() {
    use DataType::*;
    assert_eq!(I64.unify(&F64), Some(F64));
    assert_eq!(F64.unify(&I64), Some(F64));
    assert_eq!(Str.unify(&Str), Some(Str));
    assert_eq!(Str.unify(&Bytes), None);
}

/// `i64` widens to `f64` inside an array's element and a struct's field too. Two structs unify field
/// by field, and a field one side lacks is null there.
#[test]
fn unify_reaches_inside_arrays_and_structs() {
    use DataType::*;
    let ints = Array(Box::new(I64));
    let floats = Array(Box::new(F64));
    assert_eq!(ints.unify(&ints), Some(ints.clone()));
    assert_eq!(ints.unify(&floats), Some(Array(Box::new(F64))));
    assert_eq!(ints.unify(&Array(Box::new(Str))), None);

    let s = Struct(vec![("x".into(), I64)], false);
    let s2 = Struct(vec![("x".into(), F64), ("y".into(), Str)], false);
    assert_eq!(s.unify(&s), Some(s.clone()));
    assert_eq!(
        s.unify(&s2),
        Some(Struct(vec![("x".into(), F64), ("y".into(), Str)], false))
    );
    assert_eq!(s.unify(&Struct(vec![("x".into(), Str)], false)), None);
}

/// A constant answers what it can without rows, refuses what it cannot, and expands from the caller's pool.
#[test]
fn a_constant_column_holds_one_value_until_it_is_expanded() {
    let budget = crate::core::buffer::MemoryBudget::new(1 << 20);
    let mut pool = BufferPool::with_budget(budget.clone());

    let mut c = Column::constant(Scalar::Str("x".into()), 3);
    assert_eq!(c.constant_value(), Some(&Scalar::Str("x".into())));
    assert_eq!(c.data_type(), DataType::Str);
    assert!(!c.has_nulls() && c.is_valid(2) && !c.is_unbound());
    assert!(
        c.var().is_err(),
        "unexpanded, an accessor refuses it the way it refuses an Empty"
    );
    assert_eq!(budget.used(), 0, "no rows, no charge");

    // Gather and copy carry the value across rather than the rows.
    let taken = c.gather(&mut pool, &[2, 0]);
    assert_eq!(taken.constant_value(), Some(&Scalar::Str("x".into())));
    assert_eq!(taken.len(), 2);
    let copy = c.deep_copy(&mut pool);
    assert!(copy.constant_value().is_some());
    taken.reclaim(&mut pool);
    copy.reclaim(&mut pool);

    c.expand(&mut pool);
    assert_eq!(c.constant_value(), None, "expanded, it is rows now");
    let v = c.var().unwrap();
    assert_eq!(
        (v.get(0), v.get(2), v.validity.is_valid(1)),
        (b"x".as_slice(), b"x".as_slice(), true)
    );
    c.reclaim(&mut pool);

    let mut n = Column::constant(Scalar::I64(7), 4);
    n.expand(&mut pool);
    assert_eq!(n.i64().unwrap().values, &[7, 7, 7, 7]);
    n.i64_mut().unwrap().values[1] = 8;
    assert_eq!(n.i64().unwrap().values, &[7, 8, 7, 7]);
    n.reclaim(&mut pool);

    let mut b = Column::constant(Scalar::Bool(true), 70);
    b.expand(&mut pool);
    assert!((0..70).all(|i| bits::get_row(b.bool().unwrap().values, i)));
    b.reclaim(&mut pool);
    drop(pool);
    assert_eq!(budget.used(), 0, "every buffer came back");
}

/// The point of expanding through the caller's pool: a recycled buffer is already charged, so a warm pool
/// answers for nothing. A pool of the constant's own holds no buffers, so it would allocate fresh and charge
/// again for what the caller already had.
#[test]
fn expanding_a_constant_takes_the_pools_buffers() {
    let budget = crate::core::buffer::MemoryBudget::new(1 << 24);
    let mut pool = BufferPool::with_budget(budget.clone());

    let warm = Column::filled(&mut pool, &Scalar::I64(7), 5000);
    warm.reclaim(&mut pool);
    let warmed = budget.used();
    assert!(warmed > 0, "the pool holds buffers now");

    let mut c = Column::constant(Scalar::I64(7), 5000);
    c.expand(&mut pool);
    assert_eq!(
        budget.used(),
        warmed,
        "the warm pool answered it; nothing fresh was allocated"
    );
    assert_eq!(c.i64().unwrap().values[4999], 7);
    c.reclaim(&mut pool);
}

fn pool() -> BufferPool {
    BufferPool::new()
}

// --- Validity ---------------------------------------------------------

#[test]
fn fresh_validity_is_all_valid() {
    let mut p = pool();
    let v = Validity::alloc(&mut p, 100);
    assert_eq!(v.valid_count(), 100);
    assert!(!v.has_nulls());
    assert!((0..100).all(|i| v.is_valid(i)));
    // `is_valid` answers from the count while nothing is null. Only the bitmap shows the bits were set.
    assert_eq!(bits::popcount(v.bits()), 100);
    v.reclaim(&mut p);
}

#[test]
fn alloc_null_is_all_null() {
    let mut p = pool();
    let v = Validity::alloc_null(&mut p, 100);
    assert_eq!(v.valid_count(), 0);
    assert!(v.has_nulls());
    assert!(!v.is_valid(0));
    v.reclaim(&mut p);
}

#[test]
fn set_valid_maintains_the_count() {
    let mut p = pool();
    let mut v = Validity::alloc(&mut p, 10);
    v.set_valid(3, false);
    v.set_valid(3, false); // idempotent
    assert_eq!(v.valid_count(), 9);
    v.set_valid(3, true);
    assert_eq!(v.valid_count(), 10);
    v.reclaim(&mut p);
}

#[test]
fn recount_matches_the_bitmap_after_a_bulk_write() {
    let mut p = pool();
    let mut v = Validity::alloc(&mut p, 200);
    v.bits_mut().iter_mut().for_each(|w| *w = 0);
    assert_eq!(v.recount(), 0);
    assert_eq!(v.valid_count(), 0);
    assert!(v.has_nulls());
    v.reclaim(&mut p);
}

/// Pool buffers come back dirty, so `alloc` must write every bit: set within `len`, clear past it.
#[test]
fn validity_on_a_dirty_buffer_is_all_valid() {
    let mut p = pool();
    let mut v = Validity::alloc(&mut p, 128);
    (0..128).for_each(|i| v.set_valid(i, false));
    v.reclaim(&mut p);
    let v = Validity::alloc(&mut p, 128);
    assert_eq!(bits::popcount(v.bits()), 128);
    v.reclaim(&mut p);

    let mut dirty = p.take(bits::bytes_for(100));
    dirty.as_mut_slice::<u64>().fill(!0u64);
    p.give(dirty);
    let v = Validity::alloc(&mut p, 100);
    assert_eq!(bits::popcount(v.bits()), 100, "the tail past len is clear");
    v.reclaim(&mut p);
}

// --- Empty ------------------------------------------------------------

#[test]
fn empty_costs_nothing_and_reads_as_null() {
    let c = Column::empty(DataType::I64, 50);
    assert!(c.is_unbound());
    assert_eq!(c.len(), 50);
    assert_eq!(c.data_type(), DataType::I64);
    assert!(c.has_nulls());
    assert!(!c.is_valid(0));
    assert!(c.validity().is_none());
}

#[test]
fn accessors_reject_empty_loudly() {
    let c = Column::empty(DataType::I64, 50);
    let e = c.i64().unwrap_err();
    assert_eq!(e.found, DataType::I64);
}

// --- typed accessors --------------------------------------------------

#[test]
fn i64_roundtrip() {
    let mut p = pool();
    let mut c = Column::alloc(&mut p, &DataType::I64, 4);
    c.i64_mut().unwrap().values.copy_from_slice(&[1, -2, 3, 4]);
    assert_eq!(c.i64().unwrap().values, &[1, -2, 3, 4]);
    assert!(!c.has_nulls());
    c.reclaim(&mut p);
}

#[test]
fn accessor_slice_length_is_rows_not_capacity() {
    let mut p = pool();
    let c = Column::alloc(&mut p, &DataType::I64, 3);
    assert_eq!(c.i64().unwrap().values.len(), 3);
    c.reclaim(&mut p);
}

#[test]
fn wrong_type_accessor_errors_with_the_real_type() {
    let mut p = pool();
    let c = Column::alloc(&mut p, &DataType::I64, 4);
    let e = c.f64().unwrap_err();
    assert_eq!(e.expected, "F64");
    assert_eq!(e.found, DataType::I64);
    c.reclaim(&mut p);
}

#[test]
fn datetime_and_duration_are_distinct_despite_sharing_i64() {
    let mut p = pool();
    let c = Column::alloc(&mut p, &DataType::DateTime, 2);
    assert!(c.datetime().is_ok());
    assert!(c.duration().is_err());
    assert!(c.i64().is_err());
    c.reclaim(&mut p);
}

#[test]
fn bool_is_a_bitmap() {
    let mut p = pool();
    let mut c = Column::alloc(&mut p, &DataType::Bool, 130);
    {
        let b = c.bool_mut().unwrap();
        assert_eq!(b.values.len(), 3, "130 rows = 3 words");
        bits::set_row(b.values, 0, true);
        bits::set_row(b.values, 129, true);
    }
    let b = c.bool().unwrap();
    assert!(bits::get_row(b.values, 0));
    assert!(bits::get_row(b.values, 129));
    assert_eq!(bits::popcount(b.values), 2);
    c.reclaim(&mut p);
}

/// A Bool column's tail past `len` is zero even on a recycled buffer.
#[test]
fn bool_alloc_and_gather_zero_the_tail_of_a_dirty_buffer() {
    let mut p = pool();
    let mut dirty = p.take(8);
    dirty.as_mut_slice::<u64>()[0] = !0u64;
    p.give(dirty);
    let mut c = Column::alloc(&mut p, &DataType::Bool, 4);
    {
        let b = c.bool_mut().unwrap();
        for i in 0..4 {
            bits::set_row(b.values, i, false);
        }
    }
    assert_eq!(bits::popcount(c.bool().unwrap().values), 0);

    let mut dirty = p.take(8);
    dirty.as_mut_slice::<u64>()[0] = !0u64;
    p.give(dirty);
    let g = c.gather(&mut p, &[3, 2]);
    assert_eq!(bits::popcount(g.bool().unwrap().values), 0);
    c.reclaim(&mut p);
    g.reclaim(&mut p);
}

#[test]
fn validity_and_values_travel_together() {
    let mut p = pool();
    let mut c = Column::alloc(&mut p, &DataType::I64, 4);
    {
        let f = c.i64_mut().unwrap();
        f.values.copy_from_slice(&[10, 20, 30, 40]);
        f.validity.set_valid(2, false);
    }
    let f = c.i64().unwrap();
    assert_eq!(f.values[2], 30);
    assert!(!f.validity.is_valid(2));
    assert_eq!(f.validity.valid_count(), 3);
    c.reclaim(&mut p);
}

// --- storage shape ----------------------------------------------------

#[test]
fn same_storage_shape_governs_buffer_reuse() {
    use DataType::*;
    assert!(I64.same_storage_shape(&F64));
    assert!(DateTime.same_storage_shape(&Duration));
    assert!(I64.same_storage_shape(&DateTime));
    assert!(!Str.same_storage_shape(&I64));
    assert!(!Bool.same_storage_shape(&I64));
    assert!(Str.same_storage_shape(&Bytes));
}

// --- reclaim ----------------------------------------------------------

#[test]
fn reclaim_recurses_into_list_children() {
    let mut p = pool();
    let child = Column::alloc(&mut p, &DataType::I64, 6);
    let child_ptr = child.i64().unwrap().values.as_ptr();
    let offsets = p.take(4 * 4);
    let validity = Validity::alloc(&mut p, 3);
    let list = Column::list_from_parts(offsets, child, validity, 3);
    list.reclaim(&mut p);
    let reused = p.take(48);
    assert_eq!(reused.as_slice::<i64>().as_ptr(), child_ptr);
}

#[test]
fn reclaim_recurses_into_struct_fields() {
    let mut p = pool();
    let ty = DataType::Struct(vec![("a".into(), DataType::I64), ("b".into(), DataType::I64)], false);
    let c = Column::alloc(&mut p, &ty, 4);
    assert!(c.field("a").is_some());
    assert!(c.field("zz").is_none());
    c.reclaim(&mut p);
}

#[test]
fn set_all_null_returns_buffers_and_keeps_the_type() {
    let mut p = pool();
    let mut c = Column::alloc(&mut p, &DataType::I64, 8);
    c.set_all_null(&mut p);
    assert!(c.is_unbound());
    assert!(c.has_nulls());
    assert_eq!(c.data_type(), DataType::I64);
}

// --- StrBuilder -------------------------------------------------------

#[test]
fn strbuilder_basic_roundtrip() {
    let mut p = pool();
    let mut b = StrBuilder::new(&mut p, VarType::Str, 3);
    b.reserve(&mut p, 3 + 5 + 2);
    b.push(b"foo");
    b.push(b"hello");
    b.push(b"hi");
    let c = b.finish();

    let v = c.var().unwrap();
    assert_eq!(v.get(0), b"foo");
    assert_eq!(v.get(1), b"hello");
    assert_eq!(v.get(2), b"hi");
    assert_eq!(v.offsets, &[0, 3, 8, 10]);
    assert!(v.ascii);
    c.reclaim(&mut p);
}

#[test]
fn strbuilder_nulls_are_zero_length_spans() {
    let mut p = pool();
    let mut b = StrBuilder::new(&mut p, VarType::Str, 3);
    b.reserve(&mut p, 6);
    b.push(b"aaa");
    b.push_null();
    b.push(b"bbb");
    let c = b.finish();

    let v = c.var().unwrap();
    assert_eq!(v.offsets, &[0, 3, 3, 6]);
    assert!(v.validity.is_valid(0));
    assert!(!v.validity.is_valid(1));
    assert_eq!(v.validity.valid_count(), 2);
    assert_eq!(v.row_len(1), 0);
    c.reclaim(&mut p);
}

#[test]
fn strbuilder_detects_non_ascii() {
    let mut p = pool();
    let mut b = StrBuilder::new(&mut p, VarType::Str, 2);
    b.reserve(&mut p, 16);
    b.push(b"plain");
    b.push("caf\u{e9}".as_bytes());
    let c = b.finish();
    assert!(!c.var().unwrap().ascii);
    c.reclaim(&mut p);
}

#[test]
fn empty_strings_are_ascii_and_not_null() {
    let mut p = pool();
    let mut b = StrBuilder::new(&mut p, VarType::Str, 1);
    b.push(b"");
    let c = b.finish();
    let v = c.var().unwrap();
    assert!(v.ascii);
    assert_eq!(v.get(0), b"");
    assert!(v.validity.is_valid(0));
    c.reclaim(&mut p);
}

#[test]
fn total_bytes_measures_from_offsets() {
    let mut p = pool();
    let mut b = StrBuilder::new(&mut p, VarType::Str, 3);
    b.reserve(&mut p, 10);
    b.push(b"ab");
    b.push(b"cde");
    b.push(b"fghij");
    let c = b.finish();
    assert_eq!(c.var().unwrap().total_bytes(), 10);
    c.reclaim(&mut p);
}

#[test]
fn push_grow_handles_unknown_output_size() {
    let mut p = pool();
    let mut b = StrBuilder::new(&mut p, VarType::Str, 4);
    for s in [&b"aaaa"[..], b"bbbbbbbb", b"cc", b"dddddddddddddddd"] {
        b.push_grow(&mut p, s);
    }
    let c = b.finish();
    let v = c.var().unwrap();
    assert_eq!(v.get(3), b"dddddddddddddddd");
    assert_eq!(v.total_bytes(), 4 + 8 + 2 + 16);
    c.reclaim(&mut p);
}

#[test]
fn push_written_grow_writes_each_row_in_place() {
    let mut p = pool();
    let mut b = StrBuilder::new(&mut p, VarType::Str, 2);
    b.push_written_grow(&mut p, 3, |out| out.copy_from_slice(b"abc"));
    b.push_written_grow(&mut p, 100, |out| out.fill(b'z'));
    let c = b.finish();
    let v = c.var().unwrap();
    assert_eq!(v.get(0), b"abc");
    assert_eq!(v.get(1), [b'z'; 100]);
    c.reclaim(&mut p);
}

#[test]
#[should_panic(expected = "rows written")]
fn strbuilder_rejects_a_partially_filled_column() {
    let mut p = pool();
    let mut b = StrBuilder::new(&mut p, VarType::Str, 3);
    b.reserve(&mut p, 8);
    b.push(b"only-one");
    let _ = b.finish();
}

#[test]
fn zero_row_column_is_valid() {
    let mut p = pool();
    let c = Column::alloc(&mut p, &DataType::I64, 0);
    assert_eq!(c.len(), 0);
    assert_eq!(c.i64().unwrap().values.len(), 0);
    assert!(!c.has_nulls());
    c.reclaim(&mut p);
}

// --- gather -------------------------------------------------------------

#[test]
fn gather_selects_and_reorders_fixed_rows() {
    let mut p = pool();
    let mut c = Column::alloc(&mut p, &DataType::I64, 4);
    c.i64_mut().unwrap().values.copy_from_slice(&[10, 20, 30, 40]);
    let g = c.gather(&mut p, &[2, 0, 0, 3]);
    assert_eq!(g.i64().unwrap().values, &[30, 10, 10, 40]);
    c.reclaim(&mut p);
    g.reclaim(&mut p);
}

#[test]
fn gather_carries_fixed_validity() {
    let mut p = pool();
    let mut c = Column::alloc(&mut p, &DataType::I64, 3);
    {
        let f = c.i64_mut().unwrap();
        f.values.copy_from_slice(&[1, 2, 3]);
        f.validity.set_valid(1, false);
    }
    let g = c.gather(&mut p, &[1, 2, 1]);
    let f = g.i64().unwrap();
    assert!(!f.validity.is_valid(0));
    assert!(f.validity.is_valid(1));
    assert!(!f.validity.is_valid(2));
    assert_eq!(f.validity.valid_count(), 1);
    c.reclaim(&mut p);
    g.reclaim(&mut p);
}

#[test]
fn gather_selects_bool_rows() {
    let mut p = pool();
    let mut c = Column::alloc(&mut p, &DataType::Bool, 3);
    {
        let b = c.bool_mut().unwrap();
        bits::set_row(b.values, 0, true);
        bits::set_row(b.values, 1, false);
        bits::set_row(b.values, 2, true);
        b.validity.set_valid(2, false);
    }
    let g = c.gather(&mut p, &[2, 1, 0]);
    let b = g.bool().unwrap();
    assert!(!b.validity.is_valid(0));
    assert!(!bits::get_row(b.values, 1));
    assert!(bits::get_row(b.values, 2));
    c.reclaim(&mut p);
    g.reclaim(&mut p);
}

#[test]
fn gather_selects_str_rows_and_nulls() {
    let mut p = pool();
    let mut b = StrBuilder::new(&mut p, VarType::Str, 3);
    b.reserve(&mut p, 11);
    b.push(b"apple");
    b.push_null();
    b.push(b"cherry");
    let c = b.finish();

    let g = c.gather(&mut p, &[2, 1, 0, 2]);
    let v = g.var().unwrap();
    assert_eq!(v.get(0), b"cherry");
    assert!(!v.validity.is_valid(1));
    assert_eq!(v.get(2), b"apple");
    assert_eq!(v.get(3), b"cherry");
    c.reclaim(&mut p);
    g.reclaim(&mut p);
}

#[test]
fn gather_selects_list_rows_by_concatenating_element_spans() {
    // [ [1,2], [3], [4,5,6] ] gathered as rows [2, 0] -> [ [4,5,6], [1,2] ]
    let mut p = pool();
    let mut child = Column::alloc(&mut p, &DataType::I64, 6);
    child.i64_mut().unwrap().values.copy_from_slice(&[1, 2, 3, 4, 5, 6]);
    let mut offsets = p.take(4 * 4);
    offsets.as_mut_slice::<u32>().copy_from_slice(&[0, 2, 3, 6]);
    let validity = Validity::alloc(&mut p, 3);
    let list = Column::list_from_parts(offsets, child, validity, 3);

    let g = list.gather(&mut p, &[2, 0]);
    let l = g.list().unwrap();
    assert_eq!(l.offsets, &[0, 3, 5]);
    assert_eq!(l.child.i64().unwrap().values, &[4, 5, 6, 1, 2]);
    list.reclaim(&mut p);
    g.reclaim(&mut p);
}

#[test]
fn gather_marks_a_null_list_row() {
    let mut p = pool();
    let child = Column::alloc(&mut p, &DataType::I64, 2);
    let mut offsets = p.take(3 * 4);
    offsets.as_mut_slice::<u32>().copy_from_slice(&[0, 2, 2]);
    let mut validity = Validity::alloc(&mut p, 2);
    validity.set_valid(1, false);
    let list = Column::list_from_parts(offsets, child, validity, 2);

    let g = list.gather(&mut p, &[1, 0]);
    let l = g.list().unwrap();
    assert!(!l.validity.is_valid(0));
    assert!(l.validity.is_valid(1));
    list.reclaim(&mut p);
    g.reclaim(&mut p);
}

#[test]
fn gather_selects_struct_rows_field_by_field() {
    let mut p = pool();
    let mut a = Column::alloc(&mut p, &DataType::I64, 3);
    a.i64_mut().unwrap().values.copy_from_slice(&[1, 2, 3]);
    let mut sb = StrBuilder::new(&mut p, VarType::Str, 3);
    sb.reserve(&mut p, 3);
    sb.push(b"x");
    sb.push(b"y");
    sb.push(b"z");
    let b_col = sb.finish();
    let c = Column::struct_from_parts(vec![("a".into(), a), ("b".into(), b_col)], 3);

    let g = c.gather(&mut p, &[2, 0]);
    assert_eq!(g.field("a").unwrap().i64().unwrap().values, &[3, 1]);
    let gv = g.field("b").unwrap().var().unwrap();
    assert_eq!(gv.get(0), b"z");
    assert_eq!(gv.get(1), b"x");
    c.reclaim(&mut p);
    g.reclaim(&mut p);
}

#[test]
fn gather_on_empty_stays_empty_of_the_same_type() {
    let mut p = pool();
    let c = Column::empty(DataType::I64, 5);
    let g = c.gather(&mut p, &[3, 1, 0]);
    assert!(g.is_unbound());
    assert_eq!(g.data_type(), DataType::I64);
    assert_eq!(g.len(), 3);
    c.reclaim(&mut p);
    g.reclaim(&mut p);
}

#[test]
fn gather_with_no_indices_yields_a_zero_row_column() {
    let mut p = pool();
    let mut c = Column::alloc(&mut p, &DataType::I64, 3);
    c.i64_mut().unwrap().values.copy_from_slice(&[1, 2, 3]);
    let g = c.gather(&mut p, &[]);
    assert_eq!(g.len(), 0);
    c.reclaim(&mut p);
    g.reclaim(&mut p);
}
