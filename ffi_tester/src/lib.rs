use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
        Decimal128Array, Decimal256Array, DurationNanosecondArray, FixedSizeBinaryArray,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray,
        LargeStringArray, NullArray, StringArray, StringViewArray, StructArray, Time32SecondArray,
        Time64NanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array, builder,
    },
    buffer::NullBuffer,
    datatypes::{
        DataType, Field, Fields, Float32Type, Int8Type, Int16Type, IntervalMonthDayNano,
        UInt32Type, i256,
    },
    ffi::{FFI_ArrowArray, FFI_ArrowSchema, to_ffi},
};

// Each `id` corresponds to a specific arrow array, this function is supposed to import the given array, create a new array based on the `id` it receives, assert these two arrays are equal,
// and export the array it created back to the caller.
/// # Safety
///
/// There is no safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrow_ffi_test_case(
    id: u8,
    array: FFI_ArrowArray,
    schema: FFI_ArrowSchema,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) {
    unsafe {
        let array_data = arrow::ffi::from_ffi(array, &schema).unwrap();
        array_data.validate_full().unwrap();

        let out_data = make_array(id).to_data();
        assert_eq!(array_data, out_data);

        let (out_a, out_s) = to_ffi(&out_data).unwrap();
        *out_array = out_a;
        *out_schema = out_s;
    };
}

fn make_array(id: u8) -> ArrayRef {
    match id {
        0 => make_i8(),
        1 => make_i16(),
        2 => make_i32(),
        3 => make_i64(),
        4 => make_u8(),
        5 => make_u16(),
        6 => make_u32(),
        7 => make_u64(),
        8 => make_null(),
        9 => make_bool(),
        10 => make_f32(),
        11 => make_f64(),
        12 => make_binary(),
        13 => make_large_binary(),
        14 => make_utf8(),
        15 => make_large_utf8(),
        16 => make_decimal128(),
        17 => make_decimal256(),
        18 => make_date32(),
        19 => make_date64(),
        20 => make_time32(),
        21 => make_time64(),
        22 => make_timestamp(),
        23 => make_interval_year_month(),
        24 => make_interval_day_time(),
        25 => make_interval_month_day_nano(),
        26 => make_list(),
        27 => make_large_list(),
        28 => make_struct(),
        29 => make_dense_union(),
        30 => make_sparse_union(),
        31 => make_fixed_size_binary(),
        32 => make_fixed_size_list(),
        33 => make_map(),
        34 => make_duration(),
        35 => make_run_end_encoded(),
        36 => make_binary_view(),
        37 => make_utf8_view(),
        38 => make_list_view(),
        39 => make_large_list_view(),
        40 => make_dict(),
        _ => unreachable!(),
    }
}

fn make_dict() -> ArrayRef {
    let mut builder = builder::BinaryDictionaryBuilder::<Int8Type>::new();

    builder.append(b"abc").unwrap();
    builder.append_null();
    builder.append(b"def").unwrap();
    builder.append(b"def").unwrap();
    builder.append(b"abc").unwrap();

    Arc::new(builder.finish())
}

fn make_large_list_view() -> ArrayRef {
    let mut b = builder::LargeListViewBuilder::new(builder::UInt16Builder::new());

    b.append_null();
    b.append_value([Some(5), None, Some(69)]);
    b.append_null();
    b.append_value([Some(5), None, Some(69), Some(11)]);

    Arc::new(b.finish().slice(1, 2))
}

fn make_list_view() -> ArrayRef {
    let mut b = builder::ListViewBuilder::new(builder::UInt16Builder::new());

    b.append_null();
    b.append_value([Some(5), None, Some(69)]);
    b.append_null();
    b.append_value([Some(5), None, Some(69), Some(11)]);

    Arc::new(b.finish().slice(1, 2))
}

fn make_utf8_view() -> ArrayRef {
    Arc::new(StringViewArray::from_iter([
        Some("hello"),
        Some("world"),
        None,
    ]))
}

fn make_binary_view() -> ArrayRef {
    Arc::new(BinaryViewArray::from_iter([
        Some("hello"),
        Some("world"),
        None,
    ]))
}

fn make_run_end_encoded() -> ArrayRef {
    let mut builder = builder::PrimitiveRunBuilder::<Int16Type, UInt32Type>::new();
    builder.append_value(1234);
    builder.append_value(1234);
    builder.append_value(1234);
    builder.append_null();
    builder.append_value(5678);
    builder.append_value(5678);

    Arc::new(builder.finish())
}

fn make_map() -> ArrayRef {
    let mut builder = builder::MapBuilder::new(
        None,
        builder::StringBuilder::new(),
        builder::UInt32Builder::with_capacity(4),
    );

    builder.keys().append_value("joe");
    builder.values().append_value(1);
    builder.append(true).unwrap();

    builder.keys().append_value("blogs");
    builder.values().append_value(2);
    builder.keys().append_value("foo");
    builder.values().append_value(4);
    builder.append(true).unwrap();
    builder.append(true).unwrap();
    builder.append(false).unwrap();

    Arc::new(builder.finish())
}

fn make_fixed_size_list() -> ArrayRef {
    let mut b = builder::FixedSizeListBuilder::new(builder::UInt16Builder::new(), 3);

    b.values().append_value(1);
    b.values().append_null();
    b.values().append_value(2);
    b.append(true);
    b.values().append_value(32);
    b.values().append_value(32);
    b.values().append_null();
    b.append(false);

    Arc::new(b.finish())
}

fn make_duration() -> ArrayRef {
    Arc::new(DurationNanosecondArray::from_iter_values([
        69i64, 69, 11, 15,
    ]))
}

fn make_fixed_size_binary() -> ArrayRef {
    Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [Some(b"anan"), Some(b"zaaa"), None, Some(b"xddd")].into_iter(),
            4,
        )
        .unwrap(),
    )
}

fn make_dense_union() -> ArrayRef {
    let mut b = builder::UnionBuilder::new_dense();

    b.append::<Float32Type>("ft", 69.69f32).unwrap();
    b.append::<UInt32Type>("mint", 699).unwrap();
    b.append_null::<Float32Type>("ft").unwrap();

    Arc::new(b.build().unwrap())
}

fn make_sparse_union() -> ArrayRef {
    let mut b = builder::UnionBuilder::new_sparse();

    b.append::<Float32Type>("ft", 69.69f32).unwrap();
    b.append::<UInt32Type>("mint", 699).unwrap();
    b.append_null::<Float32Type>("ft").unwrap();

    Arc::new(b.build().unwrap())
}

fn make_struct() -> ArrayRef {
    Arc::new(StructArray::new(
        Fields::from(vec![
            Arc::new(Field::new("a", DataType::UInt32, true)),
            Arc::new(Field::new("b", DataType::UInt64, true)),
            Arc::new(Field::new(
                "c",
                DataType::List(Arc::new(Field::new_list_field(DataType::UInt16, true))),
                true,
            )),
        ]),
        vec![
            make_u32().slice(0, 2),
            make_u64().slice(0, 2),
            make_list().slice(0, 2),
        ],
        Some(NullBuffer::from_iter([true, false])),
    ))
}

fn make_large_list() -> ArrayRef {
    let mut b = builder::LargeListBuilder::new(builder::UInt16Builder::new());

    b.append_null();
    b.append_value([Some(5), None, Some(69)]);
    b.append_null();
    b.append_value([Some(5), None, Some(69), Some(11)]);

    Arc::new(b.finish().slice(1, 2))
}

fn make_list() -> ArrayRef {
    let mut b = builder::ListBuilder::new(builder::UInt16Builder::new());

    b.append_null();
    b.append_value([Some(5), None, Some(69)]);
    b.append_null();
    b.append_value([Some(5), None, Some(69), Some(11)]);

    Arc::new(b.finish().slice(1, 2))
}

fn make_interval_year_month() -> ArrayRef {
    Arc::new(IntervalYearMonthArray::from_iter([Some(9i32), None]))
}

fn make_interval_day_time() -> ArrayRef {
    Arc::new(IntervalDayTimeArray::from_iter([
        None,
        Some(arrow::datatypes::IntervalDayTime {
            days: 69i32,
            milliseconds: 11i32,
        }),
    ]))
}

fn make_interval_month_day_nano() -> ArrayRef {
    Arc::new(IntervalMonthDayNanoArray::from_iter([
        None,
        None,
        None,
        None,
        Some(IntervalMonthDayNano {
            days: 69,
            months: 69,
            nanoseconds: 1131,
        }),
    ]))
}

fn make_date32() -> ArrayRef {
    Arc::new(Date32Array::from_iter_values([69i32, 69, 11, 15]))
}

fn make_date64() -> ArrayRef {
    Arc::new(Date64Array::from_iter_values([69i64, 69, 11, 15]))
}

fn make_time32() -> ArrayRef {
    Arc::new(Time32SecondArray::from_iter([
        Some(69i32),
        Some(11),
        None,
        None,
    ]))
}

fn make_time64() -> ArrayRef {
    Arc::new(Time64NanosecondArray::from_iter([
        Some(69i64),
        Some(11),
        None,
        None,
    ]))
}

fn make_timestamp() -> ArrayRef {
    Arc::new(TimestampSecondArray::from_iter([Some(123i64), None]).with_timezone("Africa/Abidjan"))
}

fn make_decimal128() -> ArrayRef {
    Arc::new(
        Decimal128Array::from_iter([1i128, 2, 3, 4, 69])
            .with_precision_and_scale(31, -31)
            .unwrap(),
    )
}

fn make_decimal256() -> ArrayRef {
    Arc::new(
        Decimal256Array::from_iter([i256::from(69), i256::from(-69)])
            .with_precision_and_scale(31, -31)
            .unwrap(),
    )
}

fn make_large_utf8() -> ArrayRef {
    Arc::new(LargeStringArray::from_iter([
        Some("hello"),
        Some("world"),
        None,
    ]))
}

fn make_utf8() -> ArrayRef {
    Arc::new(StringArray::from_iter([Some("hello"), Some("world"), None]))
}

fn make_large_binary() -> ArrayRef {
    Arc::new(LargeBinaryArray::from_iter([
        Some("hello"),
        Some("world"),
        None,
    ]))
}

fn make_binary() -> ArrayRef {
    Arc::new(BinaryArray::from_iter([Some("hello"), Some("world"), None]))
}

fn make_f32() -> ArrayRef {
    Arc::new(Float32Array::from_iter([
        Some(1f32),
        Some(-1.),
        Some(69.),
        None,
        Some(-69.),
        None,
    ]))
}

fn make_f64() -> ArrayRef {
    Arc::new(Float64Array::from_iter([
        Some(1f64),
        Some(-1.),
        Some(69.),
        None,
        Some(-69.),
        None,
    ]))
}

fn make_bool() -> ArrayRef {
    Arc::new(BooleanArray::from_iter([Some(true), Some(false), None]))
}

fn make_null() -> ArrayRef {
    Arc::new(NullArray::new(69))
}

fn make_i8() -> ArrayRef {
    Arc::new(Int8Array::from_iter([
        Some(1i8),
        Some(-1),
        Some(69),
        None,
        Some(-69),
        None,
    ]))
}

fn make_i16() -> ArrayRef {
    let out: ArrayRef = Arc::new(Int16Array::from_iter([
        Some(1i16),
        Some(-1),
        Some(69),
        None,
        Some(-69),
        None,
        Some(111),
    ]));

    out.slice(0, 6)
}

fn make_i32() -> ArrayRef {
    Arc::new(Int32Array::from_iter([
        Some(1i32),
        Some(-1),
        Some(69),
        None,
        Some(-69),
        None,
    ]))
}

fn make_i64() -> ArrayRef {
    Arc::new(Int64Array::from_iter([
        Some(1i64),
        Some(-1),
        Some(69),
        None,
        Some(-69),
        None,
    ]))
}

fn make_u8() -> ArrayRef {
    Arc::new(UInt8Array::from_iter([1u8, 1, 69, 69]))
}

fn make_u16() -> ArrayRef {
    Arc::new(UInt16Array::from_iter([
        Some(1u16),
        Some(1),
        None,
        None,
        Some(69),
        Some(69),
    ]))
}

fn make_u32() -> ArrayRef {
    Arc::new(UInt32Array::from_iter([
        Some(1u32),
        Some(1),
        None,
        None,
        Some(69),
        Some(69),
    ]))
}

fn make_u64() -> ArrayRef {
    Arc::new(UInt64Array::from_iter([
        Some(1u64),
        Some(1),
        None,
        None,
        Some(69),
        Some(69),
    ]))
}
