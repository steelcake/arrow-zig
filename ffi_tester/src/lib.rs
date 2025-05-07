use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Decimal256Array, Float32Array,
        Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
        LargeStringArray, NullArray, StringArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    },
    datatypes::i256,
    ffi::{FFI_ArrowArray, FFI_ArrowSchema, to_ffi},
};

// Each `id` corresponds to a specific arrow array, this function is supposed to import the given array, create a new array based on the `id` it receives, assert these two arrays are equal,
// and export the array it created back to the caller.
#[unsafe(no_mangle)]
pub extern "C" fn arrow_ffi_test_case(
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
        _ => unreachable!(),
    }
}

fn make_decimal128() -> ArrayRef {
    Arc::new(
        Decimal128Array::from_iter([1i128, 2, 3, 4, 69].into_iter())
            .with_precision_and_scale(31, -31)
            .unwrap(),
    )
}

fn make_decimal256() -> ArrayRef {
    Arc::new(
        Decimal256Array::from_iter([i256::from(69), i256::from(-69)].into_iter())
            .with_precision_and_scale(31, -31)
            .unwrap(),
    )
}

fn make_large_utf8() -> ArrayRef {
    Arc::new(LargeStringArray::from_iter(
        [Some("hello"), Some("world"), None].into_iter(),
    ))
}

fn make_utf8() -> ArrayRef {
    Arc::new(StringArray::from_iter(
        [Some("hello"), Some("world"), None].into_iter(),
    ))
}

fn make_large_binary() -> ArrayRef {
    Arc::new(LargeBinaryArray::from_iter(
        [Some("hello"), Some("world"), None].into_iter(),
    ))
}

fn make_binary() -> ArrayRef {
    Arc::new(BinaryArray::from_iter(
        [Some("hello"), Some("world"), None].into_iter(),
    ))
}

fn make_f32() -> ArrayRef {
    Arc::new(Float32Array::from_iter(
        [Some(1f32), Some(-1.), Some(69.), None, Some(-69.), None].into_iter(),
    ))
}

fn make_f64() -> ArrayRef {
    Arc::new(Float64Array::from_iter(
        [Some(1f64), Some(-1.), Some(69.), None, Some(-69.), None].into_iter(),
    ))
}

fn make_bool() -> ArrayRef {
    Arc::new(BooleanArray::from_iter(
        [Some(true), Some(false), None].into_iter(),
    ))
}

fn make_null() -> ArrayRef {
    Arc::new(NullArray::new(69))
}

fn make_i8() -> ArrayRef {
    Arc::new(Int8Array::from_iter(
        [Some(1i8), Some(-1), Some(69), None, Some(-69), None].into_iter(),
    ))
}

fn make_i16() -> ArrayRef {
    let out: ArrayRef = Arc::new(Int16Array::from_iter(
        [
            Some(1i16),
            Some(-1),
            Some(69),
            None,
            Some(-69),
            None,
            Some(111),
        ]
        .into_iter(),
    ));

    out.slice(0, 6)
}

fn make_i32() -> ArrayRef {
    Arc::new(Int32Array::from_iter(
        [Some(1i32), Some(-1), Some(69), None, Some(-69), None].into_iter(),
    ))
}

fn make_i64() -> ArrayRef {
    Arc::new(Int64Array::from_iter(
        [Some(1i64), Some(-1), Some(69), None, Some(-69), None].into_iter(),
    ))
}

fn make_u8() -> ArrayRef {
    Arc::new(UInt8Array::from_iter([1u8, 1, 69, 69].into_iter()))
}

fn make_u16() -> ArrayRef {
    Arc::new(UInt16Array::from_iter(
        [Some(1u16), Some(1), None, None, Some(69), Some(69)].into_iter(),
    ))
}

fn make_u32() -> ArrayRef {
    Arc::new(UInt32Array::from_iter(
        [Some(1u32), Some(1), None, None, Some(69), Some(69)].into_iter(),
    ))
}

fn make_u64() -> ArrayRef {
    Arc::new(UInt64Array::from_iter(
        [Some(1u64), Some(1), None, None, Some(69), Some(69)].into_iter(),
    ))
}
