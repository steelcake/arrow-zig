use std::sync::Arc;

use arrow::{array::{ArrayRef, Int8Array}, ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema}};

// Each `id` corresponds to a specific arrow array, this function is supposed to import the given array, create a new array based on the `id` it receives, assert these two arrays are equal,
// and export the array it created back to the caller.
#[unsafe(no_mangle)]
pub extern "C" fn arrow_ffi_test_case(id: u8, array: FFI_ArrowArray, schema: FFI_ArrowSchema, out_array: *mut FFI_ArrowArray, out_schema: *mut FFI_ArrowSchema) {
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
        _ => unreachable!(),
    }
}

fn make_i8() -> ArrayRef {
    Arc::new(Int8Array::from_iter([Some(1i8), Some(-1), Some(69), None, Some(-69), None].into_iter()))
}
