//! This module is for comparing arrays for equality

const arr = @import("./array.zig");
const bitmap = @import("./bitmap.zig");

pub const Error = error{
    Lengths,
    Types,
    NullCounts,
    Elements,
    Validity,
};

/// Checks if two arrays are semantically equal, returns an error if they are not
///
/// Mostly intended for implementing tests
pub fn expect_equal(left: arr.Array, right: arr.Array) Error!void {
    if (left.type_ != right.type_) {
        return error.Types;
    }

    return switch (left.type_) {
        .i8 => expect_equal_primitive(i8, left.to(.i8), right.to(.i8)),
        .i16 => expect_equal_primitive(i16, left.to(.i16), right.to(.i16)),
        .i32 => expect_equal_primitive(i32, left.to(.i32), right.to(.i32)),
        .i64 => expect_equal_primitive(i64, left.to(.i64), right.to(.i64)),
        .u8 => expect_equal_primitive(u8, left.to(.u8), right.to(.u8)),
        .u16 => expect_equal_primitive(u16, left.to(.u16), right.to(.u16)),
        .u32 => expect_equal_primitive(u32, left.to(.u32), right.to(.u32)),
        .u64 => expect_equal_primitive(u64, left.to(.u64), right.to(.u64)),
        else => unreachable,
    };
}

fn expect_equal_primitive(comptime T: type, left: *const arr.PrimitiveArr(T), right: *const arr.PrimitiveArr(T)) Error!void {
    if (left.len != right.len) {
        return error.Lengths;
    }

    if (left.null_count != right.null_count) {
        return error.NullCounts;
    }

    if (left.null_count == 0) {
        var i: u32 = left.offset;
        while (i < left.len + left.offset) : (i += 1) {
            const l = left.values[i];
            const r = right.values[i];

            if (l != r) {
                return error.Elements;
            }
        }
    } else {
        const left_validity = left.validity.?;
        const right_validity = right.validity.?;

        var i: u32 = left.offset;
        while (i < left.len + left.offset) : (i += 1) {
            if (bitmap.get(left_validity, i) != bitmap.get(right_validity, i)) {
                return error.Validity;
            }
        }

        i = left.offset;
        while (i < left.len + left.offset) : (i += 1) {
            const l = left.values[i];
            const r = right.values[i];

            if (l != r and !bitmap.get(left_validity, i)) {
                return error.Elements;
            }
        }
    }
}
