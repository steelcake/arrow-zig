//! This module is for comparing arrays for equality

const std = @import("std");

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
pub fn expect_equal(left: *const arr.Array, right: *const arr.Array) Error!void {
    if (@intFromEnum(left.*) != @intFromEnum(right.*)) {
        return error.Types;
    }

    return switch (left.*) {
        .null => |*l| expect_equal_null(l, &right.null),
        .i8 => |*l| expect_equal_primitive(i8, l, &right.i8),
        .i16 => |*l| expect_equal_primitive(i16, l, &right.i16),
        .i32 => |*l| expect_equal_primitive(i32, l, &right.i32),
        .i64 => |*l| expect_equal_primitive(i64, l, &right.i64),
        .u8 => |*l| expect_equal_primitive(u8, l, &right.u8),
        .u16 => |*l| expect_equal_primitive(u16, l, &right.u16),
        .u32 => |*l| expect_equal_primitive(u32, l, &right.u32),
        .u64 => |*l| expect_equal_primitive(u64, l, &right.u64),
        .f16 => |*l| expect_equal_primitive(f16, l, &right.f16),
        .f32 => |*l| expect_equal_primitive(f32, l, &right.f32),
        .f64 => |*l| expect_equal_primitive(f64, l, &right.f64),
        .binary => |*l| expect_equal_binary(.i32, l, &right.binary),
        .utf8 => |*l| expect_equal_binary(.i32, &l.inner, &right.utf8.inner),
        // .bool => BoolArray,
        // .decimal32 => Decimal32Array,
        // .decimal64 => Decimal64Array,
        // .decimal128 => Decimal128Array,
        // .decimal256 => Decimal256Array,
        // .date32 => Date32Array,
        // .date64 => Date64Array,
        // .time32 => Time32Array,
        // .time64 => Time64Array,
        // .timestamp => TimestampArray,
        // .interval_year_month => IntervalYearMonthArray,
        // .interval_day_time => IntervalDayTimeArray,
        // .interval_month_day_nano => IntervalMonthDayNanoArray,
        // .list => ListArray,
        // .struct_ => StructArray,
        // .dense_union => DenseUnionArray,
        // .sparse_union => SparseUnionArray,
        // .fixed_size_binary => FixedSizeBinaryArray,
        // .fixed_size_list => FixedSizeListArray,
        // .map => MapArray,
        // .duration => DurationArray,
        .large_binary => |*l| expect_equal_binary(.i64, l, &right.large_binary),
        .large_utf8 => |*l| expect_equal_binary(.i64, &l.inner, &right.large_utf8.inner),
        // .large_list => LargeListArray,
        // .run_end_encoded => RunEndArray,
        // .binary_view => BinaryViewArray,
        // .utf8_view => Utf8ViewArray,
        // .list_view => ListViewArray,
        // .large_list_view => LargeListViewArray,
        // .dict => DictArray,
        else => unreachable,
    };
}

fn expect_equal_null(left: *const arr.NullArray, right: *const arr.NullArray) Error!void {
    if (left.len != right.len) {
        return error.Lengths;
    }
}

fn expect_equal_binary(comptime index_type: arr.IndexType, left: *const arr.BinaryArr(index_type), right: *const arr.BinaryArr(index_type)) Error!void {
    if (left.len != right.len) {
        return error.Lengths;
    }

    if (left.null_count != right.null_count) {
        return error.NullCounts;
    }

    if (left.null_count == 0) {
        var i: u32 = left.offset;
        while (i < left.len + left.offset) : (i += 1) {
            const l = left.data[@intCast(left.offsets[i])..@intCast(left.offsets[i + 1])];
            const r = right.data[@intCast(right.offsets[i])..@intCast(right.offsets[i + 1])];

            if (!std.mem.eql(u8, l, r)) {
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
            const l = left.data[@intCast(left.offsets[i])..@intCast(left.offsets[i + 1])];
            const r = right.data[@intCast(right.offsets[i])..@intCast(right.offsets[i + 1])];

            if (!std.mem.eql(u8, l, r) and bitmap.get(left_validity, i)) {
                return error.Elements;
            }
        }
    }
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

            if (l != r and bitmap.get(left_validity, i)) {
                return error.Elements;
            }
        }
    }
}
