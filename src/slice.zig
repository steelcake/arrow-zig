const std = @import("std");
const testing = std.testing;
const ArenaAllocator = std.heap.ArenaAllocator;

const arr = @import("./array.zig");
const bitmap = @import("./bitmap.zig");

// for  testing
const builder = @import("./builder.zig");

const OffsetLen = struct {
    null_count: u32,
    offset: u32,
    len: u32,
};

fn slice_impl(validity: ?[]const u8, base: OffsetLen, offset: u32, len: u32) OffsetLen {
    if (base.len < offset + len) {
        unreachable;
    }

    var out = OffsetLen{ .offset = base.offset + offset, .len = len, .null_count = 0 };
    if (base.null_count > 0) {
        const v = validity orelse unreachable;
        out.null_count = bitmap.count_nulls(v, out.offset, out.len);
    }

    return out;
}

pub fn slice_null(array: *const arr.NullArray, offset: u32, len: u32) arr.NullArray {
    const offset_len = slice_impl(null, .{ .offset = 0, .len = array.len, .null_count = 0 }, offset, len);
    return arr.NullArray{ .len = offset_len.len };
}

pub fn slice_primitive(comptime T: type, array: *const arr.PrimitiveArray(T), offset: u32, len: u32) arr.PrimitiveArray(T) {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.PrimitiveArray(T){
        .values = array.values,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_binary(comptime index_type: arr.IndexType, array: *const arr.GenericBinaryArray(index_type), offset: u32, len: u32) arr.GenericBinaryArray(index_type) {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.GenericBinaryArray(index_type){
        .data = array.data,
        .offsets = array.offsets,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_bool(array: *const arr.BoolArray, offset: u32, len: u32) arr.BoolArray {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.BoolArray{
        .values = array.values,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_binary_view(array: *const arr.BinaryViewArray, offset: u32, len: u32) arr.BinaryViewArray {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.BinaryViewArray{
        .views = array.views,
        .buffers = array.buffers,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_fixed_size_binary(array: *const arr.FixedSizeBinaryArray, offset: u32, len: u32) arr.FixedSizeBinaryArray {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.FixedSizeBinaryArray{
        .data = array.data,
        .byte_width = array.byte_width,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_list(comptime index_type: arr.IndexType, array: *const arr.GenericListArray(index_type), offset: u32, len: u32) arr.GenericListArray(index_type) {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.GenericListArray(index_type){
        .inner = array.inner,
        .offsets = array.offsets,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_list_view(comptime index_type: arr.IndexType, array: *const arr.GenericListViewArray(index_type), offset: u32, len: u32) arr.GenericListViewArray(index_type) {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.GenericListViewArray(index_type){
        .inner = array.inner,
        .sizes = array.sizes,
        .offsets = array.offsets,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_fixed_size_list(array: *const arr.FixedSizeListArray, offset: u32, len: u32) arr.FixedSizeListArray {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.FixedSizeListArray{
        .inner = array.inner,
        .item_width = array.item_width,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_struct(array: *const arr.StructArray, offset: u32, len: u32) arr.StructArray {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.StructArray{
        .field_names = array.field_names,
        .field_values = array.field_values,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_map(array: *const arr.MapArray, offset: u32, len: u32) arr.MapArray {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.MapArray{
        .entries = array.entries,
        .offsets = array.offsets,
        .keys_are_sorted = array.keys_are_sorted,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_union(array: *const arr.UnionArray, offset: u32, len: u32) arr.UnionArray {
    const offset_len = slice_impl(null, .{ .offset = array.offset, .len = array.len, .null_count = 0 }, offset, len);
    return arr.UnionArray{
        .type_ids = array.type_ids,
        .children = array.children,
        .type_id_set = array.type_id_set,
        .field_names = array.field_names,
        .len = offset_len.len,
        .offset = offset_len.offset,
    };
}

pub fn slice_sparse_union(array: *const arr.SparseUnionArray, offset: u32, len: u32) arr.SparseUnionArray {
    return arr.SparseUnionArray{ .inner = slice_union(&array.inner, offset, len) };
}

pub fn slice_dense_union(array: *const arr.DenseUnionArray, offset: u32, len: u32) arr.DenseUnionArray {
    return arr.DenseUnionArray{ .inner = slice_union(&array.inner, offset, len), .offsets = array.offsets };
}

pub fn slice_run_end_encoded(array: *const arr.RunEndArray, offset: u32, len: u32) arr.RunEndArray {
    const offset_len = slice_impl(null, .{ .offset = array.offset, .len = array.len, .null_count = 0 }, offset, len);
    return arr.RunEndArray{
        .values = array.values,
        .run_ends = array.run_ends,
        .len = offset_len.len,
        .offset = offset_len.offset,
    };
}

pub fn slice(array: *const arr.Array, offset: u32, len: u32) arr.Array {
    switch (array.*) {
        .null => |*a| {
            return .{ .null = slice_null(a, offset, len) };
        },
        .i8 => |*a| {
            return .{ .i8 = slice_primitive(i8, a, offset, len) };
        },
        .i16 => |*a| {
            return .{ .i16 = slice_primitive(i16, a, offset, len) };
        },
        .i32 => |*a| {
            return .{ .i32 = slice_primitive(i32, a, offset, len) };
        },
        .i64 => |*a| {
            return .{ .i64 = slice_primitive(i64, a, offset, len) };
        },
        .u8 => |*a| {
            return .{ .u8 = slice_primitive(u8, a, offset, len) };
        },
        .u16 => |*a| {
            return .{ .u16 = slice_primitive(u16, a, offset, len) };
        },
        .u32 => |*a| {
            return .{ .u32 = slice_primitive(u32, a, offset, len) };
        },
        .u64 => |*a| {
            return .{ .u64 = slice_primitive(u64, a, offset, len) };
        },
        .f16 => |*a| {
            return .{ .f16 = slice_primitive(f16, a, offset, len) };
        },
        .f32 => |*a| {
            return .{ .f32 = slice_primitive(f32, a, offset, len) };
        },
        .f64 => |*a| {
            return .{ .f64 = slice_primitive(f64, a, offset, len) };
        },
        .binary => |*a| {
            return .{ .binary = slice_binary(.i32, a, offset, len) };
        },
        .large_binary => |*a| {
            return .{ .large_binary = slice_binary(.i64, a, offset, len) };
        },
        .utf8 => |*a| {
            return .{ .utf8 = .{ .inner = slice_binary(.i32, &a.inner, offset, len) } };
        },
        .large_utf8 => |*a| {
            return .{ .large_utf8 = .{ .inner = slice_binary(.i64, &a.inner, offset, len) } };
        },
        .bool => |*a| {
            return .{ .bool = slice_bool(a, offset, len) };
        },
        .binary_view => |*a| {
            return .{ .binary_view = slice_binary_view(a, offset, len) };
        },
        .utf8_view => |*a| {
            return .{ .utf8_view = .{ .inner = slice_binary_view(&a.inner, offset, len) } };
        },
        .decimal32 => |*a| {
            return .{ .decimal32 = .{ .inner = slice_primitive(i32, &a.inner, offset, len), .params = a.params } };
        },
        .decimal64 => |*a| {
            return .{ .decimal64 = .{ .inner = slice_primitive(i64, &a.inner, offset, len), .params = a.params } };
        },
        .decimal128 => |*a| {
            return .{ .decimal128 = .{ .inner = slice_primitive(i128, &a.inner, offset, len), .params = a.params } };
        },
        .decimal256 => |*a| {
            return .{ .decimal256 = .{ .inner = slice_primitive(i256, &a.inner, offset, len), .params = a.params } };
        },
        .fixed_size_binary => |*a| {
            return .{ .fixed_size_binary = slice_fixed_size_binary(a, offset, len) };
        },
        .date32 => |*a| {
            return .{ .date32 = .{ .inner = slice_primitive(i32, &a.inner, offset, len) } };
        },
        .date64 => |*a| {
            return .{ .date64 = .{ .inner = slice_primitive(i64, &a.inner, offset, len) } };
        },
        .time32 => |*a| {
            return .{ .time32 = .{ .inner = slice_primitive(i32, &a.inner, offset, len), .unit = a.unit } };
        },
        .time64 => |*a| {
            return .{ .time64 = .{ .inner = slice_primitive(i64, &a.inner, offset, len), .unit = a.unit } };
        },
        .timestamp => |*a| {
            return .{ .timestamp = .{ .inner = slice_primitive(i64, &a.inner, offset, len), .ts = a.ts } };
        },
        .duration => |*a| {
            return .{ .duration = .{ .inner = slice_primitive(i64, &a.inner, offset, len), .unit = a.unit } };
        },
        .interval_year_month => |*a| {
            return .{ .interval_year_month = .{ .inner = slice_primitive(i32, &a.inner, offset, len) } };
        },
        .interval_day_time => |*a| {
            return .{ .interval_day_time = .{ .inner = slice_primitive([2]i32, &a.inner, offset, len) } };
        },
        .interval_month_day_nano => |*a| {
            return .{ .interval_month_day_nano = .{ .inner = slice_primitive(arr.MonthDayNano, &a.inner, offset, len) } };
        },
        .list => |*a| {
            return .{ .list = slice_list(.i32, a, offset, len) };
        },
        .large_list => |*a| {
            return .{ .large_list = slice_list(.i64, a, offset, len) };
        },
        .list_view => |*a| {
            return .{ .list_view = slice_list_view(.i32, a, offset, len) };
        },
        .large_list_view => |*a| {
            return .{ .large_list_view = slice_list_view(.i64, a, offset, len) };
        },
        .fixed_size_list => |*a| {
            return .{ .fixed_size_list = slice_fixed_size_list(a, offset, len) };
        },
        .struct_ => |*a| {
            return .{ .struct_ = slice_struct(a, offset, len) };
        },
        .map => |*a| {
            return .{ .map = slice_map(a, offset, len) };
        },
        .dense_union => |*a| {
            return .{ .dense_union = slice_dense_union(a, offset, len) };
        },
        .sparse_union => |*a| {
            return .{ .sparse_union = slice_sparse_union(a, offset, len) };
        },
        .run_end_encoded => |*a| {
            return .{ .run_end_encoded = slice_run_end_encoded(a, offset, len) };
        },
        .dict => |*a| {
            switch (a.keys) {
                .i8 => |*i| {
                    return .{ .i8 = slice_primitive(i8, i, offset, len) };
                },
                .i16 => |*i| {
                    return .{ .i16 = slice_primitive(i16, i, offset, len) };
                },
                .i32 => |*i| {
                    return .{ .i32 = slice_primitive(i32, i, offset, len) };
                },
                .i64 => |*i| {
                    return .{ .i64 = slice_primitive(i64, i, offset, len) };
                },
                .u8 => |*i| {
                    return .{ .u8 = slice_primitive(u8, i, offset, len) };
                },
                .u16 => |*i| {
                    return .{ .u16 = slice_primitive(u16, i, offset, len) };
                },
                .u32 => |*i| {
                    return .{ .u32 = slice_primitive(u32, i, offset, len) };
                },
                .u64 => |*i| {
                    return .{ .u64 = slice_primitive(u64, i, offset, len) };
                },
            }
        },
    }
}

test slice {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var b = try builder.Int16Builder.with_capacity(4, true, allocator);

    try b.append_null();
    try b.append_value(69);
    try b.append_option(-69);
    try b.append_null();

    const array = arr.Array{ .i16 = try b.finish() };

    const sliced = slice(&array, 1, 3).i16;

    try testing.expectEqual(1, sliced.null_count);
    try testing.expectEqual(3, sliced.len);
    try testing.expectEqual(1, sliced.offset);

    const array2 = arr.Array{ .i16 = sliced };

    const sliced2 = slice(&array2, 0, 1).i16;

    try testing.expectEqual(0, sliced2.null_count);
    try testing.expectEqual(1, sliced2.len);
    try testing.expectEqual(1, sliced2.offset);
}
