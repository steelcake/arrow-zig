const std = @import("std");
const testing = std.testing;
const arr = @import("./array.zig");
const bitmap = @import("./bitmap.zig");
const slice_mod = @import("./slice.zig");
const builder = @import("./builder.zig");
const test_array = @import("./test_array.zig");
const slice = slice_mod.slice;
const slice_struct = slice_mod.slice_struct;

pub fn get_bool(values: [*]const u8, index: u32) bool {
    return bitmap.get(values, index);
}

pub fn get_bool_opt(values: [*]const u8, validity: [*]const u8, index: u32) ?bool {
    return if (bitmap.get(validity, index))
        bitmap.get(values, index)
    else
        null;
}

pub fn get_primitive(comptime T: type, values: [*]const T, index: u32) T {
    return values[index];
}

pub fn get_primitive_opt(comptime T: type, values: [*]const T, validity: [*]const u8, index: u32) ?T {
    return if (bitmap.get(validity, index))
        values[index]
    else
        null;
}

fn index_cast(comptime index_type: arr.IndexType, index: index_type.to_type()) usize {
    return switch (index_type) {
        .i32 => @intCast(@as(u32, @bitCast(index))),
        .i64 => @intCast(@as(u64, @bitCast(index))),
    };
}

pub fn get_binary(comptime index_type: arr.IndexType, data: [*]const u8, offsets: [*]const index_type.to_type(), index: u32) []const u8 {
    const start = index_cast(index_type, offsets[index]);
    const end = index_cast(index_type, offsets[index +% 1]);

    return data[start..end];
}

pub fn get_binary_opt(comptime index_type: arr.IndexType, data: [*]const u8, offsets: [*]const index_type.to_type(), validity: [*]const u8, index: u32) ?[]const u8 {
    return if (bitmap.get(validity, index))
        get_binary(index_type, data, offsets, index)
    else
        null;
}

pub fn get_binary_view(buffers: [*]const [*]const u8, views: [*]const arr.BinaryView, index: u32) []const u8 {
    const view = views[index];
    const vl = @as(u32, @bitCast(view.length));
    const vo = @as(u32, @bitCast(view.offset));
    const vbi = @as(u32, @bitCast(view.buffer_idx));
    return if (view.length <= 12)
        @as([*]const u8, @ptrCast(&views[index].prefix))[0..vl]
    else
        buffers[vbi][vo .. vo + vl];
}

pub fn get_binary_view_opt(buffers: [*]const [*]const u8, views: [*]const arr.BinaryView, validity: [*]const u8, index: u32) ?[]const u8 {
    return if (bitmap.get(validity, index))
        get_binary_view(buffers, views, index)
    else
        null;
}

pub fn get_fixed_size_binary(data: [*]const u8, byte_width: i32, index: u32) []const u8 {
    const bw = @as(u32, @bitCast(byte_width));
    const start = bw *% index;
    const end = start +% bw;
    return data[start..end];
}

pub fn get_fixed_size_binary_opt(data: [*]const u8, byte_width: i32, validity: [*]const u8, index: u32) ?[]const u8 {
    return if (bitmap.get(validity, index))
        get_fixed_size_binary(data, byte_width, index)
    else
        null;
}

pub fn get_list(comptime index_type: arr.IndexType, inner: *const arr.Array, offsets: [*]const index_type.to_type(), index: u32) arr.Array {
    const start: u32 = @intCast(index_cast(index_type, offsets[index]));
    const end: u32 = @intCast(index_cast(index_type, offsets[index +% 1]));
    return slice(inner, start, end -% start);
}

pub fn get_list_opt(comptime index_type: arr.IndexType, inner: *const arr.Array, offsets: [*]const index_type.to_type(), validity: [*]const u8, index: u32) ?arr.Array {
    return if (bitmap.get(validity, index))
        get_list(index_type, inner, offsets, index)
    else
        null;
}

pub fn get_list_view(comptime index_type: arr.IndexType, inner: *const arr.Array, offsets: [*]const index_type.to_type(), sizes: [*]const index_type.to_type(), index: u32) arr.Array {
    const start: u32 = @intCast(index_cast(index_type, offsets[index]));
    const size: u32 = @intCast(index_cast(index_type, sizes[index]));
    return slice(inner, start, size);
}

pub fn get_list_view_opt(comptime index_type: arr.IndexType, inner: *const arr.Array, offsets: [*]const index_type.to_type(), sizes: [*]const index_type.to_type(), validity: [*]const u8, index: u32) ?arr.Array {
    return if (bitmap.get(validity, index))
        get_list_view(index_type, inner, offsets, sizes, index)
    else
        null;
}

pub fn get_fixed_size_list(inner: *const arr.Array, item_width: i32, index: u32) arr.Array {
    const iw = @as(u32, @bitCast(item_width));
    const start = iw *% index;
    const end = start +% iw;
    return slice(inner, start, end);
}

pub fn get_fixed_size_list_opt(inner: *const arr.Array, item_width: i32, validity: [*]const u8, index: u32) ?arr.Array {
    return if (bitmap.get(validity, index))
        get_fixed_size_list(inner, item_width, index)
    else
        null;
}

pub fn get_map(entries: *const arr.StructArray, offsets: [*]const i32, index: u32) arr.StructArray {
    const start: u32 = @intCast(index_cast(.i32, offsets[index]));
    const end: u32 = @intCast(index_cast(.i32, offsets[index +% 1]));
    return slice_struct(entries, start, end -% start);
}

pub fn item_type(comptime ArrayT: type) type {
    return switch (ArrayT) {
        arr.NullArray => unreachable,
        arr.Int8Array => i8,
        arr.Int16Array => i16,
        arr.Int32Array => i32,
        arr.Int64Array => i64,
        arr.UInt8Array => u8,
        arr.UInt16Array => u16,
        arr.UInt32Array => u32,
        arr.UInt64Array => u64,
        arr.Float16Array => f16,
        arr.Float32Array => f32,
        arr.Float64Array => f64,
        arr.BinaryArray => []const u8,
        arr.LargeBinaryArray => []const u8,
        arr.Utf8Array => []const u8,
        arr.LargeUtf8Array => []const u8,
        arr.BoolArray => bool,
        arr.BinaryViewArray => []const u8,
        arr.Utf8ViewArray => []const u8,
        arr.Decimal32Array => i32,
        arr.Decimal64Array => i64,
        arr.Decimal128Array => i128,
        arr.Decimal256Array => i256,
        arr.FixedSizeBinaryArray => []const u8,
        arr.Date32Array => i32,
        arr.Date64Array => i64,
        arr.Time32Array => i32,
        arr.Time64Array => i64,
        arr.TimestampArray => i64,
        arr.DurationArray => i64,
        arr.IntervalYearMonthArray => i32,
        arr.IntervalDayTimeArray => [2]i32,
        arr.IntervalMonthDayNanoArray => arr.MonthDayNano,
        arr.ListArray => arr.Array,
        arr.LargeListArray => arr.Array,
        arr.ListViewArray => arr.Array,
        arr.LargeListViewArray => arr.Array,
        arr.FixedSizeListArray => arr.Array,
        arr.StructArray => unreachable,
        arr.MapArray => arr.StructArray,
        arr.DenseUnionArray => unreachable,
        arr.SparseUnionArray => unreachable,
        arr.RunEndArray => unreachable,
        arr.DictArray => unreachable,
        else => unreachable,
    };
}

pub fn get(comptime ArrayT: type, array: *const ArrayT, index: u32) item_type(ArrayT) {
    switch (ArrayT) {
        arr.Int8Array, arr.Int16Array, arr.Int32Array, arr.Int64Array, arr.UInt8Array, arr.UInt16Array, arr.UInt32Array, arr.UInt64Array, arr.Float16Array, arr.Float32Array, arr.Float64Array => {
            return get_primitive(item_type(ArrayT), array.values.ptr, index);
        },
        arr.Date32Array, arr.Date64Array, arr.Time32Array, arr.Time64Array, arr.TimestampArray, arr.DurationArray, arr.IntervalYearMonthArray, arr.IntervalDayTimeArray, arr.IntervalMonthDayNanoArray, arr.Decimal32Array, arr.Decimal64Array, arr.Decimal128Array, arr.Decimal256Array => {
            return get_primitive(item_type(ArrayT), array.inner.values.ptr, index);
        },
        arr.BoolArray => {
            return get_bool(array.values.ptr, index);
        },
        arr.BinaryArray => {
            return get_binary(.i32, array.data.ptr, array.offsets.ptr, index);
        },
        arr.LargeBinaryArray => {
            return get_binary(.i64, array.data.ptr, array.offsets.ptr, index);
        },
        arr.Utf8Array => {
            return get_binary(.i32, array.inner.data.ptr, array.inner.offsets.ptr, index);
        },
        arr.LargeUtf8Array => {
            return get_binary(.i64, array.inner.data.ptr, array.inner.offsets.ptr, index);
        },
        arr.FixedSizeBinaryArray => {
            return get_fixed_size_binary(array.data.ptr, array.byte_width, index);
        },
        arr.ListArray => {
            return get_list(.i32, array.inner, array.offsets.ptr, index);
        },
        arr.LargeListArray => {
            return get_list(.i64, array.inner, array.offsets.ptr, index);
        },
        arr.ListViewArray => {
            return get_list_view(.i32, array.inner, array.offsets.ptr, array.sizes.ptr, index);
        },
        arr.LargeListViewArray => {
            return get_list_view(.i64, array.inner, array.offsets.ptr, array.sizes.ptr, index);
        },
        arr.FixedSizeListArray => {
            return get_fixed_size_list(array.inner, array.item_width, index);
        },
        arr.StructArray => unreachable,
        arr.MapArray => {
            return get_map(array.entries, array.offsets.ptr, index);
        },
        arr.DenseUnionArray, arr.SparseUnionArray, arr.RunEndArray, arr.DictArray => unreachable,
        else => unreachable,
    }
}

pub fn get_opt(comptime ArrayT: type, array: *const ArrayT, validity: [*]const u8, index: u32) ?item_type(ArrayT) {
    switch (ArrayT) {
        arr.Int8Array, arr.Int16Array, arr.Int32Array, arr.Int64Array, arr.UInt8Array, arr.UInt16Array, arr.UInt32Array, arr.UInt64Array, arr.Float16Array, arr.Float32Array, arr.Float64Array => {
            return get_primitive_opt(item_type(ArrayT), array.values.ptr, validity, index);
        },
        arr.Date32Array, arr.Date64Array, arr.Time32Array, arr.Time64Array, arr.TimestampArray, arr.DurationArray, arr.IntervalYearMonthArray, arr.IntervalDayTimeArray, arr.IntervalMonthDayNanoArray, arr.Decimal32Array, arr.Decimal64Array, arr.Decimal128Array, arr.Decimal256Array => {
            return get_primitive_opt(item_type(ArrayT), array.inner.values.ptr, index);
        },
        arr.BoolArray => {
            return get_bool_opt(array.values.ptr, validity, index);
        },
        arr.BinaryArray => {
            return get_binary_opt(.i32, array.data.ptr, array.offsets.ptr, validity, index);
        },
        arr.LargeBinaryArray => {
            return get_binary_opt(.i64, array.data.ptr, array.offsets.ptr, validity, index);
        },
        arr.Utf8Array => {
            return get_binary_opt(.i32, array.inner.data.ptr, array.inner.offsets.ptr, validity, index);
        },
        arr.LargeUtf8Array => {
            return get_binary_opt(.i64, array.inner.data.ptr, array.inner.offsets.ptr, validity, index);
        },
        arr.FixedSizeBinaryArray => {
            return get_fixed_size_binary_opt(array.data.ptr, array.byte_width, validity, index);
        },
        arr.ListArray => {
            return get_list_opt(.i32, array.inner, array.offsets.ptr, validity, index);
        },
        arr.LargeListArray => {
            return get_list_opt(.i64, array.inner, array.offsets.ptr, validity, index);
        },
        arr.ListViewArray => {
            return get_list_view_opt(.i32, array.inner, array.offsets.ptr, array.sizes.ptr, validity, index);
        },
        arr.LargeListViewArray => {
            return get_list_view_opt(.i64, array.inner, array.offsets.ptr, array.sizes.ptr, validity, index);
        },
        arr.FixedSizeListArray => {
            return get_fixed_size_list_opt(array.inner, array.item_width, validity, index);
        },
        arr.StructArray => unreachable,
        arr.MapArray => {
            unreachable;
        },
        arr.DenseUnionArray, arr.SparseUnionArray, arr.RunEndArray, arr.DictArray => unreachable,
        else => unreachable,
    }
}

test "get primitive" {
    var array_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer array_arena.deinit();
    const array = try builder.UInt32Builder.from_slice_opt(&.{ 1, 2, 3, null, 69, 69 }, array_arena.allocator());

    try testing.expectEqual(3, get_opt(@TypeOf(array), &array, array.validity.?.ptr, 2));
    try testing.expectEqual(null, get_opt(@TypeOf(array), &array, array.validity.?.ptr, 3));
    try testing.expectEqual(69, get_opt(@TypeOf(array), &array, array.validity.?.ptr, 4));
}

test "get binary" {
    var array_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer array_arena.deinit();
    const array = try builder.BinaryBuilder.from_slice_opt(&.{ "1", "2", "3", null, "69", "69" }, array_arena.allocator());

    try testing.expectEqualDeep("3", get_opt(@TypeOf(array), &array, array.validity.?.ptr, 2));
    try testing.expectEqualDeep(null, get_opt(@TypeOf(array), &array, array.validity.?.ptr, 3));
    try testing.expectEqualDeep("69", get_opt(@TypeOf(array), &array, array.validity.?.ptr, 4));
}
