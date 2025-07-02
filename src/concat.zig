const std = @import("std");
const Allocator = std.mem.Allocator;
const arr = @import("./array.zig");
const bitmap = @import("./bitmap.zig");
const equals = @import("./equals.zig");
const testing = std.testing;
const ArenaAllocator = std.heap.ArenaAllocator;
const builder = @import("./builder.zig");

const Error = error{
    OutOfMemory,
};

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
pub fn concat_primitive(comptime T: type, arrays: []const arr.PrimitiveArray(T), alloc: Allocator) Error!arr.PrimitiveArray(T) {
    var total_len: u32 = 0;
    var total_null_count: u32 = 0;

    for (arrays) |array| {
        total_len += array.len;
        total_null_count += array.null_count;
    }

    const has_nulls = total_null_count > 0;
    const bitmap_len = (total_len + 7) / 8;

    const values = try alloc.alloc(T, total_len);
    const validity: []u8 = if (has_nulls) has_n: {
        const v = try alloc.alloc(u8, bitmap_len);
        @memset(v, 0xff);
        break :has_n v;
    } else &.{};

    var write_offset: u32 = 0;
    for (arrays) |array| {
        @memcpy(values.ptr[write_offset .. write_offset + array.len], array.values.ptr[array.offset .. array.offset + array.len]);

        if (has_nulls and array.null_count > 0) {
            const v = (array.validity orelse unreachable).ptr;

            var idx: u32 = array.offset;
            var w_idx: u32 = write_offset;
            while (idx < array.offset + array.len) : ({
                idx += 1;
                w_idx += 1;
            }) {
                if (!bitmap.get(v, idx)) {
                    bitmap.unset(validity.ptr, w_idx);
                }
            }
        }

        write_offset += array.len;
    }

    return .{
        .len = total_len,
        .null_count = total_null_count,
        .validity = if (has_nulls) validity else null,
        .values = values,
        .offset = 0,
    };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
pub fn concat_binary(comptime index_t: arr.IndexType, arrays: []const arr.GenericBinaryArray(index_t), alloc: Allocator) Error!arr.GenericBinaryArray(index_t) {
    const I = index_t.to_type();

    var total_len: u32 = 0;
    var total_null_count: u32 = 0;
    var total_data_len: u32 = 0;

    for (arrays) |array| {
        total_len += array.len;
        total_null_count += array.null_count;

        const start = array.offsets.ptr[array.offset];
        const end = array.offsets.ptr[array.offset + array.len];
        const data_len: u32 = @intCast(end - start);
        total_data_len += data_len;
    }

    const has_nulls = total_null_count > 0;
    const bitmap_len = (total_len + 7) / 8;

    const data = try alloc.alloc(u8, total_data_len);
    const validity: []u8 = if (has_nulls) has_n: {
        const v = try alloc.alloc(u8, bitmap_len);
        @memset(v, 0xff);
        break :has_n v;
    } else &.{};
    const offsets = try alloc.alloc(I, total_len + 1);
}

test "concat_primitive non-null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.Int32Builder.from_slice(&.{ 1, 2, 3 }, false, alloc);
    const arr1 = try builder.Int32Builder.from_slice(&.{ 4, 5, 6 }, false, alloc);
    const arr2 = try builder.Int32Builder.from_slice(&.{}, false, alloc);
    const arr3 = try builder.Int32Builder.from_slice(&.{ 7, 8, 9, 10 }, false, alloc);

    const result = try concat_primitive(i32, &.{ arr0, arr1, arr2, arr3 }, alloc);
    const expected = try builder.Int32Builder.from_slice(&.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, false, alloc);

    try equals.equals_primitive(i32, &result, &expected);
}

test "concat primitive nullable" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.Int32Builder.from_slice_opt(&.{ 1, 2, 3, null, null }, alloc);
    const arr1 = try builder.Int32Builder.from_slice(&.{ 4, 5, 6 }, false, alloc);
    const arr2 = try builder.Int32Builder.from_slice_opt(&.{null}, alloc);
    const arr3 = try builder.Int32Builder.from_slice(&.{ 7, 8, 9, 10 }, false, alloc);

    const result = try concat_primitive(i32, &.{ arr0, arr1, arr2, arr3 }, alloc);
    const expected = try builder.Int32Builder.from_slice_opt(&.{ 1, 2, 3, null, null, 4, 5, 6, null, 7, 8, 9, 10 }, alloc);

    try equals.equals_primitive(i32, &result, &expected);
}

test "concat_primitive empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.Int32Builder.from_slice_opt(&.{}, alloc);
    const arr1 = try builder.Int32Builder.from_slice(&.{}, false, alloc);
    const arr2 = try builder.Int32Builder.from_slice_opt(&.{}, alloc);
    const arr3 = try builder.Int32Builder.from_slice(&.{}, false, alloc);

    const result = try concat_primitive(i32, &.{ arr0, arr1, arr2, arr3 }, alloc);
    const expected = try builder.Int32Builder.from_slice_opt(&.{}, alloc);

    try equals.equals_primitive(i32, &result, &expected);
}
