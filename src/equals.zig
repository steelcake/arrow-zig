const std = @import("std");
const ArenaAllocator = std.heap.ArenaAllocator;
const testing = std.testing;

const arr = @import("./array.zig");
const bitmap = @import("./bitmap.zig");
const builder = @import("./builder.zig");
const slice = @import("./slice.zig").slice;

pub fn equals_bitmap(left_offset: u32, left_len: u32, left: []const u8, right_offset: u32, right_len: u32, right: []const u8) bool {
    if (left_len != right_len) {
        return false;
    }

    var li: u32 = left_offset;
    var ri: u32 = right_offset;
    for (0..left_len) |_| {
        if (bitmap.get(left.ptr, li) != bitmap.get(right.ptr, ri)) {
            return false;
        }
        li += 1;
        ri += 1;
    }

    return true;
}

pub fn equals_bool(l: *const arr.BoolArray, r: *const arr.BoolArray) bool {
    if (l.len != r.len or r.null_count != l.null_count) {
        return false;
    }

    if (l.null_count > 0) {
        const l_validity = l.validity orelse unreachable;
        const r_validity = r.validity orelse unreachable;

        if (!equals_bitmap(l.offset, l.len, l_validity, r.offset, r.len, r_validity)) {
            return false;
        }
    }

    return equals_bitmap(l.offset, l.len, l.values, r.offset, r.len, r.values);
}

pub fn equals_primitive(comptime T: type, l: *const arr.PrimitiveArray(T), r: *const arr.PrimitiveArray(T)) bool {
    if (l.len != r.len or r.null_count != l.null_count) {
        return false;
    }

    if (l.null_count > 0) {
        const l_validity = l.validity orelse unreachable;
        const r_validity = r.validity orelse unreachable;

        if (!equals_bitmap(l.offset, l.len, l_validity, r.offset, r.len, r_validity)) {
            return false;
        }
    }

    var li: u32 = l.offset;
    var ri: u32 = r.offset;
    for (0..l.len) |_| {
        if (l.values.ptr[li] != r.values.ptr[ri]) {
            return false;
        }

        li += 1;
        ri += 1;
    }

    return true;
}

pub fn equals_binary(comptime index_type: arr.IndexType, l: *const arr.GenericBinaryArray(index_type), r: *const arr.GenericBinaryArray(index_type)) bool {
    if (l.len != r.len or r.null_count != l.null_count) {
        return false;
    }

    if (l.null_count > 0) {
        const l_validity = l.validity orelse unreachable;
        const r_validity = r.validity orelse unreachable;

        if (!equals_bitmap(l.offset, l.len, l_validity, r.offset, r.len, r_validity)) {
            return false;
        }
    }

    if (l.len == 0) {
        return true;
    }

    const l_start: usize = @intCast(l.offsets[l.offset]);
    const l_end: usize = @intCast(l.offsets[l.offset + l.len]);
    const r_start: usize = @intCast(r.offsets[r.offset]);
    const r_end: usize = @intCast(r.offsets[r.offset + r.len]);
    if (!std.mem.eql(u8, l.data[l_start..l_end], r.data[r_start..r_end])) {
        return false;
    }

    return std.mem.eql(index_type.to_type(), l.offsets[l.offset .. l.offset + l.len], r.offsets[r.offset .. r.offset + r.len]);
}

pub fn equals_utf8(comptime index_type: arr.IndexType, l: *const arr.GenericUtf8Array(index_type), r: *const arr.GenericUtf8Array(index_type)) bool {
    return equals_binary(index_type, &l.inner, &r.inner);
}

pub fn equals_decimal(comptime int: arr.DecimalInt, left: *const arr.DecimalArray(int), right: *const arr.DecimalArray(int)) bool {
    if (left.params.scale != right.params.scale or left.params.precision != right.params.precision) {
        return false;
    }

    return equals_primitive(int.to_type(), &left.inner, &right.inner);
}

// pub fn equals_binary_view(l: *const arr.BinaryViewArray, r: *const arr.BinaryViewArray) bool {

// }

/// Checks if two arrays are logically equal.
///
/// Two arrays are logically equal iff:
///  - their data types are equal
///  - their items are equal
///  - their validity are equal
///
/// For example two arrays can be equal even if their value buffers are different but
/// their offset/len point to the same set of items.
///
/// array1:
/// values: [1, 2, 3],
/// offset: 0,
/// len: 3,
///
/// array2:
/// values: [1, 1, 2, 3],
/// offset: 1,
/// len: 3,
pub fn equals(left: *const arr.Array, right: *const arr.Array) bool {
    if (@intFromEnum(left.*) != @intFromEnum(right.*)) {
        return false;
    }

    return switch (left.*) {
        .null => |*l| l.len == right.null.len,
        .i8 => |*l| equals_primitive(i8, l, &right.i8),
        .i16 => |*l| equals_primitive(i16, l, &right.i16),
        .i32 => |*l| equals_primitive(i32, l, &right.i32),
        .i64 => |*l| equals_primitive(i64, l, &right.i64),
        .u8 => |*l| equals_primitive(u8, l, &right.u8),
        .u16 => |*l| equals_primitive(u16, l, &right.u16),
        .u32 => |*l| equals_primitive(u32, l, &right.u32),
        .u64 => |*l| equals_primitive(u64, l, &right.u64),
        .f16 => |*l| equals_primitive(f16, l, &right.f16),
        .f32 => |*l| equals_primitive(f32, l, &right.f32),
        .f64 => |*l| equals_primitive(f64, l, &right.f64),
        .binary => |*l| equals_binary(.i32, l, &right.binary),
        .large_binary => |*l| equals_binary(.i64, l, &right.large_binary),
        .utf8 => |*l| equals_utf8(.i32, l, &right.utf8),
        .large_utf8 => |*l| equals_utf8(.i64, l, &right.large_utf8),
        .bool => |*l| equals_bool(l, &right.bool),
        .decimal32 => |*l| equals_decimal(.i32, l, &right.decimal32),
        .decimal64 => |*l| equals_decimal(.i64, l, &right.decimal64),
        .decimal128 => |*l| equals_decimal(.i128, l, &right.decimal128),
        .decimal256 => |*l| equals_decimal(.i256, l, &right.decimal256),
        else => unreachable,
    };
}

test equals {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var left = try builder.Int16Builder.with_capacity(4, true, allocator);

    try left.append_null();
    try left.append_value(69);
    try left.append_option(-69);
    try left.append_null();

    const l_array = arr.Array{ .i16 = try left.finish() };

    var right = try builder.Int16Builder.with_capacity(5, true, allocator);

    try right.append_value(1131);
    try right.append_null();
    try right.append_value(69);
    try right.append_option(-69);
    try right.append_null();

    const r_array = arr.Array{ .i16 = try right.finish() };

    try testing.expect(equals(&l_array, &slice(&r_array, 1, 4)));
}
