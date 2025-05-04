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

    return std.mem.eql(T, l.values[l.offset .. l.offset + l.len], r.values[r.offset .. r.offset + r.len]);
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

    // check the data buffer is the same
    const l_start: usize = @intCast(l.offsets[l.offset]);
    const l_end: usize = @intCast(l.offsets[l.offset + l.len]);
    const r_start: usize = @intCast(r.offsets[r.offset]);
    const r_end: usize = @intCast(r.offsets[r.offset + r.len]);
    if (!std.mem.eql(u8, l.data[l_start..l_end], r.data[r_start..r_end])) {
        return false;
    }

    // check lengths of individual strings are same
    var li = l.offset;
    var ri = r.offset;
    for (0..l.len) |_| {
        if (l.offsets.ptr[li + 1] - l.offsets.ptr[li] != r.offsets.ptr[ri + 1] - r.offsets.ptr[ri]) {
            return false;
        }

        li += 1;
        ri += 1;
    }

    return true;
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

pub fn equals_binary_view(l: *const arr.BinaryViewArray, r: *const arr.BinaryViewArray) bool {
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

    var li: u32 = l.offset;
    var ri: u32 = r.offset;
    for (0..l.len) |_| {
        const lw = l.views.ptr[li];
        const rw = r.views.ptr[ri];

        if (lw.length != rw.length or lw.prefix != rw.prefix) {
            return false;
        }

        if (lw.length <= 12) {
            // compare u32s here but we are really comparing the string contents
            if (lw.buffer_idx != rw.buffer_idx or lw.offset != rw.offset) {
                return false;
            }
        } else {
            const lwl = l.buffers.ptr[lw.buffer_idx][lw.offset .. lw.offset + lw.length];
            const rwl = r.buffers.ptr[rw.buffer_idx][rw.offset .. rw.offset + rw.length];

            if (!std.mem.eql(u8, lwl, rwl)) {
                return false;
            }
        }

        li += 1;
        ri += 1;
    }

    return true;
}

pub fn equals_utf8_view(l: *const arr.Utf8ViewArray, r: *const arr.Utf8ViewArray) bool {
    return equals_binary_view(&l.inner, &r.inner);
}

pub fn equals_fixed_size_binary(l: *const arr.FixedSizeBinaryArray, r: *const arr.FixedSizeBinaryArray) bool {
    if (l.len != r.len or r.null_count != l.null_count or r.byte_width != l.byte_width) {
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

    const l_start: usize = @intCast(l.byte_width * l.offset);
    const l_end: usize = @intCast(l.byte_width * (l.offset + l.len));
    const r_start: usize = @intCast(r.byte_width * r.offset);
    const r_end: usize = @intCast(r.byte_width * (r.offset + r.len));
    return std.mem.eql(u8, l.data[l_start..l_end], r.data[r_start..r_end]);
}

pub fn equals_date(comptime backing_t: arr.IndexType, l: *const arr.DateArray(backing_t), r: *const arr.DateArray(backing_t)) bool {
    return equals_primitive(backing_t.to_type(), &l.inner, &r.inner);
}

pub fn equals_time(comptime backing_t: arr.IndexType, l: *const arr.TimeArray(backing_t), r: *const arr.TimeArray(backing_t)) bool {
    return l.unit == r.unit and equals_primitive(backing_t.to_type(), &l.inner, &r.inner);
}

pub fn equals_timestamp(l: *const arr.TimestampArray, r: *const arr.TimestampArray) bool {
    if (l.ts.timezone) |ltz| {
        if (r.ts.timezone) |rtz| {
            if (!std.mem.eql(u8, ltz, rtz)) {
                return false;
            }
        } else {
            return false;
        }
    } else if (r.ts.timezone != null) {
        return false;
    }

    return l.ts.unit == r.ts.unit and equals_primitive(i64, &l.inner, &r.inner);
}

pub fn equals_duration(l: *const arr.DurationArray, r: *const arr.DurationArray) bool {
    return l.unit == r.unit and equals_primitive(i64, &l.inner, &r.inner);
}

pub fn equals_interval(comptime interval_t: arr.IntervalType, l: *const arr.IntervalArray(interval_t), r: *const arr.IntervalArray(interval_t)) bool {
    return equals_primitive(interval_t.to_type(), &l.inner, &r.inner);
}

pub fn equals_list(comptime index_type: arr.IndexType, l: *const arr.GenericListArray(index_type), r: *const arr.GenericListArray(index_type)) bool {
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

    // compare the inner arrays
    const l_start: u32 = @intCast(l.offsets[l.offset]);
    const l_end: u32 = @intCast(l.offsets[l.offset + l.len]);
    const r_start: u32 = @intCast(r.offsets[r.offset]);
    const r_end: u32 = @intCast(r.offsets[r.offset + r.len]);
    if (!equals(&slice(l.inner, l_start, l_end - l_start), &slice(r.inner, r_start, r_end - r_start))) {
        return false;
    }

    // check lengths of individual items are same
    var li = l.offset;
    var ri = r.offset;
    for (0..l.len) |_| {
        if (l.offsets.ptr[li + 1] - l.offsets.ptr[li] != r.offsets.ptr[ri + 1] - r.offsets.ptr[ri]) {
            return false;
        }

        li += 1;
        ri += 1;
    }

    return true;
}

pub fn equals_list_view(comptime index_type: arr.IndexType, l: *const arr.GenericListViewArray(index_type), r: *const arr.GenericListViewArray(index_type)) bool {
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

    // compare the inner arrays
    const l_start: u32 = @intCast(l.offsets[l.offset]);
    const l_end: u32 = @intCast(l.offsets[l.offset + l.len]);
    const r_start: u32 = @intCast(r.offsets[r.offset]);
    const r_end: u32 = @intCast(r.offsets[r.offset + r.len]);
    if (!equals(&slice(l.inner, l_start, l_end - l_start), &slice(r.inner, r_start, r_end - r_start))) {
        return false;
    }

    // check lengths of individual items are same
    var li = l.offset;
    var ri = r.offset;
    for (0..l.len) |_| {
        if (l.offsets.ptr[li + 1] - l.offsets.ptr[li] != r.offsets.ptr[ri + 1] - r.offsets.ptr[ri]) {
            return false;
        }

        li += 1;
        ri += 1;
    }

    return true;
}

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
        .binary_view => |*l| equals_binary_view(l, &right.binary_view),
        .utf8_view => |*l| equals_utf8_view(l, &right.utf8_view),
        .decimal32 => |*l| equals_decimal(.i32, l, &right.decimal32),
        .decimal64 => |*l| equals_decimal(.i64, l, &right.decimal64),
        .decimal128 => |*l| equals_decimal(.i128, l, &right.decimal128),
        .decimal256 => |*l| equals_decimal(.i256, l, &right.decimal256),
        .fixed_size_binary => |*l| equals_fixed_size_binary(l, &right.fixed_size_binary),
        .date32 => |*l| equals_date(.i32, l, &right.date32),
        .date64 => |*l| equals_date(.i64, l, &right.date64),
        .time32 => |*l| equals_time(.i32, l, &right.time32),
        .time64 => |*l| equals_time(.i64, l, &right.time64),
        .timestamp => |*l| equals_timestamp(l, &right.timestamp),
        .duration => |*l| equals_duration(l, &right.duration),
        .interval_year_month => |*l| equals_interval(.year_month, l, &right.interval_year_month),
        .interval_day_time => |*l| equals_interval(.day_time, l, &right.interval_day_time),
        .interval_month_day_nano => |*l| equals_interval(.month_day_nano, l, &right.interval_month_day_nano),
        .list => |*l| equals_list(.i32, l, &right.list),
        .large_list => |*l| equals_list(.i64, l, &right.large_list),
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
