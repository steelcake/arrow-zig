const std = @import("std");
const ArenaAllocator = std.heap.ArenaAllocator;
const testing = std.testing;

const arr = @import("./array.zig");
const bitmap = @import("./bitmap.zig");
const builder = @import("./builder.zig");
const slice = @import("./slice.zig").slice;
const get = @import("./get.zig");

const Error = error{NotEqual};

pub fn equals_null(l: *const arr.NullArray, r: *const arr.NullArray) Error!void {
    if (l.len != r.len) {
        return Error.NotEqual;
    }
}

pub fn equals_bool(l: *const arr.BoolArray, r: *const arr.BoolArray) Error!void {
    if (l.len != r.len or r.null_count != l.null_count) {
        return Error.NotEqual;
    }

    if (l.len == 0) {
        return;
    }

    if (l.null_count > 0) {
        const lv = l.validity orelse unreachable;
        const rv = r.validity orelse unreachable;

        var li: u32 = l.offset;
        var ri: u32 = r.offset;
        for (0..l.len) |_| {
            if (get.get_bool_opt(l.values.ptr, lv.ptr, li) != get.get_bool_opt(r.values.ptr, rv.ptr, ri)) {
                return Error.NotEqual;
            }

            li += 1;
            ri += 1;
        }
    } else {
        var li: u32 = l.offset;
        var ri: u32 = r.offset;
        for (0..l.len) |_| {
            if (get.get_bool(l.values.ptr, li) != get.get_bool(r.values.ptr, ri)) {
                return Error.NotEqual;
            }

            li += 1;
            ri += 1;
        }
    }
}

pub fn equals_primitive(comptime T: type, l: *const arr.PrimitiveArray(T), r: *const arr.PrimitiveArray(T)) Error!void {
    if (l.len != r.len or r.null_count != l.null_count) {
        return Error.NotEqual;
    }
    if (l.len == 0) {
        return;
    }
    if (l.null_count > 0) {
        const lv = l.validity orelse unreachable;
        const rv = r.validity orelse unreachable;

        var li: u32 = l.offset;
        var ri: u32 = r.offset;
        for (0..l.len) |_| {
            if (get.get_primitive_opt(T, l.values.ptr, lv.ptr, li) != get.get_primitive_opt(T, r.values.ptr, rv.ptr, ri)) {
                return Error.NotEqual;
            }

            li += 1;
            ri += 1;
        }
    } else {
        var li: u32 = l.offset;
        var ri: u32 = r.offset;
        for (0..l.len) |_| {
            if (get.get_primitive(T, l.values.ptr, li) != get.get_primitive(T, r.values.ptr, ri)) {
                return Error.NotEqual;
            }

            li += 1;
            ri += 1;
        }
    }
}

pub fn equals_binary(comptime index_type: arr.IndexType, l: *const arr.GenericBinaryArray(index_type), r: *const arr.GenericBinaryArray(index_type)) Error!void {
    if (l.len != r.len or r.null_count != l.null_count) {
        return Error.NotEqual;
    }
    if (l.len == 0) {
        return;
    }

    if (l.null_count > 0) {
        const lv = l.validity orelse unreachable;
        const rv = r.validity orelse unreachable;

        var li: u32 = l.offset;
        var ri: u32 = r.offset;
        for (0..l.len) |_| {
            if (get.get_binary_opt(index_type, l.data.ptr, l.offsets.ptr, lv.ptr, li) != get.get_binary_opt(index_type, r.data.ptr, r.offsets.ptr, rv.ptr, ri)) {
                return Error.NotEqual;
            }

            li += 1;
            ri += 1;
        }
    } else {
        var li: u32 = l.offset;
        var ri: u32 = r.offset;
        for (0..l.len) |_| {
            if (get.get_binary(index_type, l.values.ptr, l.offsets.ptr, li) != get.get_binary(index_type, r.values.ptr, r.offsets.ptr, ri)) {
                return Error.NotEqual;
            }

            li += 1;
            ri += 1;
        }
    }
}

pub fn equals_utf8(comptime index_type: arr.IndexType, l: *const arr.GenericUtf8Array(index_type), r: *const arr.GenericUtf8Array(index_type)) Error!void {
    try equals_binary(index_type, &l.inner, &r.inner);
}

pub fn equals_decimal(comptime int: arr.DecimalInt, left: *const arr.DecimalArray(int), right: *const arr.DecimalArray(int)) Error!void {
    if (left.params.scale != right.params.scale or left.params.precision != right.params.precision) {
        return Error.NotEqual;
    }

    try equals_primitive(int.to_type(), &left.inner, &right.inner);
}

pub fn equals_binary_view(l: *const arr.BinaryViewArray, r: *const arr.BinaryViewArray) Error!void {
    if (l.len != r.len or r.null_count != l.null_count) {
        return Error.NotEqual;
    }
    if (l.len == 0) {
        return;
    }

    if (l.null_count > 0) {
        const lv = l.validity orelse unreachable;
        const rv = r.validity orelse unreachable;

        var li: u32 = l.offset;
        var ri: u32 = r.offset;
        for (0..l.len) |_| {
            if (get.get_binary_view_opt(l.buffers.ptr, l.views.ptr, lv.ptr, li) != get.get_binary_view_opt(r.buffers.ptr, r.views.ptr, rv.ptr, ri)) {
                return Error.NotEqual;
            }

            li += 1;
            ri += 1;
        }
    } else {
        var li: u32 = l.offset;
        var ri: u32 = r.offset;
        for (0..l.len) |_| {
            if (get.get_binary_view(l.buffers.ptr, l.views.ptr, li) != get.get_binary_view(r.buffers.ptr, r.views.ptr, ri)) {
                return Error.NotEqual;
            }

            li += 1;
            ri += 1;
        }
    }
}

pub fn equals_utf8_view(l: *const arr.Utf8ViewArray, r: *const arr.Utf8ViewArray) Error!void {
    try equals_binary_view(&l.inner, &r.inner);
}

pub fn equals_fixed_size_binary(l: *const arr.FixedSizeBinaryArray, r: *const arr.FixedSizeBinaryArray) Error!void {
    if (l.len != r.len or r.null_count != l.null_count or r.byte_width != l.byte_width) {
        return false;
    }
    if (l.len == 0) {
        return;
    }

    if (l.null_count > 0) {
        const lv = l.validity orelse unreachable;
        const rv = r.validity orelse unreachable;

        var li: u32 = l.offset;
        var ri: u32 = r.offset;
        for (0..l.len) |_| {
            if (get.get_fixed_size_binary_opt(l.data.ptr, l.byte_width, lv.ptr, li) != get.get_binary_view_opt(r.data.ptr, r.byte_width, rv.ptr, ri)) {
                return Error.NotEqual;
            }

            li += 1;
            ri += 1;
        }
    } else {
        var li: u32 = l.offset;
        var ri: u32 = r.offset;
        for (0..l.len) |_| {
            if (get.get_fixed_size_binary(l.data.ptr, l.byte_width, li) != get.get_fixed_size_binary(r.data.ptr, r.byte_width, ri)) {
                return Error.NotEqual;
            }

            li += 1;
            ri += 1;
        }
    }
}

pub fn equals_date(comptime backing_t: arr.IndexType, l: *const arr.DateArray(backing_t), r: *const arr.DateArray(backing_t)) Error!void {
    try equals_primitive(backing_t.to_type(), &l.inner, &r.inner);
}

pub fn equals_time(comptime backing_t: arr.IndexType, l: *const arr.TimeArray(backing_t), r: *const arr.TimeArray(backing_t)) Error!void {
    if (l.unit != r.unit) {
        return Error.NotEqual;
    }

    try equals_primitive(backing_t.to_type(), &l.inner, &r.inner);
}

pub fn equals_timestamp(l: *const arr.TimestampArray, r: *const arr.TimestampArray) Error!void {
    if (l.ts.timezone) |ltz| {
        if (r.ts.timezone) |rtz| {
            if (!std.mem.eql(u8, ltz, rtz)) {
                return Error.NotEqual;
            }
        } else {
            return Error.NotEqual;
        }
    } else if (r.ts.timezone != null) {
        return Error.NotEqual;
    }

    if (l.ts.unit != r.ts.unit) {
        return Error.NotEqual;
    }

    try equals_primitive(i64, &l.inner, &r.inner);
}

pub fn equals_duration(l: *const arr.DurationArray, r: *const arr.DurationArray) Error!void {
    if (l.unit != r.unit) {
        return Error.NotEqual;
    }
    try equals_primitive(i64, &l.inner, &r.inner);
}

pub fn equals_interval(comptime interval_t: arr.IntervalType, l: *const arr.IntervalArray(interval_t), r: *const arr.IntervalArray(interval_t)) Error!void {
    try equals_primitive(interval_t.to_type(), &l.inner, &r.inner);
}

pub fn equals_list(comptime index_type: arr.IndexType, l: *const arr.GenericListArray(index_type), r: *const arr.GenericListArray(index_type)) Error!void {
    if (l.len != r.len or r.null_count != l.null_count) {
        return Error.NotEqual;
    }
    if (l.len == 0) {
        return;
    }

    if (l.null_count > 0) {
        const lv = l.validity orelse unreachable;
        const rv = r.validity orelse unreachable;

        var li: u32 = l.offset;
        var ri: u32 = r.offset;
        for (0..l.len) |_| {
            const l_inner = get.get_list_opt(index_type, &l.inner, l.offsets.ptr, lv.ptr, li);
            const r_inner = get.get_list_opt(index_type, &r.inner, r.offsets.ptr, rv.ptr, ri);

            if (l_inner) |linner| {
                const rinner = r_inner orelse return Error.NotEqual;
                try equals(&linner, &rinner);
            } else if (r_inner != null) {
                return Error.NotEqual;
            }

            li += 1;
            ri += 1;
        }
    } else {
        var li: u32 = l.offset;
        var ri: u32 = r.offset;
        for (0..l.len) |_| {
            const l_inner = get.get_list(index_type, &l.inner, l.offsets.ptr, li);
            const r_inner = get.get_list(index_type, &r.inner, r.offsets.ptr, ri);
            try equals(&l_inner, &r_inner);

            li += 1;
            ri += 1;
        }
    }
}

pub fn equals_list_view(comptime index_type: arr.IndexType, l: *const arr.GenericListViewArray(index_type), r: *const arr.GenericListViewArray(index_type)) Error!void {
    if (l.len != r.len or r.null_count != l.null_count) {
        return Error.NotEqual;
    }
    if (l.len == 0) {
        return;
    }

    if (l.null_count > 0) {
        const lv = l.validity orelse unreachable;
        const rv = r.validity orelse unreachable;

        var li: u32 = l.offset;
        var ri: u32 = r.offset;
        for (0..l.len) |_| {
            const l_inner = get.get_list_view_opt(index_type, &l.inner, l.offsets.ptr, l.sizes.ptr, lv.ptr, li);
            const r_inner = get.get_list_view_opt(index_type, &r.inner, r.offsets.ptr, r.sizes.ptr, rv.ptr, ri);

            if (l_inner) |linner| {
                const rinner = r_inner orelse return Error.NotEqual;
                try equals(&linner, &rinner);
            } else if (r_inner != null) {
                return Error.NotEqual;
            }

            li += 1;
            ri += 1;
        }
    } else {
        var li: u32 = l.offset;
        var ri: u32 = r.offset;
        for (0..l.len) |_| {
            const l_inner = get.get_list_view(index_type, &l.inner, l.offsets.ptr, l.sizes.ptr, li);
            const r_inner = get.get_list_view(index_type, &r.inner, r.offsets.ptr, r.sizes.ptr, ri);
            try equals(&l_inner, &r_inner);

            li += 1;
            ri += 1;
        }
    }
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
pub fn equals(left: *const arr.Array, right: *const arr.Array) Error!void {
    if (@intFromEnum(left.*) != @intFromEnum(right.*)) {
        return Error.NotEqual;
    }

    switch (left.*) {
        .null => |*l| try equals_null(l, &right.null),
        .i8 => |*l| try equals_primitive(i8, l, &right.i8),
        .i16 => |*l| try equals_primitive(i16, l, &right.i16),
        .i32 => |*l| try equals_primitive(i32, l, &right.i32),
        .i64 => |*l| try equals_primitive(i64, l, &right.i64),
        .u8 => |*l| try equals_primitive(u8, l, &right.u8),
        .u16 => |*l| try equals_primitive(u16, l, &right.u16),
        .u32 => |*l| try equals_primitive(u32, l, &right.u32),
        .u64 => |*l| try equals_primitive(u64, l, &right.u64),
        .f16 => |*l| try equals_primitive(f16, l, &right.f16),
        .f32 => |*l| try equals_primitive(f32, l, &right.f32),
        .f64 => |*l| try equals_primitive(f64, l, &right.f64),
        .binary => |*l| try equals_binary(.i32, l, &right.binary),
        .large_binary => |*l| try equals_binary(.i64, l, &right.large_binary),
        .utf8 => |*l| try equals_utf8(.i32, l, &right.utf8),
        .large_utf8 => |*l| try equals_utf8(.i64, l, &right.large_utf8),
        .bool => |*l| try equals_bool(l, &right.bool),
        .binary_view => |*l| try equals_binary_view(l, &right.binary_view),
        .utf8_view => |*l| try equals_utf8_view(l, &right.utf8_view),
        .decimal32 => |*l| try equals_decimal(.i32, l, &right.decimal32),
        .decimal64 => |*l| try equals_decimal(.i64, l, &right.decimal64),
        .decimal128 => |*l| try equals_decimal(.i128, l, &right.decimal128),
        .decimal256 => |*l| try equals_decimal(.i256, l, &right.decimal256),
        .fixed_size_binary => |*l| try equals_fixed_size_binary(l, &right.fixed_size_binary),
        .date32 => |*l| try equals_date(.i32, l, &right.date32),
        .date64 => |*l| try equals_date(.i64, l, &right.date64),
        .time32 => |*l| try equals_time(.i32, l, &right.time32),
        .time64 => |*l| try equals_time(.i64, l, &right.time64),
        .timestamp => |*l| try equals_timestamp(l, &right.timestamp),
        .duration => |*l| try equals_duration(l, &right.duration),
        .interval_year_month => |*l| try equals_interval(.year_month, l, &right.interval_year_month),
        .interval_day_time => |*l| try equals_interval(.day_time, l, &right.interval_day_time),
        .interval_month_day_nano => |*l| try equals_interval(.month_day_nano, l, &right.interval_month_day_nano),
        .list => |*l| try equals_list(.i32, l, &right.list),
        .large_list => |*l| try equals_list(.i64, l, &right.large_list),
        else => unreachable,
    }
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

    try equals(&l_array, &slice(&r_array, 1, 4));
}
