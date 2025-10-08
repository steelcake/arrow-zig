const std = @import("std");
const arr = @import("./array.zig");
const length = @import("./length.zig");
const bitmap = @import("./bitmap.zig");
const slice = @import("./slice.zig");
const get = @import("./get.zig");

const Error = error{
    Invalid,
};

fn validate_validity(null_count: u32, offset: u32, len: u32, validity: ?[]const u8) Error!void {
    if (@as(u64, offset) + @as(u64, len) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    const total_len = offset + len;
    const needed_bitmap_len = (total_len + 7) / 8;

    if (null_count > 0) {
        const v = validity orelse return Error.Invalid;

        if (needed_bitmap_len > v.len) {
            return Error.Invalid;
        }

        var count: u32 = 0;
        var idx: u32 = offset;
        while (idx < offset + len) : (idx += 1) {
            if (!bitmap.get(v.ptr, idx)) {
                count += 1;
            }
        }

        if (null_count != count) {
            return Error.Invalid;
        }
    } else if (validity) |v| {
        var idx: u32 = offset;
        while (idx < offset + len) : (idx += 1) {
            if (!bitmap.get(v.ptr, idx)) {
                return Error.Invalid;
            }
        }
    }
}

pub fn validate_primitive(comptime T: type, array: *const arr.PrimitiveArray(T)) Error!void {
    if (@as(u64, array.offset) + @as(u64, array.len) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    const needed_len = array.offset + array.len;
    if (needed_len > array.values.len) {
        return Error.Invalid;
    }

    try validate_validity(array.null_count, array.offset, array.len, array.validity);
}

pub fn validate_bool(array: *const arr.BoolArray) Error!void {
    if (@as(u64, array.offset) + @as(u64, array.len) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    const needed_bitmap_len = (array.offset + array.len + 7) / 8;
    if (needed_bitmap_len > array.values.len) {
        return Error.Invalid;
    }

    try validate_validity(array.null_count, array.offset, array.len, array.validity);
}

pub fn validate_binary(comptime index_t: arr.IndexType, array: *const arr.GenericBinaryArray(index_t)) Error!void {
    if (@as(u64, array.offset) + @as(u64, array.len) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    const needed_len = array.offset + array.len;
    if (needed_len + 1 > array.offsets.len) {
        return Error.Invalid;
    }

    try validate_validity(array.null_count, array.offset, array.len, array.validity);

    var idx: u32 = array.offset;
    while (idx < array.len + array.offset) : (idx += 1) {
        const start = array.offsets[idx];
        const end = array.offsets[idx + 1];

        if (start < 0 or end < 0) {
            return Error.Invalid;
        }

        if (start > end) {
            return Error.Invalid;
        }
    }

    if (array.data.len < array.offsets[array.offset + array.len]) {
        return Error.Invalid;
    }
}

pub fn validate_decimal(comptime decimal_t: arr.DecimalInt, array: *const arr.DecimalArray(decimal_t)) Error!void {
    const max_precision = switch (decimal_t) {
        .i32 => 9,
        .i64 => 19,
        .i128 => 38,
        .i256 => 76,
    };

    if (array.params.precision == 0) {
        return Error.Invalid;
    }

    if (array.params.precision > max_precision) {
        return Error.Invalid;
    }

    try validate_primitive(decimal_t.to_type(), &array.inner);
}

pub fn validate_list(comptime index_t: arr.IndexType, array: *const arr.GenericListArray(index_t)) Error!void {
    if (@as(u64, array.offset) + @as(u64, array.len) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    const needed_len = array.offset + array.len;
    if (needed_len + 1 > array.offsets.len) {
        return Error.Invalid;
    }

    try validate_validity(array.null_count, array.offset, array.len, array.validity);

    var idx: u32 = array.offset;
    while (idx < array.len + array.offset) : (idx += 1) {
        const start = array.offsets[idx];
        const end = array.offsets[idx + 1];

        if (start < 0 or end < 0) {
            return Error.Invalid;
        }

        if (start > end) {
            return Error.Invalid;
        }
    }

    if (length.length(array.inner) < array.offsets[array.offset + array.len]) {
        return Error.Invalid;
    }

    try validate(array.inner);
}

fn validate_field_names(field_names: []const []const u8) Error!void {
    for (field_names, 0..) |field_name, field_idx| {
        if (field_name.len == 0) {
            return Error.Invalid;
        }

        for (field_names[0..field_idx]) |other_name| {
            if (std.mem.eql(u8, field_name, other_name)) {
                return Error.Invalid;
            }
        }
    }
}

pub fn validate_struct(array: *const arr.StructArray) Error!void {
    if (@as(u64, array.offset) + @as(u64, array.len) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    try validate_validity(array.null_count, array.offset, array.len, array.validity);

    if (array.field_names.len == 0) {
        return Error.Invalid;
    }

    if (array.field_names.len != array.field_values.len) {
        return Error.Invalid;
    }

    try validate_field_names(array.field_names);

    for (array.field_values) |*child| {
        if (array.offset + array.len > length.length(child)) {
            return Error.Invalid;
        }

        try validate(child);
    }
}

fn validate_union(array: *const arr.UnionArray) Error!void {
    if (@as(u64, array.offset) + @as(u64, array.len) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    if (array.field_names.len == 0) {
        return Error.Invalid;
    }

    const needed_len = array.offset + array.len;
    if (needed_len > array.type_ids.len) {
        return Error.Invalid;
    }

    if (array.type_id_set.len != array.children.len) {
        return Error.Invalid;
    }

    if (array.field_names.len != array.children.len) {
        return Error.Invalid;
    }

    try validate_field_names(array.field_names);

    for (array.type_id_set, 0..) |tid, idx| {
        for (array.type_id_set[idx + 1 ..]) |other_tid| {
            if (tid == other_tid) {
                return Error.Invalid;
            }
        }
    }

    for (array.children) |*child| {
        try validate(child);
    }
}

pub fn validate_dense_union(array: *const arr.DenseUnionArray) Error!void {
    try validate_union(&array.inner);

    const needed_len = array.inner.offset + array.inner.len;
    if (needed_len > array.offsets.len) {
        return Error.Invalid;
    }

    var idx: u32 = array.inner.offset;
    while (idx < array.inner.offset + array.inner.len) : (idx += 1) {
        const type_id = array.inner.type_ids[idx];
        const offset = array.offsets[idx];

        const child_idx = for (array.inner.type_id_set, 0..) |tid, cidx| {
            if (tid == type_id) {
                break cidx;
            }
        } else return Error.Invalid;

        if (child_idx >= array.inner.children.len) {
            return Error.Invalid;
        }

        const child_len = length.length(&array.inner.children[child_idx]);

        if (offset >= child_len) {
            return Error.Invalid;
        }
    }
}

pub fn validate_sparse_union(array: *const arr.SparseUnionArray) Error!void {
    try validate_union(&array.inner);

    const needed_len = array.inner.len + array.inner.offset;
    for (array.inner.children) |*child| {
        if (needed_len > length.length(child)) {
            return Error.Invalid;
        }
    }
}

pub fn validate_fixed_size_binary(array: *const arr.FixedSizeBinaryArray) Error!void {
    if (@as(u64, array.offset) + @as(u64, array.len) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    const needed_len = array.offset + array.len;

    if (array.byte_width <= 0) {
        return Error.Invalid;
    }

    const byte_width: u32 = @intCast(array.byte_width);

    if (@as(u64, needed_len) * @as(u64, byte_width) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    if (needed_len * byte_width > array.data.len) {
        return Error.Invalid;
    }

    try validate_validity(array.null_count, array.offset, array.len, array.validity);
}

pub fn validate_fixed_size_list(array: *const arr.FixedSizeListArray) Error!void {
    if (@as(u64, array.offset) + @as(u64, array.len) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    const needed_len = array.offset + array.len;

    if (array.item_width <= 0) {
        return Error.Invalid;
    }

    const item_width: u32 = @intCast(array.item_width);

    if (@as(u64, needed_len) * @as(u64, item_width) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    if (needed_len * item_width > length.length(array.inner)) {
        return Error.Invalid;
    }

    try validate_validity(array.null_count, array.offset, array.len, array.validity);

    try validate(array.inner);
}

pub fn validate_map(array: *const arr.MapArray) Error!void {
    if (@as(u64, array.offset) + @as(u64, array.len) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    const needed_len = array.offset + array.len;
    if (needed_len + 1 > array.offsets.len) {
        return Error.Invalid;
    }

    try validate_validity(array.null_count, array.offset, array.len, array.validity);

    var idx: u32 = array.offset;
    while (idx < array.len + array.offset) : (idx += 1) {
        const start = array.offsets[idx];
        const end = array.offsets[idx + 1];

        if (start < 0 or end < 0) {
            return Error.Invalid;
        }

        if (start > end) {
            return Error.Invalid;
        }
    }

    if (array.offsets[array.offset + array.len] > array.entries.len) {
        return Error.Invalid;
    }

    if (array.entries.field_names.len != 2 or
        !std.mem.eql(u8, array.entries.field_names[0], "keys") or
        !std.mem.eql(u8, array.entries.field_names[1], "values"))
    {
        return Error.Invalid;
    }

    switch (array.entries.field_values[0]) {
        .i8, .i16, .i32, .i64, .u8, .u16, .u32, .u64, .binary, .large_binary, .utf8, .large_utf8, .binary_view, .utf8_view, .fixed_size_binary => {},
        else => return Error.Invalid,
    }

    try validate_struct(array.entries);

    // if (array.keys_are_sorted and !is_sorted.is_sorted(.ascending, &slice.slice(&array.entries.field_values[0], array.offset, array.len))) {
    //     return Error.Invalid;
    // }
}

fn validate_run_ends(comptime T: type, array: *const arr.RunEndArray, run_ends: *const arr.PrimitiveArray(T)) Error!void {
    try validate_primitive(T, run_ends);

    if (run_ends.null_count > 0) {
        return Error.Invalid;
    }

    if (run_ends.len == 0) return;

    const last_end: u64 = @intCast(run_ends.values[run_ends.offset + run_ends.len - 1]);
    if (@as(u64, array.len + array.offset) > last_end) {
        return Error.Invalid;
    }

    var idx: u32 = run_ends.offset + 1;
    while (idx < run_ends.offset + run_ends.len) : (idx += 1) {
        const re = run_ends.values.ptr[idx];

        if (@as(i64, re) > @as(i64, @intCast(std.math.maxInt(u32)))) {
            return Error.Invalid;
        }

        const prev = run_ends.values.ptr[idx - 1];
        if (re < prev) {
            return Error.Invalid;
        }
    }
}

pub fn validate_run_end_encoded(array: *const arr.RunEndArray) Error!void {
    if (@as(u64, array.offset) + @as(u64, array.len) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    const run_ends_len = length.length(array.run_ends);
    const values_len = length.length(array.values);

    // This is conservative but it is a b*** to implement it properly
    if (run_ends_len != values_len) {
        return Error.Invalid;
    }

    try validate(array.values);

    switch (array.run_ends.*) {
        .i16 => |*run_ends| try validate_run_ends(i16, array, run_ends),
        .i32 => |*run_ends| try validate_run_ends(i32, array, run_ends),
        .i64 => |*run_ends| try validate_run_ends(i64, array, run_ends),
        else => {
            return Error.Invalid;
        },
    }
}

fn validate_dict_keys(comptime T: type, keys: *const arr.PrimitiveArray(T), values_len: u32) Error!void {
    if (keys.null_count > 0) {
        const v = (keys.validity orelse unreachable).ptr;
        var idx: u32 = keys.offset;
        while (idx < keys.offset + keys.len) : (idx += 1) {
            if (get.get_primitive_opt(T, keys.values.ptr, v, idx)) |key| {
                if (key >= values_len) {
                    return Error.Invalid;
                }
            }
        }
    } else {
        var idx: u32 = keys.offset;
        while (idx < keys.offset + keys.len) : (idx += 1) {
            if (keys.values[idx] >= values_len) {
                return Error.Invalid;
            }
        }
    }
}

pub fn validate_dict(array: *const arr.DictArray) Error!void {
    if (@as(u64, array.offset) + @as(u64, array.len) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    const needed_len = array.len + array.offset;
    const keys_len = length.length(array.keys);

    if (needed_len > keys_len) {
        return Error.Invalid;
    }

    try validate(array.keys);
    try validate(array.values);

    const values_len = length.length(array.values);

    switch (slice.slice(&array.keys.*, array.offset, array.len)) {
        .i8 => |*a| try validate_dict_keys(i8, a, values_len),
        .i16 => |*a| try validate_dict_keys(i16, a, values_len),
        .i32 => |*a| try validate_dict_keys(i32, a, values_len),
        .i64 => |*a| try validate_dict_keys(i64, a, values_len),
        .u8 => |*a| try validate_dict_keys(u8, a, values_len),
        .u16 => |*a| try validate_dict_keys(u16, a, values_len),
        .u32 => |*a| try validate_dict_keys(u32, a, values_len),
        .u64 => |*a| try validate_dict_keys(u64, a, values_len),
        else => {
            return Error.Invalid;
        },
    }

    // if (array.is_ordered and !is_sorted.is_sorted(.ascending, array.values)) {
    //     return Error.Invalid;
    // }
}

pub fn validate_binary_view(array: *const arr.BinaryViewArray) Error!void {
    if (@as(u64, array.offset) + @as(u64, array.len) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    try validate_validity(array.null_count, array.offset, array.len, array.validity);

    if (array.offset + array.len > array.views.len) {
        return Error.Invalid;
    }

    for (array.views) |view| {
        if (view.length < 0) {
            return Error.Invalid;
        }
        const len: u32 = @intCast(view.length);

        if (len <= 12) {
            continue;
        }

        if (view.buffer_idx < 0 or view.offset < 0) {
            return Error.Invalid;
        }

        const buffer_idx: u32 = @intCast(view.buffer_idx);
        const offset: u32 = @intCast(view.offset);

        if (array.buffers.len <= buffer_idx) {
            return Error.Invalid;
        }

        const buffer = array.buffers[buffer_idx];

        if (offset + len > buffer.len) {
            return Error.Invalid;
        }

        if (@as(u64, offset) + @as(u64, len) + 4 > std.math.maxInt(i32)) {
            return Error.Invalid;
        }

        const prefix_from_buffer = std.mem.readVarInt(i32, buffer[offset .. offset + 4], .little);

        if (prefix_from_buffer != view.prefix) {
            return Error.Invalid;
        }
    }
}

pub fn validate_list_view(comptime index_t: arr.IndexType, array: *const arr.GenericListViewArray(index_t)) Error!void {
    if (@as(u64, array.offset) + @as(u64, array.len) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    const needed_len = array.len + array.offset;

    try validate_validity(array.null_count, array.offset, array.len, array.validity);

    if (needed_len > array.offsets.len) {
        return Error.Invalid;
    }

    if (needed_len > array.sizes.len) {
        return Error.Invalid;
    }

    const inner_len = length.length(array.inner);

    const I = index_t.to_type();

    var idx: u32 = array.offset;
    while (idx < array.offset + array.len) : (idx += 1) {
        const offset = array.offsets[idx];
        const size = array.sizes[idx];

        if (offset < 0) {
            return Error.Invalid;
        }

        if (size < 0) {
            return Error.Invalid;
        }

        if (@as(i128, offset) + @as(i128, size) > std.math.maxInt(I)) {
            return Error.Invalid;
        }

        if (offset + size > inner_len) {
            return Error.Invalid;
        }
    }
}

pub fn validate_timestamp(array: *const arr.TimestampArray) Error!void {
    if (array.ts.timezone) |tz| {
        if (tz.len == 0) {
            return Error.Invalid;
        }
    }
}

pub fn validate(array: *const arr.Array) Error!void {
    switch (array.*) {
        .null => {},
        .i8 => |*a| try validate_primitive(i8, a),
        .i16 => |*a| try validate_primitive(i16, a),
        .i32 => |*a| try validate_primitive(i32, a),
        .i64 => |*a| try validate_primitive(i64, a),
        .u8 => |*a| try validate_primitive(u8, a),
        .u16 => |*a| try validate_primitive(u16, a),
        .u32 => |*a| try validate_primitive(u32, a),
        .u64 => |*a| try validate_primitive(u64, a),
        .f16 => |*a| try validate_primitive(f16, a),
        .f32 => |*a| try validate_primitive(f32, a),
        .f64 => |*a| try validate_primitive(f64, a),
        .binary => |*a| try validate_binary(.i32, a),
        .large_binary => |*a| try validate_binary(.i64, a),
        .utf8 => |*a| try validate_binary(.i32, &a.inner),
        .large_utf8 => |*a| try validate_binary(.i64, &a.inner),
        .bool => |*a| try validate_bool(a),
        .binary_view => |*a| try validate_binary_view(a),
        .utf8_view => |*a| try validate_binary_view(&a.inner),
        .decimal32 => |*a| try validate_decimal(.i32, a),
        .decimal64 => |*a| try validate_decimal(.i64, a),
        .decimal128 => |*a| try validate_decimal(.i128, a),
        .decimal256 => |*a| try validate_decimal(.i256, a),
        .fixed_size_binary => |*a| try validate_fixed_size_binary(a),
        .date32 => |*a| try validate_primitive(i32, &a.inner),
        .date64 => |*a| try validate_primitive(i64, &a.inner),
        .time32 => |*a| try validate_primitive(i32, &a.inner),
        .time64 => |*a| try validate_primitive(i64, &a.inner),
        .timestamp => |*a| try validate_timestamp(a),
        .duration => |*a| try validate_primitive(i64, &a.inner),
        .interval_year_month => |*a| try validate_primitive(i32, &a.inner),
        .interval_day_time => |*a| try validate_primitive([2]i32, &a.inner),
        .interval_month_day_nano => |*a| try validate_primitive(arr.MonthDayNano, &a.inner),
        .list => |*a| try validate_list(.i32, a),
        .large_list => |*a| try validate_list(.i64, a),
        .list_view => |*a| try validate_list_view(.i32, a),
        .large_list_view => |*a| try validate_list_view(.i64, a),
        .fixed_size_list => |*a| try validate_fixed_size_list(a),
        .struct_ => |*a| try validate_struct(a),
        .map => |*a| try validate_map(a),
        .dense_union => |*a| try validate_dense_union(a),
        .sparse_union => |*a| try validate_sparse_union(a),
        .run_end_encoded => |*a| try validate_run_end_encoded(a),
        .dict => |*a| try validate_dict(a),
    }
}
