const std = @import("std");
const arr = @import("./array.zig");
const length = @import("./length.zig");
const bitmap = @import("./bitmap.zig");
const slice = @import("./slice.zig");
const get = @import("./get.zig");
const dt_mod = @import("./data_type.zig");

const Error = error{
    Invalid,
};

fn validate_validity(null_count: u32, offset: u32, len: u32, valid: ?[]const u8) Error!void {
    if (@as(u64, offset) + @as(u64, len) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    const total_len = offset + len;
    const needed_bitmap_len = (total_len + 7) / 8;

    if (null_count > 0) {
        const v = valid orelse return Error.Invalid;

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
    } else if (valid) |v| {
        var idx: u32 = offset;
        while (idx < offset + len) : (idx += 1) {
            if (!bitmap.get(v.ptr, idx)) {
                return Error.Invalid;
            }
        }
    }
}

pub fn validate_primitive_array(comptime T: type, array: *const arr.PrimitiveArray(T)) Error!void {
    if (@as(u64, array.offset) + @as(u64, array.len) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    const needed_len = array.offset + array.len;
    if (needed_len > array.values.len) {
        return Error.Invalid;
    }

    try validate_validity(array.null_count, array.offset, array.len, array.validity);
}

pub fn validate_bool_array(array: *const arr.BoolArray) Error!void {
    if (@as(u64, array.offset) + @as(u64, array.len) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    const needed_bitmap_len = (array.offset + array.len + 7) / 8;
    if (needed_bitmap_len > array.values.len) {
        return Error.Invalid;
    }

    try validate_validity(array.null_count, array.offset, array.len, array.validity);
}

pub fn validate_binary_array(
    comptime index_t: arr.IndexType,
    array: *const arr.GenericBinaryArray(index_t),
) Error!void {
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

pub fn validate_decimal_params(comptime decimal_t: arr.DecimalInt, params: arr.DecimalParams) Error!void {
    const max_precision = switch (decimal_t) {
        .i32 => 9,
        .i64 => 19,
        .i128 => 38,
        .i256 => 76,
    };

    if (params.precision == 0) {
        return Error.Invalid;
    }

    if (params.precision > max_precision) {
        return Error.Invalid;
    }
}

pub fn validate_decimal_array(
    comptime decimal_t: arr.DecimalInt,
    array: *const arr.DecimalArray(decimal_t),
) Error!void {
    try validate_decimal_params(decimal_t, array.params);
    try validate_primitive_array(decimal_t.to_type(), &array.inner);
}

pub fn validate_list_array(
    comptime index_t: arr.IndexType,
    array: *const arr.GenericListArray(index_t),
) Error!void {
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

    try validate_array(array.inner);
}

fn validate_field_names(field_names: []const [:0]const u8) Error!void {
    for (field_names, 0..) |field_name, field_idx| {
        for (field_name) |c| {
            if (c == 0) {
                return Error.Invalid;
            }
        }

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

pub fn validate_struct_array(array: *const arr.StructArray) Error!void {
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

        try validate_array(child);
    }
}

fn validate_union_array(array: *const arr.UnionArray) Error!void {
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
        try validate_array(child);
    }
}

pub fn validate_dense_union_array(array: *const arr.DenseUnionArray) Error!void {
    try validate_union_array(&array.inner);

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

pub fn validate_sparse_union_array(array: *const arr.SparseUnionArray) Error!void {
    try validate_union_array(&array.inner);

    const needed_len = array.inner.len + array.inner.offset;
    for (array.inner.children) |*child| {
        if (needed_len > length.length(child)) {
            return Error.Invalid;
        }
    }
}

pub fn validate_fixed_size_binary_array(array: *const arr.FixedSizeBinaryArray) Error!void {
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

pub fn validate_fixed_size_list_array(array: *const arr.FixedSizeListArray) Error!void {
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

    try validate_array(array.inner);
}

pub fn validate_map_array(array: *const arr.MapArray) Error!void {
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
        .i8,
        .i16,
        .i32,
        .i64,
        .u8,
        .u16,
        .u32,
        .u64,
        .binary,
        .large_binary,
        .utf8,
        .large_utf8,
        .binary_view,
        .utf8_view,
        .fixed_size_binary,
        => {},
        else => return Error.Invalid,
    }

    try validate_struct_array(array.entries);

    // if (array.keys_are_sorted and !is_sorted.is_sorted(.ascending, &slice.slice(&array.entries.field_values[0], array.offset, array.len))) {
    //     return Error.Invalid;
    // }
}

fn validate_run_ends_array(comptime T: type, array: *const arr.RunEndArray, run_ends: *const arr.PrimitiveArray(T)) Error!void {
    try validate_primitive_array(T, run_ends);

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

pub fn validate_run_end_encoded_array(array: *const arr.RunEndArray) Error!void {
    if (@as(u64, array.offset) + @as(u64, array.len) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    const run_ends_len = length.length(array.run_ends);
    const values_len = length.length(array.values);

    // This is conservative but it is a b*** to implement it properly
    if (run_ends_len != values_len) {
        return Error.Invalid;
    }

    try validate_array(array.values);

    switch (array.run_ends.*) {
        .i16 => |*run_ends| try validate_run_ends_array(i16, array, run_ends),
        .i32 => |*run_ends| try validate_run_ends_array(i32, array, run_ends),
        .i64 => |*run_ends| try validate_run_ends_array(i64, array, run_ends),
        else => {
            return Error.Invalid;
        },
    }
}

fn validate_dict_keys_array(comptime T: type, keys: *const arr.PrimitiveArray(T), values_len: u32) Error!void {
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

pub fn validate_dict_array(array: *const arr.DictArray) Error!void {
    if (@as(u64, array.offset) + @as(u64, array.len) > std.math.maxInt(u32)) {
        return Error.Invalid;
    }

    const needed_len = array.len + array.offset;
    const keys_len = length.length(array.keys);

    if (needed_len > keys_len) {
        return Error.Invalid;
    }

    try validate_array(array.keys);
    try validate_array(array.values);

    const values_len = length.length(array.values);

    switch (slice.slice(&array.keys.*, array.offset, array.len)) {
        .i8 => |*a| try validate_dict_keys_array(i8, a, values_len),
        .i16 => |*a| try validate_dict_keys_array(i16, a, values_len),
        .i32 => |*a| try validate_dict_keys_array(i32, a, values_len),
        .i64 => |*a| try validate_dict_keys_array(i64, a, values_len),
        .u8 => |*a| try validate_dict_keys_array(u8, a, values_len),
        .u16 => |*a| try validate_dict_keys_array(u16, a, values_len),
        .u32 => |*a| try validate_dict_keys_array(u32, a, values_len),
        .u64 => |*a| try validate_dict_keys_array(u64, a, values_len),
        else => {
            return Error.Invalid;
        },
    }

    // if (array.is_ordered and !is_sorted.is_sorted(.ascending, array.values)) {
    //     return Error.Invalid;
    // }
}

pub fn validate_binary_view_array(array: *const arr.BinaryViewArray) Error!void {
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

pub fn validate_list_view_array(
    comptime index_t: arr.IndexType,
    array: *const arr.GenericListViewArray(index_t),
) Error!void {
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

    try validate_array(array.inner);
}

pub fn validate_timestamp(ts: arr.Timestamp) Error!void {
    if (ts.timezone) |tz| {
        if (tz.len == 0) {
            return Error.Invalid;
        }

        for (tz) |c| {
            if (c == 0) {
                return Error.Invalid;
            }
        }
    }
}

pub fn validate_timestamp_array(array: *const arr.TimestampArray) Error!void {
    try validate_timestamp(array.ts);
    try validate_primitive_array(i64, &array.inner);
}

pub fn validate_array(array: *const arr.Array) Error!void {
    switch (array.*) {
        .null => {},
        .i8 => |*a| try validate_primitive_array(i8, a),
        .i16 => |*a| try validate_primitive_array(i16, a),
        .i32 => |*a| try validate_primitive_array(i32, a),
        .i64 => |*a| try validate_primitive_array(i64, a),
        .u8 => |*a| try validate_primitive_array(u8, a),
        .u16 => |*a| try validate_primitive_array(u16, a),
        .u32 => |*a| try validate_primitive_array(u32, a),
        .u64 => |*a| try validate_primitive_array(u64, a),
        .f16 => |*a| try validate_primitive_array(f16, a),
        .f32 => |*a| try validate_primitive_array(f32, a),
        .f64 => |*a| try validate_primitive_array(f64, a),
        .binary => |*a| try validate_binary_array(.i32, a),
        .large_binary => |*a| try validate_binary_array(.i64, a),
        .utf8 => |*a| try validate_binary_array(.i32, &a.inner),
        .large_utf8 => |*a| try validate_binary_array(.i64, &a.inner),
        .bool => |*a| try validate_bool_array(a),
        .binary_view => |*a| try validate_binary_view_array(a),
        .utf8_view => |*a| try validate_binary_view_array(&a.inner),
        .decimal32 => |*a| try validate_decimal_array(.i32, a),
        .decimal64 => |*a| try validate_decimal_array(.i64, a),
        .decimal128 => |*a| try validate_decimal_array(.i128, a),
        .decimal256 => |*a| try validate_decimal_array(.i256, a),
        .fixed_size_binary => |*a| try validate_fixed_size_binary_array(a),
        .date32 => |*a| try validate_primitive_array(i32, &a.inner),
        .date64 => |*a| try validate_primitive_array(i64, &a.inner),
        .time32 => |*a| try validate_primitive_array(i32, &a.inner),
        .time64 => |*a| try validate_primitive_array(i64, &a.inner),
        .timestamp => |*a| try validate_timestamp_array(a),
        .duration => |*a| try validate_primitive_array(i64, &a.inner),
        .interval_year_month => |*a| try validate_primitive_array(i32, &a.inner),
        .interval_day_time => |*a| try validate_primitive_array([2]i32, &a.inner),
        .interval_month_day_nano => |*a| try validate_primitive_array(arr.MonthDayNano, &a.inner),
        .list => |*a| try validate_list_array(.i32, a),
        .large_list => |*a| try validate_list_array(.i64, a),
        .list_view => |*a| try validate_list_view_array(.i32, a),
        .large_list_view => |*a| try validate_list_view_array(.i64, a),
        .fixed_size_list => |*a| try validate_fixed_size_list_array(a),
        .struct_ => |*a| try validate_struct_array(a),
        .map => |*a| try validate_map_array(a),
        .dense_union => |*a| try validate_dense_union_array(a),
        .sparse_union => |*a| try validate_sparse_union_array(a),
        .run_end_encoded => |*a| try validate_run_end_encoded_array(a),
        .dict => |*a| try validate_dict_array(a),
    }
}

pub fn validate_data_type(dt: *const dt_mod.DataType) Error!void {
    switch (dt.*) {
        .null,
        .i8,
        .i16,
        .i32,
        .i64,
        .u8,
        .u16,
        .u32,
        .u64,
        .f16,
        .f32,
        .f64,
        .binary,
        .large_binary,
        .utf8,
        .large_utf8,
        .bool,
        .binary_view,
        .utf8_view,
        .date32,
        .date64,
        .time32,
        .time64,
        .duration,
        .interval_year_month,
        .interval_day_time,
        .interval_month_day_nano,
        => {},
        .decimal32 => |params| try validate_decimal_params(.i32, params),
        .decimal64 => |params| try validate_decimal_params(.i64, params),
        .decimal128 => |params| try validate_decimal_params(.i128, params),
        .decimal256 => |params| try validate_decimal_params(.i256, params),
        .fixed_size_binary => |bw| {
            if (bw <= 0) {
                return Error.Invalid;
            }
        },
        .timestamp => |ts| try validate_timestamp(ts),
        .list,
        .large_list,
        .list_view,
        .large_list_view,
        => |inner| try validate_data_type(inner),
        .fixed_size_list => |d| {
            if (d.item_width <= 0) {
                return Error.Invalid;
            }

            try validate_data_type(&d.inner);
        },
        .struct_ => |struct_t| try validate_struct_type(struct_t),
        .map => |map_t| try validate_map_type(map_t),
        .dense_union, .sparse_union => |union_t| try validate_union_type(union_t),
        .run_end_encoded => |ree_t| try validate_run_end_encoded_type(ree_t),
        .dict => |dict_t| try validate_dict_type(dict_t),
    }
}

pub fn validate_dict_type(dt: *const dt_mod.DictType) Error!void {
    try validate_data_type(&dt.value);
}

pub fn validate_run_end_encoded_type(dt: *const dt_mod.RunEndEncodedType) Error!void {
    try validate_data_type(&dt.value);
}

pub fn validate_union_type(dt: *const dt_mod.UnionType) Error!void {
    if (dt.field_names.len != dt.field_types.len) {
        return Error.Invalid;
    }

    if (dt.type_id_set.len != dt.field_names.len) {
        return Error.Invalid;
    }

    try validate_field_names(dt.field_names);

    for (dt.field_types) |*t| {
        try validate_data_type(t);
    }
}

pub fn validate_map_type(dt: *const dt_mod.MapType) Error!void {
    try validate_data_type(&dt.value);
}

pub fn validate_struct_type(dt: *const dt_mod.StructType) Error!void {
    if (dt.field_names.len != dt.field_types.len) {
        return Error.Invalid;
    }

    try validate_field_names(dt.field_names);

    for (dt.field_types) |*t| {
        try validate_data_type(t);
    }
}
