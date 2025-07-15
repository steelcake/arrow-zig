const std = @import("std");
const arr = @import("./array.zig");
const length = @import("./length.zig");

const Error = error{
    ValuesTooShort,
    ValidityTooShort,
    OffsetsTooShort,
    OffsetsOutOfOrder,
    DataTooShort,
    NoValidity,
    PrecisionTooHigh,
    ChildArrayTooShort,
    NumChildrenMismatch,
    TypeIdsTooShort,
    UnknownTypeId,
    DuplicateTypeId,
    InvalidEntries,
    ChildLenMismatch,
    InvalidRunEndsType,
    ViewsSliceTooShort,
    BuffersSliceTooShort,
    BufferTooShort,
    ViewFieldNegative,
    ViewPrefixMismatch,
    SizesTooShort,
    OffsetsSizesLenMismatch,
    OffsetNegative,
    SizeNegative,
    TypeIdsOffsetLenMismatch,
    WidthNegative,
    InvalidDictKeysType,
};

fn validate_validity(needed_bitmap_len: u32, null_count: u32, validity: ?[]const u8) Error!void {
    if (null_count > 0) {
        const v = validity orelse return Error.NoValidity;

        if (needed_bitmap_len > v.len) {
            return Error.ValidityTooShort;
        }
    }
}

pub fn validate_primitive(comptime T: type, array: *const arr.PrimitiveArray(T)) Error!void {
    const needed_len = array.offset + array.len;
    const needed_bitmap_len = (needed_len + 7) / 8;
    if (needed_len > array.values.len) {
        return Error.ValuesTooShort;
    }
    try validate_validity(needed_bitmap_len, array.null_count, array.validity);
}

pub fn validate_bool(array: *const arr.BoolArray) Error!void {
    const needed_bitmap_len = (array.offset + array.len + 7) / 8;
    if (needed_bitmap_len > array.values.len) {
        return Error.ValuesTooShort;
    }
    try validate_validity(needed_bitmap_len, array.null_count, array.validity);
}

pub fn validate_binary(comptime index_t: arr.IndexType, array: *const arr.GenericBinaryArray(index_t)) Error!void {
    const needed_len = array.offset + array.len;
    const needed_bitmap_len = (needed_len + 7) / 8;
    if (needed_len + 1 > array.offsets.len) {
        return Error.OffsetsTooShort;
    }
    try validate_validity(needed_bitmap_len, array.null_count, array.validity);

    var idx: u32 = 0;
    while (idx < needed_len) : (idx += 1) {
        if (array.offsets[idx] < 0) {
            return Error.OffsetNegative;
        }

        if (array.offsets[idx] > array.offsets[idx + 1]) {
            return Error.OffsetsOutOfOrder;
        }
    }

    const needed_data_len = array.offsets[needed_len];
    if (needed_data_len > array.data.len) {
        return Error.DataTooShort;
    }
}

pub fn validate_decimal(comptime decimal_t: arr.DecimalInt, array: *const arr.DecimalArray(decimal_t)) Error!void {
    const max_precision = switch (decimal_t) {
        .i32 => 9,
        .i64 => 19,
        .i128 => 38,
        .i256 => 76,
    };

    if (array.params.precision > max_precision) {
        return Error.PrecisionTooHigh;
    }

    try validate_primitive(decimal_t.to_type(), &array.inner);
}

pub fn validate_list(comptime index_t: arr.IndexType, array: *const arr.GenericListArray(index_t)) Error!void {
    const needed_len = array.offset + array.len;
    const needed_bitmap_len = (needed_len + 7) / 8;
    if (needed_len + 1 > array.offsets.len) {
        return Error.OffsetsTooShort;
    }
    try validate_validity(needed_bitmap_len, array.null_count, array.validity);

    var idx: u32 = 0;
    while (idx < needed_len) : (idx += 1) {
        if (array.offsets[idx] < 0) {
            return Error.OffsetNegative;
        }

        if (array.offsets[idx] > array.offsets[idx + 1]) {
            return Error.OffsetsOutOfOrder;
        }
    }

    const needed_inner_len = array.offsets[needed_len];
    if (needed_inner_len > length.length(array.inner)) {
        return Error.ChildArrayTooShort;
    }

    try validate(array.inner);
}

pub fn validate_struct(array: *const arr.StructArray) Error!void {
    const needed_len = array.offset + array.len;
    const needed_bitmap_len = (needed_len + 7) / 8;
    try validate_validity(needed_bitmap_len, array.null_count, array.validity);

    if (array.field_names.len != array.field_names.len) {
        return Error.NumChildrenMismatch;
    }

    for (array.field_values) |*child| {
        if (needed_len > length.length(child)) {
            return Error.ChildArrayTooShort;
        }

        try validate(child);
    }
}

pub fn validate_dense_union(array: *const arr.DenseUnionArray) Error!void {
    const needed_len = array.inner.offset + array.inner.len;
    if (needed_len > array.offsets.len) {
        return Error.OffsetsTooShort;
    }
    if (needed_len > array.inner.type_ids.len) {
        return Error.TypeIdsTooShort;
    }
    if (array.offsets.len != array.inner.type_ids.len) {
        return Error.TypeIdsOffsetLenMismatch;
    }

    if (array.inner.type_id_set.len != array.inner.children.len) {
        return Error.NumChildrenMismatch;
    }

    for (array.inner.type_id_set, 0..) |tid, idx| {
        for (array.inner.type_id_set[idx + 1 ..]) |other_tid| {
            if (tid == other_tid) {
                return Error.DuplicateTypeId;
            }
        }
    }

    for (array.inner.type_ids, array.offsets) |type_id, offset| {
        const child_idx = for (array.inner.type_id_set, 0..) |tid, idx| {
            if (tid == type_id) {
                break idx;
            }
        } else return Error.UnknownTypeId;

        if (child_idx >= array.inner.children.len) {
            return Error.NumChildrenMismatch;
        }

        const child_len = length.length(&array.inner.children[child_idx]);

        if (offset >= child_len) {
            return Error.ChildArrayTooShort;
        }
    }

    for (array.inner.children) |*child| {
        try validate(child);
    }
}

pub fn validate_sparse_union(array: *const arr.SparseUnionArray) Error!void {
    const needed_len = array.inner.offset + array.inner.len;
    if (needed_len > array.inner.type_ids.len) {
        return Error.TypeIdsTooShort;
    }

    if (array.inner.type_id_set.len != array.inner.children.len) {
        return Error.NumChildrenMismatch;
    }

    for (array.inner.type_id_set, 0..) |tid, idx| {
        for (array.inner.type_id_set[idx + 1 ..]) |other_tid| {
            if (tid == other_tid) {
                return Error.DuplicateTypeId;
            }
        }
    }

    for (array.inner.type_ids) |type_id| {
        _ = for (array.inner.type_id_set, 0..) |tid, idx| {
            if (tid == type_id) {
                break idx;
            }
        } else return Error.UnknownTypeId;
    }

    for (array.inner.children) |*child| {
        if (needed_len > length.length(child)) {
            return Error.ChildArrayTooShort;
        }
        try validate(child);
    }
}

pub fn validate_fixed_size_binary(array: *const arr.FixedSizeBinaryArray) Error!void {
    const needed_len = array.offset + array.len;
    const needed_bitmap_len = (needed_len + 7) / 8;

    if (array.byte_width < 0) {
        return Error.WidthNegative;
    }

    const byte_width: u32 = @intCast(array.byte_width);

    if (needed_len * byte_width > array.data.len) {
        return Error.DataTooShort;
    }

    try validate_validity(needed_bitmap_len, array.null_count, array.validity);
}

pub fn validate_fixed_size_list(array: *const arr.FixedSizeListArray) Error!void {
    const needed_len = array.offset + array.len;
    const needed_bitmap_len = (needed_len + 7) / 8;

    if (array.item_width < 0) {
        return Error.WidthNegative;
    }

    const item_width: u32 = @intCast(array.item_width);

    if (needed_len * item_width > length.length(array.inner)) {
        return Error.ChildArrayTooShort;
    }

    try validate_validity(needed_bitmap_len, array.null_count, array.validity);

    try validate(array.inner);
}

pub fn validate_map(array: *const arr.MapArray) Error!void {
    const needed_len = array.offset + array.len;
    const needed_bitmap_len = (needed_len + 7) / 8;

    try validate_validity(needed_bitmap_len, array.null_count, array.validity);

    var idx: u32 = 0;
    while (idx < needed_len) : (idx += 1) {
        if (array.offsets[idx] > array.offsets[idx + 1]) {
            return Error.OffsetsOutOfOrder;
        }
    }

    const needed_inner_len = array.offsets[needed_len];
    if (needed_inner_len > array.entries.len) {
        return Error.ChildArrayTooShort;
    }

    if (array.entries.field_names.len != 2 and
        std.mem.eql(u8, array.entries.field_names[0], "keys") and
        std.mem.eql(u8, array.entries.field_names[1], "values"))
    {
        return Error.InvalidEntries;
    }

    try validate_struct(array.entries);
}

pub fn validate_run_end_encoded(array: *const arr.RunEndArray) Error!void {
    const needed_len = array.offset + array.len;

    const run_ends_len = length.length(array.run_ends);
    const values_len = length.length(array.values);

    if (needed_len > run_ends_len) {
        return Error.ChildArrayTooShort;
    }
    if (needed_len > values_len) {
        return Error.ChildArrayTooShort;
    }

    switch (array.run_ends.*) {
        .i16, .i32, .i64 => {},
        else => {
            return Error.InvalidRunEndsType;
        },
    }

    try validate(array.run_ends);
    try validate(array.values);
}

pub fn validate_dict(array: *const arr.DictArray) Error!void {
    const needed_len = array.len + array.offset;
    const keys_len = length.length(array.keys);

    if (needed_len > keys_len) {
        return Error.ChildArrayTooShort;
    }

    switch (array.keys.*) {
        .i8, .i16, .i32, .i64, .u8, .u16, .u32, .u64 => {},
        else => {
            return Error.InvalidDictKeysType;
        },
    }

    // for (array.keys) |key| {
    //     if (array.values.len <= key) {
    //         error
    //     }
    // }

    try validate(array.values);
    try validate(array.values);

    // if (array.is_ordered) {
    //     check_is_ordered(array.values);
    // }
}

pub fn validate_binary_view(array: *const arr.BinaryViewArray) Error!void {
    const needed_len = array.len + array.offset;
    const needed_bitmap_len = (needed_len + 7) / 8;

    try validate_validity(needed_bitmap_len, array.null_count, array.validity);

    if (needed_len > array.views.len) {
        return Error.ViewsSliceTooShort;
    }

    for (array.views) |view| {
        if (view.length < 0) {
            return Error.ViewFieldNegative;
        }
        const len: u32 = @intCast(view.length);

        if (len <= 12) {
            continue;
        }

        if (view.buffer_idx < 0 or view.offset < 0) {
            return Error.ViewFieldNegative;
        }

        const buffer_idx: u32 = @intCast(view.buffer_idx);
        const offset: u32 = @intCast(view.offset);

        if (array.buffers.len <= buffer_idx) {
            return Error.BuffersSliceTooShort;
        }

        const buffer = array.buffers[buffer_idx];

        if (offset + len > buffer.len) {
            return Error.BufferTooShort;
        }

        const prefix_from_buffer = std.mem.readVarInt(i32, buffer[offset .. offset + @min(4, len)], .little);

        if (prefix_from_buffer != view.prefix) {
            return Error.ViewPrefixMismatch;
        }
    }
}

pub fn validate_list_view(comptime index_t: arr.IndexType, array: *const arr.GenericListViewArray(index_t)) Error!void {
    const needed_len = array.len + array.offset;
    const needed_bitmap_len = (needed_len + 7) / 8;

    try validate_validity(needed_bitmap_len, array.null_count, array.validity);

    if (needed_len > array.offsets.len) {
        return Error.OffsetsTooShort;
    }

    if (needed_len > array.sizes.len) {
        return Error.SizesTooShort;
    }

    if (array.offsets.len != array.sizes.len) {
        return Error.OffsetsSizesLenMismatch;
    }

    const inner_len = length.length(array.inner);

    for (array.offsets, array.sizes) |offset, size| {
        if (offset < 0) {
            return Error.OffsetNegative;
        }

        if (size < 0) {
            return Error.SizeNegative;
        }

        if (offset + size > inner_len) {
            return Error.ChildArrayTooShort;
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
        .timestamp => |*a| try validate_primitive(i64, &a.inner),
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
