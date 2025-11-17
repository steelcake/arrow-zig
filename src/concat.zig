const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const ArenaAllocator = std.heap.ArenaAllocator;

const arr = @import("./array.zig");
const slice = @import("./slice.zig");
const bitmap = @import("./bitmap.zig");
const equals = @import("./equals.zig");
const get = @import("./get.zig");
const data_type = @import("./data_type.zig");
const length = @import("./length.zig");

const Error = error{
    OutOfMemory,
};

fn convert_arrays(comptime ArrayT: type, comptime field_name: []const u8, arrays: []const arr.Array, scratch_alloc: Allocator) Error![]ArrayT {
    const out = try scratch_alloc.alloc(ArrayT, arrays.len);

    for (arrays, 0..) |array, idx| {
        out[idx] = @field(array, field_name);
    }

    return out;
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
/// Scratch alloc will be used to allocate intermediary slices, it can be deallocated after this function returns.
pub fn concat(dt: data_type.DataType, arrays: []const arr.Array, alloc: Allocator, scratch_alloc: Allocator) Error!arr.Array {
    switch (dt) {
        .null => {
            const a = try convert_arrays(arr.NullArray, "null", arrays, scratch_alloc);
            return .{ .null = concat_null(a) };
        },
        .i8 => {
            const a = try convert_arrays(arr.Int8Array, "i8", arrays, scratch_alloc);
            return .{ .i8 = try concat_primitive(i8, a, alloc) };
        },
        .i16 => {
            const a = try convert_arrays(arr.Int16Array, "i16", arrays, scratch_alloc);
            return .{ .i16 = try concat_primitive(i16, a, alloc) };
        },
        .i32 => {
            const a = try convert_arrays(arr.Int32Array, "i32", arrays, scratch_alloc);
            return .{ .i32 = try concat_primitive(i32, a, alloc) };
        },
        .i64 => {
            const a = try convert_arrays(arr.Int64Array, "i64", arrays, scratch_alloc);
            return .{ .i64 = try concat_primitive(i64, a, alloc) };
        },
        .u8 => {
            const a = try convert_arrays(arr.UInt8Array, "u8", arrays, scratch_alloc);
            return .{ .u8 = try concat_primitive(u8, a, alloc) };
        },
        .u16 => {
            const a = try convert_arrays(arr.UInt16Array, "u16", arrays, scratch_alloc);
            return .{ .u16 = try concat_primitive(u16, a, alloc) };
        },
        .u32 => {
            const a = try convert_arrays(arr.UInt32Array, "u32", arrays, scratch_alloc);
            return .{ .u32 = try concat_primitive(u32, a, alloc) };
        },
        .u64 => {
            const a = try convert_arrays(arr.UInt64Array, "u64", arrays, scratch_alloc);
            return .{ .u64 = try concat_primitive(u64, a, alloc) };
        },
        .f16 => {
            const a = try convert_arrays(arr.Float16Array, "f16", arrays, scratch_alloc);
            return .{ .f16 = try concat_primitive(f16, a, alloc) };
        },
        .f32 => {
            const a = try convert_arrays(arr.Float32Array, "f32", arrays, scratch_alloc);
            return .{ .f32 = try concat_primitive(f32, a, alloc) };
        },
        .f64 => {
            const a = try convert_arrays(arr.Float64Array, "f64", arrays, scratch_alloc);
            return .{ .f64 = try concat_primitive(f64, a, alloc) };
        },
        .binary => {
            const a = try convert_arrays(arr.BinaryArray, "binary", arrays, scratch_alloc);
            return .{ .binary = try concat_binary(.i32, a, alloc) };
        },
        .utf8 => {
            const a = try convert_arrays(arr.Utf8Array, "utf8", arrays, scratch_alloc);
            return .{ .utf8 = try concat_utf8(.i32, a, alloc, scratch_alloc) };
        },
        .bool => {
            const a = try convert_arrays(arr.BoolArray, "bool", arrays, scratch_alloc);
            return .{ .bool = try concat_bool(a, alloc) };
        },
        .decimal32 => |params| {
            const a = try convert_arrays(arr.Decimal32Array, "decimal32", arrays, scratch_alloc);
            return .{ .decimal32 = try concat_decimal(.i32, params, a, alloc, scratch_alloc) };
        },
        .decimal64 => |params| {
            const a = try convert_arrays(arr.Decimal64Array, "decimal64", arrays, scratch_alloc);
            return .{ .decimal64 = try concat_decimal(.i64, params, a, alloc, scratch_alloc) };
        },
        .decimal128 => |params| {
            const a = try convert_arrays(arr.Decimal128Array, "decimal128", arrays, scratch_alloc);
            return .{ .decimal128 = try concat_decimal(.i128, params, a, alloc, scratch_alloc) };
        },
        .decimal256 => |params| {
            const a = try convert_arrays(arr.Decimal256Array, "decimal256", arrays, scratch_alloc);
            return .{ .decimal256 = try concat_decimal(.i256, params, a, alloc, scratch_alloc) };
        },
        .date32 => {
            const a = try convert_arrays(arr.Date32Array, "date32", arrays, scratch_alloc);
            return .{ .date32 = try concat_date(.i32, a, alloc, scratch_alloc) };
        },
        .date64 => {
            const a = try convert_arrays(arr.Date64Array, "date64", arrays, scratch_alloc);
            return .{ .date64 = try concat_date(.i64, a, alloc, scratch_alloc) };
        },
        .time32 => |unit| {
            const a = try convert_arrays(arr.Time32Array, "time32", arrays, scratch_alloc);
            return .{ .time32 = try concat_time(.i32, unit, a, alloc, scratch_alloc) };
        },
        .time64 => |unit| {
            const a = try convert_arrays(arr.Time64Array, "time64", arrays, scratch_alloc);
            return .{ .time64 = try concat_time(.i64, unit, a, alloc, scratch_alloc) };
        },
        .timestamp => |ts| {
            const a = try convert_arrays(arr.TimestampArray, "timestamp", arrays, scratch_alloc);
            return .{ .timestamp = try concat_timestamp(ts, a, alloc, scratch_alloc) };
        },
        .interval_year_month => {
            const a = try convert_arrays(arr.IntervalYearMonthArray, "interval_year_month", arrays, scratch_alloc);
            return .{ .interval_year_month = try concat_interval(.year_month, a, alloc, scratch_alloc) };
        },
        .interval_day_time => {
            const a = try convert_arrays(arr.IntervalDayTimeArray, "interval_day_time", arrays, scratch_alloc);
            return .{ .interval_day_time = try concat_interval(.day_time, a, alloc, scratch_alloc) };
        },
        .interval_month_day_nano => {
            const a = try convert_arrays(arr.IntervalMonthDayNanoArray, "interval_month_day_nano", arrays, scratch_alloc);
            return .{ .interval_month_day_nano = try concat_interval(.month_day_nano, a, alloc, scratch_alloc) };
        },
        .list => |inner_dt| {
            const a = try convert_arrays(arr.ListArray, "list", arrays, scratch_alloc);
            return .{ .list = try concat_list(.i32, inner_dt.*, a, alloc, scratch_alloc) };
        },
        .struct_ => |sdt| {
            const a = try convert_arrays(arr.StructArray, "struct_", arrays, scratch_alloc);
            return .{ .struct_ = try concat_struct(sdt.*, a, alloc, scratch_alloc) };
        },
        .dense_union => |union_t| {
            const a = try convert_arrays(arr.DenseUnionArray, "dense_union", arrays, scratch_alloc);
            return .{ .dense_union = try concat_dense_union(union_t.*, a, alloc, scratch_alloc) };
        },
        .sparse_union => |union_t| {
            const a = try convert_arrays(arr.SparseUnionArray, "sparse_union", arrays, scratch_alloc);
            return .{ .sparse_union = try concat_sparse_union(union_t.*, a, alloc, scratch_alloc) };
        },
        .fixed_size_binary => |bw| {
            const a = try convert_arrays(arr.FixedSizeBinaryArray, "fixed_size_binary", arrays, scratch_alloc);
            return .{ .fixed_size_binary = try concat_fixed_size_binary(bw, a, alloc) };
        },
        .fixed_size_list => |fsl_t| {
            const a = try convert_arrays(arr.FixedSizeListArray, "fixed_size_list", arrays, scratch_alloc);
            return .{ .fixed_size_list = try concat_fixed_size_list(fsl_t.*, a, alloc, scratch_alloc) };
        },
        .map => |map_t| {
            const a = try convert_arrays(arr.MapArray, "map", arrays, scratch_alloc);
            return .{ .map = try concat_map(map_t.*, a, alloc, scratch_alloc) };
        },
        .duration => |unit| {
            const a = try convert_arrays(arr.DurationArray, "duration", arrays, scratch_alloc);
            return .{ .duration = try concat_duration(unit, a, alloc, scratch_alloc) };
        },
        .large_binary => {
            const a = try convert_arrays(arr.LargeBinaryArray, "large_binary", arrays, scratch_alloc);
            return .{ .large_binary = try concat_binary(.i64, a, alloc) };
        },
        .large_utf8 => {
            const a = try convert_arrays(arr.LargeUtf8Array, "large_utf8", arrays, scratch_alloc);
            return .{ .large_utf8 = try concat_utf8(.i64, a, alloc, scratch_alloc) };
        },
        .large_list => |inner_dt| {
            const a = try convert_arrays(arr.LargeListArray, "large_list", arrays, scratch_alloc);
            return .{ .large_list = try concat_list(.i64, inner_dt.*, a, alloc, scratch_alloc) };
        },
        .run_end_encoded => |reet| {
            const a = try convert_arrays(arr.RunEndArray, "run_end_encoded", arrays, scratch_alloc);
            return .{ .run_end_encoded = try concat_run_end_encoded(reet.*, a, alloc, scratch_alloc) };
        },
        .binary_view => {
            const a = try convert_arrays(arr.BinaryViewArray, "binary_view", arrays, scratch_alloc);
            return .{ .binary_view = try concat_binary_view(a, alloc) };
        },
        .utf8_view => {
            const a = try convert_arrays(arr.Utf8ViewArray, "utf8_view", arrays, scratch_alloc);
            return .{ .utf8_view = try concat_utf8_view(a, alloc, scratch_alloc) };
        },
        .list_view => |idt| {
            const a = try convert_arrays(arr.ListViewArray, "list_view", arrays, scratch_alloc);
            return .{ .list_view = try concat_list_view(.i32, idt.*, a, alloc, scratch_alloc) };
        },
        .large_list_view => |idt| {
            const a = try convert_arrays(arr.LargeListViewArray, "large_list_view", arrays, scratch_alloc);
            return .{ .large_list_view = try concat_list_view(.i64, idt.*, a, alloc, scratch_alloc) };
        },
        .dict => |dict_t| {
            const a = try convert_arrays(arr.DictArray, "dict", arrays, scratch_alloc);
            return .{ .dict = try concat_dict(dict_t.*, a, alloc, scratch_alloc) };
        },
    }
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
/// Scratch alloc will be used to allocate intermediary slices, it can be deallocated after this function returns.
pub fn concat_map(map_t: data_type.MapType, arrays: []const arr.MapArray, alloc: Allocator, scratch_alloc: Allocator) Error!arr.MapArray {
    var total_len: u32 = 0;
    var total_null_count: u32 = 0;

    for (arrays) |array| {
        total_len += array.len;
        total_null_count += array.null_count;
    }

    const has_nulls = total_null_count > 0;
    const bitmap_len = (total_len + 7) / 8;

    const validity: []u8 = if (has_nulls) has_n: {
        const v = try alloc.alloc(u8, bitmap_len);
        @memset(v, 0xff);
        break :has_n v;
    } else &.{};
    const offsets = try alloc.alloc(i32, total_len + 1);

    const entries_list = try scratch_alloc.alloc(arr.StructArray, arrays.len);

    var inner_offset: u32 = 0;
    var write_offset: u32 = 0;
    for (arrays, 0..) |array, arr_idx| {
        const input_start: usize = @intCast(array.offsets[array.offset]);
        const input_end: usize = @intCast(array.offsets[array.offset + array.len]);
        const input_len = input_end - input_start;

        entries_list[arr_idx] = slice.slice_struct(array.entries, @intCast(input_start), @intCast(input_len));

        {
            var idx: u32 = array.offset;
            var w_idx: u32 = write_offset;
            const offset_diff: i32 = @as(i32, @intCast(inner_offset)) - array.offsets[array.offset];
            while (idx < array.offset + array.len) : ({
                idx += 1;
                w_idx += 1;
            }) {
                offsets[w_idx] = array.offsets[idx] + offset_diff;
            }
        }

        if (array.null_count > 0) {
            const v = (array.validity orelse unreachable);

            var idx: u32 = array.offset;
            var w_idx: u32 = write_offset;
            while (idx < array.offset + array.len) : ({
                idx += 1;
                w_idx += 1;
            }) {
                if (!bitmap.get(v, idx)) {
                    bitmap.unset(validity, w_idx);
                }
            }
        }

        write_offset += array.len;
        inner_offset += @as(u32, @intCast(input_len));
    }

    offsets[total_len] = @intCast(inner_offset);

    const entries_dt = data_type.StructType{
        .field_names = &.{ "keys", "values" },
        .field_types = &.{ map_t.key.to_data_type(), map_t.value },
    };
    const entries = try alloc.create(arr.StructArray);
    entries.* = try concat_struct(entries_dt, entries_list, alloc, scratch_alloc);

    return arr.MapArray{
        .entries = entries,
        .len = total_len,
        .offset = 0,
        .offsets = offsets,
        .validity = if (has_nulls) validity else null,
        .null_count = total_null_count,
        .keys_are_sorted = false,
    };
}

fn concat_dict_keys(comptime T: type, arrays: []const arr.DictArray, alloc: Allocator) Error!arr.PrimitiveArray(T) {
    var total_len: u32 = 0;
    for (arrays) |array| {
        total_len += array.len;
    }

    const keys_values = try alloc.alloc(T, total_len);
    var key_offset: T = 0;
    var write_offset: u32 = 0;
    for (arrays) |array| {
        const keys: arr.PrimitiveArray(T) = slice.slice_primitive(T, &@field(array.keys, @typeName(T)), array.offset, array.len);

        std.debug.assert(keys.null_count == 0);

        var idx = keys.offset;
        while (idx < keys.offset + keys.len) : ({
            idx += 1;
            write_offset += 1;
        }) {
            keys_values[write_offset] = keys.values[idx] + key_offset;
        }

        key_offset += @as(T, @intCast(length.length(array.values)));
    }

    return .{
        .offset = 0,
        .len = total_len,
        .values = keys_values,
        .validity = null,
        .null_count = 0,
    };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
/// Scratch alloc will be used to allocate intermediary slices, it can be deallocated after this function returns.
pub fn concat_dict(dt: data_type.DictType, arrays: []const arr.DictArray, alloc: Allocator, scratch_alloc: Allocator) Error!arr.DictArray {
    for (arrays) |array| {
        const keytype: data_type.DictKeyType = switch (array.keys.*) {
            .i8 => .i8,
            .i16 => .i16,
            .i32 => .i32,
            .i64 => .i64,
            .u8 => .u8,
            .u16 => .u16,
            .u32 => .u32,
            .u64 => .u64,
            else => unreachable,
        };
        std.debug.assert(keytype == dt.key);
        std.debug.assert(data_type.check_data_type(array.values, &dt.value));
    }

    const values_list = try scratch_alloc.alloc(arr.Array, arrays.len);
    for (arrays, 0..) |array, idx| {
        values_list[idx] = array.values.*;
    }
    const values = try alloc.create(arr.Array);
    values.* = try concat(dt.value, values_list, alloc, scratch_alloc);

    const keys = try alloc.create(arr.Array);
    keys.* = switch (dt.key) {
        .i8 => .{ .i8 = try concat_dict_keys(i8, arrays, alloc) },
        .i16 => .{ .i16 = try concat_dict_keys(i16, arrays, alloc) },
        .i32 => .{ .i32 = try concat_dict_keys(i32, arrays, alloc) },
        .i64 => .{ .i64 = try concat_dict_keys(i64, arrays, alloc) },
        .u8 => .{ .u8 = try concat_dict_keys(u8, arrays, alloc) },
        .u16 => .{ .u16 = try concat_dict_keys(u16, arrays, alloc) },
        .u32 => .{ .u32 = try concat_dict_keys(u32, arrays, alloc) },
        .u64 => .{ .u64 = try concat_dict_keys(u64, arrays, alloc) },
    };

    var total_len: u32 = 0;

    for (arrays) |array| {
        total_len += array.len;
    }

    return .{
        .is_ordered = false,
        .keys = keys,
        .values = values,
        .len = total_len,
        .offset = 0,
    };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
/// Scratch alloc will be used to allocate intermediary slices, it can be deallocated after this function returns.
pub fn concat_run_end_encoded(reet: data_type.RunEndEncodedType, arrays: []const arr.RunEndArray, alloc: Allocator, scratch_alloc: Allocator) Error!arr.RunEndArray {
    for (arrays) |array| {
        const retype: data_type.RunEndType = switch (array.run_ends.*) {
            .i16 => .i16,
            .i32 => .i32,
            .i64 => .i64,
            else => unreachable,
        };
        std.debug.assert(retype == reet.run_end);
        std.debug.assert(data_type.check_data_type(array.values, &reet.value));
    }

    var total_len: u32 = 0;

    for (arrays) |array| {
        total_len += array.len;
    }

    const values_list = try scratch_alloc.alloc(arr.Array, arrays.len);
    const run_ends_list = try scratch_alloc.alloc(arr.Array, arrays.len);

    var lift: u32 = 0;
    for (arrays, 0..) |*array, idx| {
        const normalized_arr = try slice.normalize_run_end_encoded(array, lift, scratch_alloc);
        values_list[idx] = normalized_arr.values.*;
        run_ends_list[idx] = normalized_arr.run_ends.*;
        lift += array.len;
    }

    const values = try alloc.create(arr.Array);
    values.* = try concat(reet.value, values_list, alloc, scratch_alloc);

    const run_ends = try alloc.create(arr.Array);
    run_ends.* = try concat(reet.run_end.to_data_type(), run_ends_list, alloc, scratch_alloc);

    return .{
        .run_ends = run_ends,
        .values = values,
        .len = total_len,
        .offset = 0,
    };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
/// Scratch alloc will be used to allocate intermediary slices, it can be deallocated after this function returns.
pub fn concat_list_view(comptime index_t: arr.IndexType, dt: data_type.DataType, arrays: []const arr.GenericListViewArray(index_t), alloc: Allocator, scratch_alloc: Allocator) Error!arr.GenericListViewArray(index_t) {
    const I = index_t.to_type();

    for (arrays) |array| {
        std.debug.assert(data_type.check_data_type(array.inner, &dt));
    }

    const inners = try scratch_alloc.alloc(arr.Array, arrays.len);
    for (arrays, 0..) |array, idx| {
        inners[idx] = array.inner.*;
    }
    const inner = try alloc.create(arr.Array);
    inner.* = try concat(dt, inners, alloc, scratch_alloc);

    var total_len: u32 = 0;
    var total_null_count: u32 = 0;

    for (arrays) |array| {
        total_len += array.len;
        total_null_count += array.null_count;
    }

    const offsets = try alloc.alloc(I, total_len);
    const sizes = try alloc.alloc(I, total_len);
    {
        var write_offset: u32 = 0;
        var offset_offset: I = 0;
        for (arrays) |array| {
            var w_idx = write_offset;
            var idx = array.offset;
            while (idx < array.offset + array.len) : ({
                w_idx += 1;
                idx += 1;
            }) {
                sizes[w_idx] = array.sizes[idx];
                offsets[w_idx] = array.offsets[idx] + offset_offset;
            }

            write_offset += array.len;
            offset_offset += @as(I, @intCast(length.length(array.inner)));
        }
    }

    const has_nulls = total_null_count > 0;
    const bitmap_len = (total_len + 7) / 8;

    const validity: []u8 = if (has_nulls) has_n: {
        const v = try alloc.alloc(u8, bitmap_len);
        @memset(v, 0xff);
        break :has_n v;
    } else &.{};

    if (has_nulls) {
        var write_offset: u32 = 0;
        for (arrays) |array| {
            if (array.null_count > 0) {
                const v = (array.validity orelse unreachable);

                var idx: u32 = array.offset;
                var w_idx: u32 = write_offset;
                while (idx < array.offset + array.len) : ({
                    idx += 1;
                    w_idx += 1;
                }) {
                    if (!bitmap.get(v, idx)) {
                        bitmap.unset(validity, w_idx);
                    }
                }
            }

            write_offset += array.len;
        }
    }

    return .{
        .inner = inner,
        .offsets = offsets,
        .sizes = sizes,
        .validity = if (has_nulls) validity else null,
        .len = total_len,
        .offset = 0,
        .null_count = total_null_count,
    };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
/// Scratch alloc will be used to allocate intermediary slices, it can be deallocated after this function returns.
pub fn concat_sparse_union(dt: data_type.UnionType, arrays: []const arr.SparseUnionArray, alloc: Allocator, scratch_alloc: Allocator) Error!arr.SparseUnionArray {
    for (arrays) |array| {
        std.debug.assert(dt.check(&array.inner));
    }

    const field_arrays = try scratch_alloc.alloc(arr.Array, arrays.len);
    const field_values = try alloc.alloc(arr.Array, dt.field_types.len);

    for (0..dt.field_names.len) |field_idx| {
        for (arrays, 0..) |array, array_idx| {
            field_arrays[array_idx] = slice.slice(&array.inner.children[field_idx], array.inner.offset, array.inner.len);
        }

        field_values[field_idx] = try concat(dt.field_types[field_idx], field_arrays, alloc, scratch_alloc);
    }

    const field_names = try alloc.alloc([:0]const u8, dt.field_names.len);
    for (dt.field_names, 0..) |name, idx| {
        const field_name = try alloc.allocSentinel(u8, name.len, 0);
        @memcpy(field_name, name);
        field_names[idx] = field_name;
    }

    var total_len: u32 = 0;

    for (arrays) |array| {
        total_len += array.inner.len;
    }

    const type_ids = try alloc.alloc(i8, total_len);
    var write_offset: i32 = 0;
    for (arrays) |array| {
        var w_idx: usize = @intCast(write_offset);
        var idx: u32 = array.inner.offset;
        while (idx < array.inner.offset + array.inner.len) : ({
            idx += 1;
            w_idx += 1;
        }) {
            type_ids[w_idx] = array.inner.type_ids[idx];
        }

        write_offset = @intCast(w_idx);
    }

    const type_id_set = try alloc.alloc(i8, dt.type_id_set.len);
    for (dt.type_id_set, 0..) |tid, idx| {
        type_id_set[idx] = tid;
    }

    return .{
        .inner = .{
            .type_id_set = type_id_set,
            .field_names = field_names,
            .type_ids = type_ids,
            .children = field_values,
            .len = total_len,
            .offset = 0,
        },
    };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
/// Scratch alloc will be used to allocate intermediary slices, it can be deallocated after this function returns.
pub fn concat_dense_union(dt: data_type.UnionType, arrays: []const arr.DenseUnionArray, alloc: Allocator, scratch_alloc: Allocator) Error!arr.DenseUnionArray {
    for (arrays) |array| {
        std.debug.assert(dt.check(&array.inner));
    }

    const field_arrays = try scratch_alloc.alloc(arr.Array, arrays.len);
    const field_values = try alloc.alloc(arr.Array, dt.field_types.len);

    for (0..dt.field_names.len) |field_idx| {
        for (arrays, 0..) |array, array_idx| {
            field_arrays[array_idx] = array.inner.children[field_idx];
        }

        field_values[field_idx] = try concat(dt.field_types[field_idx], field_arrays, alloc, scratch_alloc);
    }

    const field_names = try alloc.alloc([:0]const u8, dt.field_names.len);
    for (dt.field_names, 0..) |name, idx| {
        const field_name = try alloc.allocSentinel(u8, name.len, 0);
        @memcpy(field_name, name);
        field_names[idx] = field_name;
    }

    var total_len: u32 = 0;

    for (arrays) |array| {
        total_len += array.inner.len;
    }

    const offsets = try alloc.alloc(i32, total_len);
    const type_ids = try alloc.alloc(i8, total_len);
    var write_offset: i32 = 0;
    const child_offsets = try alloc.alloc(i32, dt.field_names.len);
    @memset(child_offsets, 0);
    for (arrays) |array| {
        var w_idx: usize = @intCast(write_offset);
        var idx: u32 = array.inner.offset;
        while (idx < array.inner.offset + array.inner.len) : ({
            idx += 1;
            w_idx += 1;
        }) {
            const type_id = array.inner.type_ids[idx];
            const child_idx = for (0..dt.type_id_set.len) |i| {
                if (dt.type_id_set[i] == type_id) {
                    break i;
                }
            } else unreachable;
            const child_offset = child_offsets[child_idx];
            offsets[w_idx] = array.offsets[idx] + child_offset;
            type_ids[w_idx] = type_id;
        }

        write_offset = @intCast(w_idx);

        for (0..dt.field_names.len) |child_idx| {
            child_offsets[child_idx] += @intCast(length.length(&array.inner.children[child_idx]));
        }
    }
    std.debug.assert(write_offset == total_len);

    const type_id_set = try alloc.alloc(i8, dt.type_id_set.len);
    for (dt.type_id_set, 0..) |tid, idx| {
        type_id_set[idx] = tid;
    }

    return .{
        .offsets = offsets,
        .inner = .{
            .type_id_set = type_id_set,
            .field_names = field_names,
            .type_ids = type_ids,
            .children = field_values,
            .len = total_len,
            .offset = 0,
        },
    };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
/// Scratch alloc will be used to allocate intermediary slices, it can be deallocated after this function returns.
pub fn concat_struct(dt: data_type.StructType, arrays: []const arr.StructArray, alloc: Allocator, scratch_alloc: Allocator) Error!arr.StructArray {
    for (arrays) |array| {
        std.debug.assert(array.field_names.len == dt.field_names.len);
        for (array.field_names, dt.field_names) |afn, dfn| {
            std.debug.assert(std.mem.eql(u8, afn, dfn));
        }

        std.debug.assert(array.field_values.len == dt.field_types.len);
        for (array.field_values, dt.field_types) |*afv, *dft| {
            std.debug.assert(data_type.check_data_type(afv, dft));
        }
    }

    const field_arrays = try scratch_alloc.alloc(arr.Array, arrays.len);

    const field_values = try alloc.alloc(arr.Array, dt.field_types.len);

    for (0..dt.field_names.len) |field_idx| {
        for (arrays, 0..) |array, array_idx| {
            field_arrays[array_idx] = slice.slice(&array.field_values[field_idx], array.offset, array.len);
        }

        field_values[field_idx] = try concat(dt.field_types[field_idx], field_arrays, alloc, scratch_alloc);
    }

    const field_names = try alloc.alloc([:0]const u8, dt.field_names.len);
    for (dt.field_names, 0..) |name, idx| {
        const field_name = try alloc.allocSentinel(u8, name.len, 0);
        @memcpy(field_name, name);
        field_names[idx] = field_name;
    }

    var total_len: u32 = 0;
    var total_null_count: u32 = 0;

    for (arrays) |array| {
        total_len += array.len;
        total_null_count += array.null_count;
    }

    const has_nulls = total_null_count > 0;
    const bitmap_len = (total_len + 7) / 8;

    const validity: []u8 = if (has_nulls) has_n: {
        const v = try alloc.alloc(u8, bitmap_len);
        @memset(v, 0xff);
        break :has_n v;
    } else &.{};

    if (has_nulls) {
        var write_offset: u32 = 0;
        for (arrays) |array| {
            if (array.null_count > 0) {
                const v = (array.validity orelse unreachable);

                var idx: u32 = array.offset;
                var w_idx: u32 = write_offset;
                while (idx < array.offset + array.len) : ({
                    idx += 1;
                    w_idx += 1;
                }) {
                    if (!bitmap.get(v, idx)) {
                        bitmap.unset(validity, w_idx);
                    }
                }
            }

            write_offset += array.len;
        }
    }

    return .{
        .field_names = field_names,
        .field_values = field_values,
        .offset = 0,
        .len = total_len,
        .null_count = total_null_count,
        .validity = if (has_nulls) validity else null,
    };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
/// Scratch alloc will be used to allocate intermediary slices, it can be deallocated after this function returns.
pub fn concat_fixed_size_list(fsl_t: data_type.FixedSizeListType, arrays: []const arr.FixedSizeListArray, alloc: Allocator, scratch_alloc: Allocator) Error!arr.FixedSizeListArray {
    var total_len: u32 = 0;
    var total_null_count: u32 = 0;

    for (arrays) |array| {
        std.debug.assert(array.item_width == fsl_t.item_width);

        total_len += array.len;
        total_null_count += array.null_count;
    }

    const has_nulls = total_null_count > 0;
    const bitmap_len = (total_len + 7) / 8;

    const validity: []u8 = if (has_nulls) has_n: {
        const v = try alloc.alloc(u8, bitmap_len);
        @memset(v, 0xff);
        break :has_n v;
    } else &.{};

    var write_offset: u32 = 0;
    for (arrays) |array| {
        if (array.null_count > 0) {
            const v = (array.validity orelse unreachable);

            var idx: u32 = array.offset;
            var w_idx: u32 = write_offset;
            while (idx < array.offset + array.len) : ({
                idx += 1;
                w_idx += 1;
            }) {
                if (!bitmap.get(v, idx)) {
                    bitmap.unset(validity, w_idx);
                }
            }
        }

        write_offset += array.len;
    }

    const inners = try scratch_alloc.alloc(arr.Array, arrays.len);
    for (0..arrays.len) |idx| {
        const array = arrays[idx];
        const iw: u32 = @intCast(array.item_width);
        const in = slice.slice(array.inner, array.offset * iw, array.len * iw);
        inners[idx] = in;
    }

    const inner = try alloc.create(arr.Array);
    inner.* = try concat(fsl_t.inner, inners, alloc, scratch_alloc);

    return .{
        .inner = inner,
        .len = total_len,
        .null_count = total_null_count,
        .validity = if (has_nulls) validity else null,
        .offset = 0,
        .item_width = fsl_t.item_width,
    };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
/// Scratch alloc will be used to allocate intermediary slices, it can be deallocated after this function returns.
pub fn concat_list(comptime index_t: arr.IndexType, inner_dt: data_type.DataType, arrays: []const arr.GenericListArray(index_t), alloc: Allocator, scratch_alloc: Allocator) Error!arr.GenericListArray(index_t) {
    const I = index_t.to_type();

    var total_len: u32 = 0;
    var total_null_count: u32 = 0;

    for (arrays) |array| {
        total_len += array.len;
        total_null_count += array.null_count;
    }

    const has_nulls = total_null_count > 0;
    const bitmap_len = (total_len + 7) / 8;

    const validity: []u8 = if (has_nulls) has_n: {
        const v = try alloc.alloc(u8, bitmap_len);
        @memset(v, 0xff);
        break :has_n v;
    } else &.{};
    const offsets = try alloc.alloc(I, total_len + 1);

    const inners = try scratch_alloc.alloc(arr.Array, arrays.len);

    var inner_offset: u32 = 0;
    var write_offset: u32 = 0;
    for (arrays, 0..) |array, arr_idx| {
        const input_start: usize = @intCast(array.offsets[array.offset]);
        const input_end: usize = @intCast(array.offsets[array.offset + array.len]);
        const input_len = input_end - input_start;

        inners[arr_idx] = slice.slice(array.inner, @intCast(input_start), @intCast(input_len));

        {
            var idx: u32 = array.offset;
            var w_idx: u32 = write_offset;
            const offset_diff: I = @as(I, @intCast(inner_offset)) - array.offsets[array.offset];
            while (idx < array.offset + array.len) : ({
                idx += 1;
                w_idx += 1;
            }) {
                offsets[w_idx] = array.offsets[idx] + offset_diff;
            }
        }

        if (array.null_count > 0) {
            const v = (array.validity orelse unreachable);

            var idx: u32 = array.offset;
            var w_idx: u32 = write_offset;
            while (idx < array.offset + array.len) : ({
                idx += 1;
                w_idx += 1;
            }) {
                if (!bitmap.get(v, idx)) {
                    bitmap.unset(validity, w_idx);
                }
            }
        }

        write_offset += array.len;
        inner_offset += @as(u32, @intCast(input_len));
    }

    offsets[total_len] = @intCast(inner_offset);

    const inner = try alloc.create(arr.Array);
    inner.* = try concat(inner_dt, inners, alloc, scratch_alloc);

    return .{
        .inner = inner,
        .offsets = offsets,
        .validity = if (has_nulls) validity else null,
        .len = total_len,
        .offset = 0,
        .null_count = total_null_count,
    };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
pub fn concat_null(arrays: []const arr.NullArray) arr.NullArray {
    var len: u32 = 0;

    for (arrays) |array| {
        len += array.len;
    }

    return .{ .len = len };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
/// Scratch alloc will be used to allocate intermediary slices, it can be deallocated after this function returns.
pub fn concat_interval(comptime backing_t: arr.IntervalType, arrays: []const arr.IntervalArray(backing_t), alloc: Allocator, scratch_alloc: Allocator) Error!arr.IntervalArray(backing_t) {
    const T = backing_t.to_type();

    const inner_arrays = try scratch_alloc.alloc(arr.PrimitiveArray(T), arrays.len);

    for (arrays, 0..) |array, idx| {
        inner_arrays[idx] = array.inner;
    }

    const inner = try concat_primitive(T, inner_arrays, alloc);

    return .{
        .inner = inner,
    };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
/// Scratch alloc will be used to allocate intermediary slices, it can be deallocated after this function returns.
pub fn concat_duration(unit: arr.TimestampUnit, arrays: []const arr.DurationArray, alloc: Allocator, scratch_alloc: Allocator) Error!arr.DurationArray {
    for (arrays) |array| {
        std.debug.assert(unit == array.unit);
    }

    const inner_arrays = try scratch_alloc.alloc(arr.Int64Array, arrays.len);

    for (arrays, 0..) |array, idx| {
        inner_arrays[idx] = array.inner;
    }

    const inner = try concat_primitive(i64, inner_arrays, alloc);

    return .{
        .unit = unit,
        .inner = inner,
    };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
/// Scratch alloc will be used to allocate intermediary slices, it can be deallocated after this function returns.
pub fn concat_timestamp(ts: arr.Timestamp, arrays: []const arr.TimestampArray, alloc: Allocator, scratch_alloc: Allocator) Error!arr.TimestampArray {
    for (arrays) |array| {
        std.debug.assert(ts.unit == array.ts.unit);

        if (ts.timezone) |tz| {
            const arr_tz = array.ts.timezone orelse unreachable;

            std.debug.assert(std.mem.eql(u8, tz, arr_tz));
        } else {
            std.debug.assert(array.ts.timezone == null);
        }
    }

    const inner_arrays = try scratch_alloc.alloc(arr.Int64Array, arrays.len);

    for (arrays, 0..) |array, idx| {
        inner_arrays[idx] = array.inner;
    }

    const inner = try concat_primitive(i64, inner_arrays, alloc);

    const out_ts = arr.Timestamp{
        .unit = ts.unit,
        .timezone = if (ts.timezone) |tz| tz_alloc: {
            const tz_out = try alloc.allocSentinel(u8, tz.len, 0);
            @memcpy(tz_out, tz);
            break :tz_alloc tz_out;
        } else null,
    };

    return .{
        .ts = out_ts,
        .inner = inner,
    };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
/// Scratch alloc will be used to allocate intermediary slices, it can be deallocated after this function returns.
pub fn concat_time(comptime backing_t: arr.IndexType, unit: arr.TimeArray(backing_t).Unit, arrays: []const arr.TimeArray(backing_t), alloc: Allocator, scratch_alloc: Allocator) Error!arr.TimeArray(backing_t) {
    const T = backing_t.to_type();

    for (arrays) |array| {
        std.debug.assert(unit == array.unit);
    }

    const inner_arrays = try scratch_alloc.alloc(arr.PrimitiveArray(T), arrays.len);

    for (arrays, 0..) |array, idx| {
        inner_arrays[idx] = array.inner;
    }

    const inner = try concat_primitive(T, inner_arrays, alloc);

    return .{
        .unit = unit,
        .inner = inner,
    };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
/// Scratch alloc will be used to allocate intermediary slices, it can be deallocated after this function returns.
pub fn concat_date(comptime backing_t: arr.IndexType, arrays: []const arr.DateArray(backing_t), alloc: Allocator, scratch_alloc: Allocator) Error!arr.DateArray(backing_t) {
    const T = backing_t.to_type();

    const inner_arrays = try scratch_alloc.alloc(arr.PrimitiveArray(T), arrays.len);

    for (arrays, 0..) |array, idx| {
        inner_arrays[idx] = array.inner;
    }

    const inner = try concat_primitive(T, inner_arrays, alloc);

    return .{
        .inner = inner,
    };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
/// Scratch alloc will be used to allocate intermediary slices, it can be deallocated after this function returns.
pub fn concat_utf8(comptime index_t: arr.IndexType, arrays: []const arr.GenericUtf8Array(index_t), alloc: Allocator, scratch_alloc: Allocator) Error!arr.GenericUtf8Array(index_t) {
    const inner_arrays = try scratch_alloc.alloc(arr.GenericBinaryArray(index_t), arrays.len);

    for (arrays, 0..) |array, idx| {
        inner_arrays[idx] = array.inner;
    }

    const inner = try concat_binary(index_t, inner_arrays, alloc);

    return .{
        .inner = inner,
    };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
/// Scratch alloc will be used to allocate intermediary slices, it can be deallocated after this function returns.
pub fn concat_utf8_view(arrays: []const arr.Utf8ViewArray, alloc: Allocator, scratch_alloc: Allocator) Error!arr.Utf8ViewArray {
    const inner_arrays = try scratch_alloc.alloc(arr.BinaryViewArray, arrays.len);

    for (arrays, 0..) |array, idx| {
        inner_arrays[idx] = array.inner;
    }

    const inner = try concat_binary_view(inner_arrays, alloc);

    return .{
        .inner = inner,
    };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
/// Scratch alloc will be used to allocate intermediary slices, it can be deallocated after this function returns.
pub fn concat_decimal(comptime d_int: arr.DecimalInt, params: arr.DecimalParams, arrays: []const arr.DecimalArray(d_int), alloc: Allocator, scratch_alloc: Allocator) Error!arr.DecimalArray(d_int) {
    const T = d_int.to_type();

    for (arrays) |array| {
        std.debug.assert(params.scale == array.params.scale and params.precision == array.params.precision);
    }

    const inner_arrays = try scratch_alloc.alloc(arr.PrimitiveArray(T), arrays.len);

    for (arrays, 0..) |array, idx| {
        inner_arrays[idx] = array.inner;
    }

    const inner = try concat_primitive(T, inner_arrays, alloc);

    return .{
        .params = params,
        .inner = inner,
    };
}

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
        @memcpy(values[write_offset .. write_offset + array.len], array.values[array.offset .. array.offset + array.len]);

        if (array.null_count > 0) {
            const v = (array.validity orelse unreachable);

            var idx: u32 = array.offset;
            var w_idx: u32 = write_offset;
            while (idx < array.offset + array.len) : ({
                idx += 1;
                w_idx += 1;
            }) {
                if (!bitmap.get(v, idx)) {
                    bitmap.unset(validity, w_idx);
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

        const start = array.offsets[array.offset];
        const end = array.offsets[array.offset + array.len];
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

    var data_offset: u32 = 0;
    var write_offset: u32 = 0;
    for (arrays) |array| {
        const input_start: usize = @intCast(array.offsets[array.offset]);
        const input_end: usize = @intCast(array.offsets[array.offset + array.len]);
        const input_len = input_end - input_start;
        @memcpy(data[data_offset .. data_offset + input_len], array.data[input_start..input_end]);

        {
            var idx: u32 = array.offset;
            var w_idx: u32 = write_offset;
            const offset_diff: I = @as(I, @intCast(data_offset)) - array.offsets[array.offset];
            while (idx < array.offset + array.len) : ({
                idx += 1;
                w_idx += 1;
            }) {
                offsets[w_idx] = array.offsets[idx] + offset_diff;
            }
        }

        if (array.null_count > 0) {
            const v = (array.validity orelse unreachable);

            var idx: u32 = array.offset;
            var w_idx: u32 = write_offset;
            while (idx < array.offset + array.len) : ({
                idx += 1;
                w_idx += 1;
            }) {
                if (!bitmap.get(v, idx)) {
                    bitmap.unset(validity, w_idx);
                }
            }
        }

        write_offset += array.len;
        data_offset += @as(u32, @intCast(input_len));
    }

    offsets[total_len] = @intCast(data_offset);

    return .{
        .len = total_len,
        .null_count = total_null_count,
        .validity = if (has_nulls) validity else null,
        .data = data[0..@intCast(data_offset)],
        .offsets = offsets,
        .offset = 0,
    };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
pub fn concat_binary_view(arrays: []const arr.BinaryViewArray, alloc: Allocator) Error!arr.BinaryViewArray {
    var total_len: u32 = 0;
    var total_null_count: u32 = 0;
    var total_data_len: usize = 0;
    var total_num_buffers: usize = 0;

    for (arrays) |array| {
        total_len += array.len;
        total_null_count += array.null_count;
        total_num_buffers += array.buffers.len;

        for (array.views) |v| {
            if (v.length > 12) {
                total_data_len += @as(u32, @intCast(v.length));
            }
        }
    }

    const has_nulls = total_null_count > 0;
    const bitmap_len = (total_len + 7) / 8;

    const buffers = try alloc.alloc([]const u8, total_num_buffers);

    const i32_max: usize = std.math.maxInt(i32);

    // This is the current buffer that will be inserted to the buffers slice
    var buffer = try alloc.alloc(u8, @min(i32_max, total_data_len));

    // keep the size of remaining data so we don't over allocate next buffers
    var remaining_data_len = total_data_len - buffer.len;

    const views = try alloc.alloc(arr.BinaryView, total_len);
    const validity: []u8 = if (has_nulls) has_n: {
        const v = try alloc.alloc(u8, bitmap_len);
        @memset(v, 0xff);
        break :has_n v;
    } else &.{};

    var buffer_idx: usize = 0;
    var buffer_offset: i32 = 0;
    var write_offset: u32 = 0;
    for (arrays) |array| {
        var wi: u32 = write_offset;
        for (array.views[array.offset .. array.offset + array.len]) |v| {
            if (v.length <= 12) {
                views[wi] = v;
            } else {
                // Handle the case where the current data buffer is at full capacity so we need to create another one
                if (@as(u32, @intCast(buffer_offset)) + @as(u32, @intCast(v.length)) > buffer.len) {
                    buffers[buffer_idx] = buffer;
                    buffer_idx += 1;
                    buffer_offset = 0;
                    // Don't allocate over i32_max because the view.buffer_index has to be of i32 type
                    buffer = try alloc.alloc(u8, @min(i32_max, remaining_data_len));
                    remaining_data_len -= buffer.len;
                }

                const boffset: u32 = @intCast(buffer_offset);
                const vlen: u32 = @intCast(v.length);
                @memcpy(buffer[boffset .. boffset + vlen], array.buffers[@as(u32, @intCast(v.buffer_idx))][@as(u32, @intCast(v.offset))..@as(u32, @intCast(v.offset + v.length))]);
                views[wi] = arr.BinaryView{
                    .length = v.length,
                    .prefix = v.prefix,
                    .offset = @intCast(buffer_offset),
                    .buffer_idx = @intCast(buffer_idx),
                };
                buffer_offset += v.length;
            }

            wi += 1;
        }

        if (array.null_count > 0) {
            const v = (array.validity orelse unreachable);

            var idx: u32 = array.offset;
            var w_idx: u32 = write_offset;
            while (idx < array.offset + array.len) : ({
                idx += 1;
                w_idx += 1;
            }) {
                if (!bitmap.get(v, idx)) {
                    bitmap.unset(validity, w_idx);
                }
            }
        }

        write_offset += array.len;
    }

    if (buffer_offset > 0) {
        buffers[buffer_idx] = buffer;
        buffer_idx += 1;
    }

    return .{
        .len = total_len,
        .null_count = total_null_count,
        .validity = if (has_nulls) validity else null,
        .buffers = buffers[0..buffer_idx],
        .views = views,
        .offset = 0,
    };
}

/// Concatenates given arrays, lifetime of the output array isn't tied to the input arrays
pub fn concat_bool(arrays: []const arr.BoolArray, alloc: Allocator) Error!arr.BoolArray {
    var total_len: u32 = 0;
    var total_null_count: u32 = 0;

    for (arrays) |array| {
        total_len += array.len;
        total_null_count += array.null_count;
    }

    const has_nulls = total_null_count > 0;
    const bitmap_len = (total_len + 7) / 8;

    const values = try alloc.alloc(u8, bitmap_len);
    @memset(values, 0);
    const validity: []u8 = if (has_nulls) has_n: {
        const v = try alloc.alloc(u8, bitmap_len);
        @memset(v, 0xff);
        break :has_n v;
    } else &.{};

    var write_offset: u32 = 0;
    for (arrays) |array| {
        {
            var idx: u32 = array.offset;
            var w_idx: u32 = write_offset;
            while (idx < array.offset + array.len) : ({
                idx += 1;
                w_idx += 1;
            }) {
                if (bitmap.get(array.values, idx)) {
                    bitmap.set(values, w_idx);
                }
            }
        }

        if (array.null_count > 0) {
            const v = (array.validity orelse unreachable);

            var idx: u32 = array.offset;
            var w_idx: u32 = write_offset;
            while (idx < array.offset + array.len) : ({
                idx += 1;
                w_idx += 1;
            }) {
                if (!bitmap.get(v, idx)) {
                    bitmap.unset(validity, w_idx);
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
pub fn concat_fixed_size_binary(byte_width: i32, arrays: []const arr.FixedSizeBinaryArray, alloc: Allocator) Error!arr.FixedSizeBinaryArray {
    const b_width: u32 = @intCast(byte_width);

    var total_len: u32 = 0;
    var total_null_count: u32 = 0;

    for (arrays) |array| {
        std.debug.assert(array.byte_width == byte_width);

        total_len += array.len;
        total_null_count += array.null_count;
    }

    const has_nulls = total_null_count > 0;
    const bitmap_len = (total_len + 7) / 8;

    const data = try alloc.alloc(u8, total_len * b_width);
    const validity: []u8 = if (has_nulls) has_n: {
        const v = try alloc.alloc(u8, bitmap_len);
        @memset(v, 0xff);
        break :has_n v;
    } else &.{};

    var write_offset: u32 = 0;
    for (arrays) |array| {
        const data_offset = write_offset * b_width;
        const input_offset = array.offset * b_width;
        const input_len = array.len * b_width;
        @memcpy(data[data_offset .. data_offset + input_len], array.data[input_offset .. input_offset + input_len]);

        if (array.null_count > 0) {
            const v = (array.validity orelse unreachable);

            var idx: u32 = array.offset;
            var w_idx: u32 = write_offset;
            while (idx < array.offset + array.len) : ({
                idx += 1;
                w_idx += 1;
            }) {
                if (!bitmap.get(v, idx)) {
                    bitmap.unset(validity, w_idx);
                }
            }
        }

        write_offset += array.len;
    }

    return .{
        .len = total_len,
        .null_count = total_null_count,
        .validity = if (has_nulls) validity else null,
        .data = data,
        .offset = 0,
        .byte_width = byte_width,
    };
}
