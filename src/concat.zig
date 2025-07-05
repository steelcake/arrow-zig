const std = @import("std");
const Allocator = std.mem.Allocator;
const arr = @import("./array.zig");
const slice = @import("./slice.zig");
const bitmap = @import("./bitmap.zig");
const equals = @import("./equals.zig");
const testing = std.testing;
const ArenaAllocator = std.heap.ArenaAllocator;
const builder = @import("./builder.zig");
const get = @import("./get.zig");
const data_type = @import("./data_type.zig");

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
        .dense_union => |_| unreachable,
        .sparse_union => |_| unreachable,
        .fixed_size_binary => |bw| {
            const a = try convert_arrays(arr.FixedSizeBinaryArray, "fixed_size_binary", arrays, scratch_alloc);
            return .{ .fixed_size_binary = try concat_fixed_size_binary(bw, a, alloc) };
        },
        .fixed_size_list => |_| unreachable,
        .map => |_| unreachable,
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
        .run_end_encoded => |_| unreachable,
        .binary_view => {
            const a = try convert_arrays(arr.BinaryViewArray, "binary_view", arrays, scratch_alloc);
            return .{ .binary_view = try concat_binary_view(a, alloc) };
        },
        .utf8_view => {
            const a = try convert_arrays(arr.Utf8ViewArray, "utf8_view", arrays, scratch_alloc);
            return .{ .utf8_view = try concat_utf8_view(a, alloc, scratch_alloc) };
        },
        .list_view => |_| unreachable,
        .large_list_view => |_| unreachable,
        .dict => |_| unreachable,
    }
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
            field_arrays[array_idx] = array.field_values[field_idx];
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
        const input_start: usize = @intCast(array.offsets.ptr[array.offset]);
        const input_end: usize = @intCast(array.offsets.ptr[array.offset + array.len]);
        const input_len = input_end - input_start;

        inners[arr_idx] = slice.slice(array.inner, @intCast(input_start), @intCast(input_end));

        {
            var idx: u32 = array.offset;
            var w_idx: u32 = write_offset;
            const offset_diff: I = @as(I, @intCast(inner_offset)) - array.offsets.ptr[array.offset];
            while (idx < array.offset + array.len) : ({
                idx += 1;
                w_idx += 1;
            }) {
                offsets.ptr[w_idx] = array.offsets.ptr[idx] +% offset_diff;
            }
        }

        if (array.null_count > 0) {
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
        inner_offset += @as(u32, @intCast(input_len));
    }

    offsets.ptr[total_len] = @intCast(inner_offset);

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
            const tz_out = try alloc.alloc(u8, tz.len);
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
        @memcpy(values.ptr[write_offset .. write_offset + array.len], array.values.ptr[array.offset .. array.offset + array.len]);

        if (array.null_count > 0) {
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

    var data_offset: u32 = 0;
    var write_offset: u32 = 0;
    for (arrays) |array| {
        const input_start: usize = @intCast(array.offsets.ptr[array.offset]);
        const input_end: usize = @intCast(array.offsets.ptr[array.offset + array.len]);
        const input_len = input_end - input_start;
        @memcpy(data.ptr[data_offset .. data_offset + input_len], array.data.ptr[input_start..input_end]);

        {
            var idx: u32 = array.offset;
            var w_idx: u32 = write_offset;
            const offset_diff: I = @as(I, @intCast(data_offset)) - array.offsets.ptr[array.offset];
            while (idx < array.offset + array.len) : ({
                idx += 1;
                w_idx += 1;
            }) {
                offsets.ptr[w_idx] = array.offsets.ptr[idx] +% offset_diff;
            }
        }

        if (array.null_count > 0) {
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
        data_offset += @as(u32, @intCast(input_len));
    }

    offsets.ptr[total_len] = @intCast(data_offset);

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

    for (arrays) |array| {
        total_len += array.len;
        total_null_count += array.null_count;

        for (array.views) |v| {
            if (v.length > 12) {
                total_data_len +%= @as(u32, @bitCast(v.length));
            }
        }
    }

    const has_nulls = total_null_count > 0;
    const bitmap_len = (total_len + 7) / 8;

    const buffer = try alloc.alloc(u8, total_data_len);
    const views = try alloc.alloc(arr.BinaryView, total_len);
    const validity: []u8 = if (has_nulls) has_n: {
        const v = try alloc.alloc(u8, bitmap_len);
        @memset(v, 0xff);
        break :has_n v;
    } else &.{};

    var buffer_offset: i32 = 0;
    var write_offset: u32 = 0;
    for (arrays) |array| {
        var wi: u32 = write_offset;
        for (array.views[array.offset .. array.offset + array.len]) |v| {
            if (v.length <= 12) {
                views.ptr[wi] = v;
            } else {
                views.ptr[wi] = arr.BinaryView{
                    .length = v.length,
                    .prefix = v.prefix,
                    .offset = @bitCast(buffer_offset),
                    .buffer_idx = 0,
                };
                buffer_offset += v.length;
            }

            wi +%= 1;
        }

        if (array.null_count > 0) {
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

    const buffers = try alloc.alloc([*]const u8, 1);
    buffers[0] = buffer.ptr;

    return .{
        .len = total_len,
        .null_count = total_null_count,
        .validity = if (has_nulls) validity else null,
        .buffers = buffers,
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
                if (bitmap.get(array.values.ptr, idx)) {
                    bitmap.set(values.ptr, w_idx);
                }
            }
        }

        if (array.null_count > 0) {
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
        @memcpy(data.ptr[data_offset .. data_offset + input_len], array.data.ptr[input_offset .. input_offset + input_len]);

        if (array.null_count > 0) {
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
        .data = data,
        .offset = 0,
        .byte_width = byte_width,
    };
}

test concat_utf8_view {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.Utf8ViewBuilder.from_slice(&.{ "1", "2", "3" }, false, alloc);
    const arr1 = try builder.Utf8ViewBuilder.from_slice(&.{ "4", "5", "6" }, false, alloc);
    const arr2 = try builder.Utf8ViewBuilder.from_slice(&.{}, false, alloc);
    const arr3 = try builder.Utf8ViewBuilder.from_slice(&.{ "7", "8", "9", "10" }, false, alloc);

    const result = try concat_utf8_view(&.{ arr0, arr1, arr2, arr3 }, alloc, alloc);
    const expected = try builder.Utf8ViewBuilder.from_slice(&.{ "1", "2", "3", "4", "5", "6", "7", "8", "9", "10" }, false, alloc);

    try equals.equals_utf8_view(&result, &expected);
}

test concat_utf8 {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.Utf8Builder.from_slice(&.{ "1", "2", "3" }, false, alloc);
    const arr1 = try builder.Utf8Builder.from_slice(&.{ "4", "5", "6" }, false, alloc);
    const arr2 = try builder.Utf8Builder.from_slice(&.{}, false, alloc);
    const arr3 = try builder.Utf8Builder.from_slice(&.{ "7", "8", "9", "10" }, false, alloc);

    const result = try concat_utf8(.i32, &.{ arr0, arr1, arr2, arr3 }, alloc, alloc);
    const expected = try builder.Utf8Builder.from_slice(&.{ "1", "2", "3", "4", "5", "6", "7", "8", "9", "10" }, false, alloc);

    try equals.equals_utf8(.i32, &result, &expected);
}

test concat_interval {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.IntervalYearMonthBuilder.from_slice(&.{ 1, 2, 3 }, false, alloc);
    const arr1 = try builder.IntervalYearMonthBuilder.from_slice(&.{ 4, 5, 6 }, false, alloc);
    const arr2 = try builder.IntervalYearMonthBuilder.from_slice(&.{}, false, alloc);
    const arr3 = try builder.IntervalYearMonthBuilder.from_slice(&.{ 7, 8, 9, 10 }, false, alloc);

    const result = try concat_interval(.year_month, &.{ arr0, arr1, arr2, arr3 }, alloc, alloc);
    const expected = try builder.IntervalYearMonthBuilder.from_slice(&.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, false, alloc);

    try equals.equals_interval_year_month(&result, &expected);
}

test concat_duration {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const unit = arr.TimestampUnit.microsecond;

    const arr0 = try builder.DurationBuilder.from_slice(unit, &.{ 1, 2, 3 }, false, alloc);
    const arr1 = try builder.DurationBuilder.from_slice(unit, &.{ 4, 5, 6 }, false, alloc);
    const arr2 = try builder.DurationBuilder.from_slice(unit, &.{}, false, alloc);
    const arr3 = try builder.DurationBuilder.from_slice(unit, &.{ 7, 8, 9, 10 }, false, alloc);

    const result = try concat_duration(unit, &.{ arr0, arr1, arr2, arr3 }, alloc, alloc);
    const expected = try builder.DurationBuilder.from_slice(unit, &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, false, alloc);

    try equals.equals_duration(&result, &expected);
}

test concat_timestamp {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const ts = arr.Timestamp{
        .unit = .nanosecond,
        .timezone = "hell",
    };

    const arr0 = try builder.TimestampBuilder.from_slice(ts, &.{ 1, 2, 3 }, false, alloc);
    const arr1 = try builder.TimestampBuilder.from_slice(ts, &.{ 4, 5, 6 }, false, alloc);
    const arr2 = try builder.TimestampBuilder.from_slice(ts, &.{}, false, alloc);
    const arr3 = try builder.TimestampBuilder.from_slice(ts, &.{ 7, 8, 9, 10 }, false, alloc);

    const result = try concat_timestamp(ts, &.{ arr0, arr1, arr2, arr3 }, alloc, alloc);
    const expected = try builder.TimestampBuilder.from_slice(ts, &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, false, alloc);

    try equals.equals_timestamp(&result, &expected);
}

test concat_time {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const unit = arr.Time32Unit.millisecond;

    const arr0 = try builder.Time32Builder.from_slice(unit, &.{ 1, 2, 3 }, false, alloc);
    const arr1 = try builder.Time32Builder.from_slice(unit, &.{ 4, 5, 6 }, false, alloc);
    const arr2 = try builder.Time32Builder.from_slice(unit, &.{}, false, alloc);
    const arr3 = try builder.Time32Builder.from_slice(unit, &.{ 7, 8, 9, 10 }, false, alloc);

    const result = try concat_time(.i32, unit, &.{ arr0, arr1, arr2, arr3 }, alloc, alloc);
    const expected = try builder.Time32Builder.from_slice(unit, &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, false, alloc);

    try equals.equals_time(.i32, &result, &expected);
}

test concat_date {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.Date64Builder.from_slice(&.{ 1, 2, 3 }, false, alloc);
    const arr1 = try builder.Date64Builder.from_slice(&.{ 4, 5, 6 }, false, alloc);
    const arr2 = try builder.Date64Builder.from_slice(&.{}, false, alloc);
    const arr3 = try builder.Date64Builder.from_slice(&.{ 7, 8, 9, 10 }, false, alloc);

    const result = try concat_date(.i64, &.{ arr0, arr1, arr2, arr3 }, alloc, alloc);
    const expected = try builder.Date64Builder.from_slice(&.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, false, alloc);

    try equals.equals_date(.i64, &result, &expected);
}

test concat_decimal {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const params = arr.DecimalParams{
        .scale = -10,
        .precision = 10,
    };

    const arr0 = try builder.Decimal128Builder.from_slice(params, &.{ 1, 2, 3 }, false, alloc);
    const arr1 = try builder.Decimal128Builder.from_slice(params, &.{ 4, 5, 6 }, false, alloc);
    const arr2 = try builder.Decimal128Builder.from_slice(params, &.{}, false, alloc);
    const arr3 = try builder.Decimal128Builder.from_slice(params, &.{ 7, 8, 9, 10 }, false, alloc);

    const result = try concat_decimal(.i128, params, &.{ arr0, arr1, arr2, arr3 }, alloc, alloc);
    const expected = try builder.Decimal128Builder.from_slice(params, &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, false, alloc);

    try equals.equals_decimal(.i128, &result, &expected);
}

test "concat_primitive empty-input" {
    const result = try concat_primitive(i32, &.{}, testing.allocator);
    const expected = arr.Int32Array{
        .offset = 0,
        .len = 0,
        .values = &.{},
        .validity = null,
        .null_count = 0,
    };

    try equals.equals_primitive(i32, &result, &expected);
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

test "concat_primitive nullable" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.Int32Builder.from_slice_opt(&.{ 1, 2, 3, null, null }, alloc);
    const arr1 = try builder.Int32Builder.from_slice(&.{ 4, 5, 6 }, false, alloc);
    const arr2 = try builder.Int32Builder.from_slice_opt(&.{null}, alloc);
    const arr3 = try builder.Int32Builder.from_slice_opt(&.{ null, 8, 9, 10 }, alloc);

    const result = try concat_primitive(i32, &.{ arr0, arr1, arr2, slice.slice_primitive(i32, &arr3, 1, 2) }, alloc);
    const expected = try builder.Int32Builder.from_slice_opt(&.{ 1, 2, 3, null, null, 4, 5, 6, null, 8, 9 }, alloc);

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

test "concat_binary empty-input" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const result = try concat_binary(.i32, &.{}, alloc);
    const expected = arr.BinaryArray{
        .offset = 0,
        .len = 0,
        .offsets = &.{0},
        .data = &.{},
        .validity = null,
        .null_count = 0,
    };

    try equals.equals_binary(.i32, &result, &expected);
}

test "concat_binary non-null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.BinaryBuilder.from_slice(&.{ "abc", "qq", "ww" }, false, alloc);
    const arr1 = try builder.BinaryBuilder.from_slice(&.{ "dd", "s", "xzc" }, false, alloc);
    const arr2 = try builder.BinaryBuilder.from_slice(&.{}, false, alloc);
    const arr3 = try builder.BinaryBuilder.from_slice(&.{"helloworld"}, false, alloc);

    const result = try concat_binary(.i32, &.{ arr0, arr1, arr2, arr3 }, alloc);
    const expected = try builder.BinaryBuilder.from_slice(&.{ "abc", "qq", "ww", "dd", "s", "xzc", "helloworld" }, false, alloc);

    try equals.equals_binary(.i32, &result, &expected);
}

test "concat_binary nullable" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.LargeBinaryBuilder.from_slice_opt(&.{ "abc", "qq", "ww", null, null }, alloc);
    const arr1 = try builder.LargeBinaryBuilder.from_slice(&.{ "dd", "s", "xzc" }, false, alloc);
    const arr2 = try builder.LargeBinaryBuilder.from_slice_opt(&.{null}, alloc);
    const arr3 = try builder.LargeBinaryBuilder.from_slice_opt(&.{ null, "helloworld", "gz", null }, alloc);

    const result = try concat_binary(.i64, &.{ arr0, arr1, arr2, slice.slice_binary(.i64, &arr3, 1, 2) }, alloc);
    const expected = try builder.LargeBinaryBuilder.from_slice_opt(&.{ "abc", "qq", "ww", null, null, "dd", "s", "xzc", null, "helloworld", "gz" }, alloc);

    try equals.equals_binary(.i64, &result, &expected);
}

test "concat_binary empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.BinaryBuilder.from_slice_opt(&.{}, alloc);
    const arr1 = try builder.BinaryBuilder.from_slice(&.{}, false, alloc);
    const arr2 = try builder.BinaryBuilder.from_slice_opt(&.{}, alloc);
    const arr3 = try builder.BinaryBuilder.from_slice(&.{}, false, alloc);

    const result = try concat_binary(.i32, &.{ arr0, arr1, arr2, arr3 }, alloc);
    const expected = try builder.BinaryBuilder.from_slice_opt(&.{}, alloc);

    try equals.equals_binary(.i32, &result, &expected);
}

test "concat_binary_view empty-input" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const result = try concat_binary_view(&.{}, alloc);
    const expected = arr.BinaryViewArray{
        .offset = 0,
        .len = 0,
        .views = &.{},
        .buffers = &.{},
        .validity = null,
        .null_count = 0,
    };

    try equals.equals_binary_view(&result, &expected);
}

test "concat_binary_view non-null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.BinaryViewBuilder.from_slice(&.{ "abc", "qq", "ww" }, false, alloc);
    const arr1 = try builder.BinaryViewBuilder.from_slice(&.{ "dd", "s", "xzc" }, false, alloc);
    const arr2 = try builder.BinaryViewBuilder.from_slice(&.{}, false, alloc);
    const arr3 = try builder.BinaryViewBuilder.from_slice(&.{"helloworld"}, false, alloc);

    const result = try concat_binary_view(&.{ arr0, arr1, arr2, arr3 }, alloc);
    const expected = try builder.BinaryViewBuilder.from_slice(&.{ "abc", "qq", "ww", "dd", "s", "xzc", "helloworld" }, false, alloc);

    try equals.equals_binary_view(&result, &expected);
}

test "concat_binary_view nullable" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.BinaryViewBuilder.from_slice_opt(&.{ "abc", "qq", "ww", null, null }, alloc);
    const arr1 = try builder.BinaryViewBuilder.from_slice(&.{ "dd", "s", "xzc" }, false, alloc);
    const arr2 = try builder.BinaryViewBuilder.from_slice_opt(&.{null}, alloc);
    const arr3 = try builder.BinaryViewBuilder.from_slice_opt(&.{ null, "helloworld", "gz", null }, alloc);

    const result = try concat_binary_view(&.{ arr0, arr1, arr2, slice.slice_binary_view(&arr3, 1, 2) }, alloc);
    const expected = try builder.BinaryViewBuilder.from_slice_opt(&.{ "abc", "qq", "ww", null, null, "dd", "s", "xzc", null, "helloworld", "gz" }, alloc);

    try equals.equals_binary_view(&result, &expected);
}

test "concat_binary_view empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.BinaryViewBuilder.from_slice_opt(&.{}, alloc);
    const arr1 = try builder.BinaryViewBuilder.from_slice(&.{}, false, alloc);
    const arr2 = try builder.BinaryViewBuilder.from_slice_opt(&.{}, alloc);
    const arr3 = try builder.BinaryViewBuilder.from_slice(&.{}, false, alloc);

    const result = try concat_binary_view(&.{ arr0, arr1, arr2, arr3 }, alloc);
    const expected = try builder.BinaryViewBuilder.from_slice_opt(&.{}, alloc);

    try equals.equals_binary_view(&result, &expected);
}

test "concat_bool empty-input" {
    const result = try concat_bool(&.{}, testing.allocator);
    const expected = arr.BoolArray{
        .offset = 0,
        .len = 0,
        .values = &.{},
        .validity = null,
        .null_count = 0,
    };

    try equals.equals_bool(&result, &expected);
}

test "concat_bool non-null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.BoolBuilder.from_slice(&.{ true, false, true }, false, alloc);
    const arr1 = try builder.BoolBuilder.from_slice(&.{ false, false }, false, alloc);
    const arr2 = try builder.BoolBuilder.from_slice(&.{}, false, alloc);
    const arr3 = try builder.BoolBuilder.from_slice(&.{ false, true, true, true }, false, alloc);

    const result = try concat_bool(&.{ arr0, arr1, arr2, arr3 }, alloc);
    const expected = try builder.BoolBuilder.from_slice(&.{ true, false, true, false, false, false, true, true, true }, false, alloc);

    try equals.equals_bool(&result, &expected);
}

test "concat_bool nullable" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.BoolBuilder.from_slice_opt(&.{ false, true, true, null, null }, alloc);
    const arr1 = try builder.BoolBuilder.from_slice(&.{ true, false, false }, true, alloc);
    const arr2 = try builder.BoolBuilder.from_slice_opt(&.{null}, alloc);
    const arr3 = try builder.BoolBuilder.from_slice_opt(&.{ null, true, false, false, true, null }, alloc);

    const result = try concat_bool(&.{ arr0, arr1, arr2, slice.slice_bool(&arr3, 2, 2) }, alloc);
    const expected = try builder.BoolBuilder.from_slice_opt(&.{ false, true, true, null, null, true, false, false, null, false, false }, alloc);

    try equals.equals_bool(&result, &expected);
}

test "concat_bool empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.BoolBuilder.from_slice_opt(&.{}, alloc);
    const arr1 = try builder.BoolBuilder.from_slice(&.{}, false, alloc);
    const arr2 = try builder.BoolBuilder.from_slice_opt(&.{}, alloc);
    const arr3 = try builder.BoolBuilder.from_slice(&.{}, false, alloc);

    const result = try concat_bool(&.{ arr0, arr1, arr2, arr3 }, alloc);
    const expected = try builder.BoolBuilder.from_slice_opt(&.{}, alloc);

    try equals.equals_bool(&result, &expected);
}

test "concat_fixed_size_binary empty-input" {
    const result = try concat_fixed_size_binary(69, &.{}, testing.allocator);
    const expected = arr.FixedSizeBinaryArray{
        .offset = 0,
        .len = 0,
        .data = &.{},
        .validity = null,
        .null_count = 0,
        .byte_width = 69,
    };

    try equals.equals_fixed_size_binary(&result, &expected);
}

test "concat_fixed_size_binary non-null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.FixedSizeBinaryBuilder.from_slice(3, &.{ "abc", "qqq", "www" }, false, alloc);
    const arr1 = try builder.FixedSizeBinaryBuilder.from_slice(3, &.{ "ddd", "sss", "xzc" }, false, alloc);
    const arr2 = try builder.FixedSizeBinaryBuilder.from_slice(3, &.{}, false, alloc);
    const arr3 = try builder.FixedSizeBinaryBuilder.from_slice(3, &.{"hww"}, false, alloc);

    const result = try concat_fixed_size_binary(3, &.{ arr0, arr1, arr2, arr3 }, alloc);
    const expected = try builder.FixedSizeBinaryBuilder.from_slice(3, &.{ "abc", "qqq", "www", "ddd", "sss", "xzc", "hww" }, false, alloc);

    try equals.equals_fixed_size_binary(&result, &expected);
}

test "concat_fixed_size_binary nullable" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.FixedSizeBinaryBuilder.from_slice_opt(3, &.{ "abc", "qqq", "www", null, null }, alloc);
    const arr1 = try builder.FixedSizeBinaryBuilder.from_slice(3, &.{ "ddd", "sss", "xzc" }, false, alloc);
    const arr2 = try builder.FixedSizeBinaryBuilder.from_slice_opt(3, &.{null}, alloc);
    const arr3 = try builder.FixedSizeBinaryBuilder.from_slice_opt(3, &.{ null, "hww", "ggz", null }, alloc);

    const result = try concat_fixed_size_binary(3, &.{ arr0, arr1, arr2, slice.slice_fixed_size_binary(&arr3, 1, 2) }, alloc);
    const expected = try builder.FixedSizeBinaryBuilder.from_slice_opt(3, &.{ "abc", "qqq", "www", null, null, "ddd", "sss", "xzc", null, "hww", "ggz" }, alloc);

    try equals.equals_fixed_size_binary(&result, &expected);
}

test "concat_fixed_size_binary empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const arr0 = try builder.FixedSizeBinaryBuilder.from_slice_opt(5, &.{}, alloc);
    const arr1 = try builder.FixedSizeBinaryBuilder.from_slice(5, &.{}, false, alloc);
    const arr2 = try builder.FixedSizeBinaryBuilder.from_slice_opt(5, &.{}, alloc);
    const arr3 = try builder.FixedSizeBinaryBuilder.from_slice(5, &.{}, false, alloc);

    const result = try concat_fixed_size_binary(5, &.{ arr0, arr1, arr2, arr3 }, alloc);
    const expected = try builder.FixedSizeBinaryBuilder.from_slice_opt(5, &.{}, alloc);

    try equals.equals_fixed_size_binary(&result, &expected);
}

test "concat_list empty-input" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const result = try concat_list(.i32, data_type.DataType{ .i8 = {} }, &.{}, alloc, alloc);
    const expected = arr.ListArray{
        .offset = 0,
        .len = 0,
        .validity = null,
        .null_count = 0,
        .inner = &.{ .i8 = arr.Int8Array{
            .offset = 0,
            .len = 0,
            .validity = null,
            .null_count = 0,
            .values = &.{},
        } },
        .offsets = &.{0},
    };

    try equals.equals_list(.i32, &result, &expected);
}

test "concat_list non-null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const inner = try builder.BinaryBuilder.from_slice(&.{ "abc", "qqq", "www", "ddd", "sss", "zxc", "hww" }, false, alloc);
    const inner_a = arr.Array{ .binary = inner };

    const arr0 = try builder.ListBuilder.from_slice(&.{ 2, 1, 1, 0, 3 }, false, &inner_a, alloc);
    const arr1 = try builder.ListBuilder.from_slice(&.{}, false, &inner_a, alloc);
    const arr2 = try builder.ListBuilder.from_slice(&.{ 0, 1, 2, 0, 3 }, false, &inner_a, alloc);

    const result = try concat_list(.i32, data_type.DataType.binary, &.{ arr0, arr1, arr2 }, alloc, alloc);
    const expected_inner = arr.Array{ .binary = try concat_binary(.i32, &.{ inner, slice.slice_binary(.i32, &inner, 0, 6) }, alloc) };
    const expected = try builder.ListBuilder.from_slice(&.{ 2, 1, 1, 0, 3, 0, 1, 2, 0, 3 }, false, &expected_inner, alloc);

    try equals.equals_list(.i32, &result, &expected);
}

test "concat_list nullable" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const inner = try builder.BinaryBuilder.from_slice(&.{ "abc", "qqq", "www", "ddd", "sss", "zxc", "hww" }, false, alloc);
    const inner_a = arr.Array{ .binary = inner };

    const arr0 = try builder.ListBuilder.from_slice_opt(&.{ 2, 1, 1, 0, 3, null, null }, &inner_a, alloc);
    const arr1 = try builder.ListBuilder.from_slice_opt(&.{null}, &inner_a, alloc);
    const arr2 = try builder.ListBuilder.from_slice_opt(&.{ null, 0, 1, 2, 0, 3, null }, &inner_a, alloc);

    const result = try concat_list(.i32, data_type.DataType.binary, &.{ arr0, arr1, arr2 }, alloc, alloc);
    const expected_inner = arr.Array{ .binary = try concat_binary(.i32, &.{ inner, slice.slice_binary(.i32, &inner, 0, 6) }, alloc) };
    const expected = try builder.ListBuilder.from_slice_opt(&.{ 2, 1, 1, 0, 3, null, null, null, null, 0, 1, 2, 0, 3, null }, &expected_inner, alloc);

    try equals.equals_list(.i32, &result, &expected);
}

test "concat_list empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const inner = try builder.BinaryBuilder.from_slice(&.{ "abc", "qqq", "www", "ddd", "sss", "zxc", "hww" }, false, alloc);
    const inner_a = arr.Array{ .binary = inner };

    const arr0 = try builder.ListBuilder.from_slice_opt(&.{}, &inner_a, alloc);
    const arr1 = try builder.ListBuilder.from_slice(&.{}, false, &inner_a, alloc);
    const arr2 = try builder.ListBuilder.from_slice_opt(&.{}, &inner_a, alloc);

    const result = try concat_list(.i32, data_type.DataType.binary, &.{ arr0, arr1, arr2 }, alloc, alloc);
    const expected_inner = arr.Array{ .binary = .{ .len = 0, .offset = 0, .null_count = 0, .offsets = &.{0}, .data = &.{}, .validity = null } };
    const expected = try builder.ListBuilder.from_slice_opt(&.{}, &expected_inner, alloc);

    try equals.equals_list(.i32, &result, &expected);
    try testing.expectEqual(result.inner.*.binary.data.len, 0);
    try testing.expectEqual(result.inner.*.binary.len, 0);
}

test "concat_struct empty-input" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const dt = data_type.StructType{
        .field_names = &.{ "asd", "qwe" },
        .field_types = &.{
            data_type.DataType{ .i32 = {} },
            data_type.DataType{ .i64 = {} },
        },
    };

    const result = try concat_struct(dt, &.{}, alloc, alloc);
    const expected = arr.StructArray{
        .offset = 0,
        .len = 0,
        .validity = null,
        .null_count = 0,
        .field_names = dt.field_names,
        .field_values = &.{
            arr.Array{ .i32 = .{ .validity = null, .len = 0, .values = &.{}, .offset = 0, .null_count = 0 } },
            arr.Array{ .i64 = .{ .validity = null, .len = 0, .values = &.{}, .offset = 0, .null_count = 0 } },
        },
    };

    try equals.equals_struct(&result, &expected);
}

test "concat_struct non-null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const field_names = &.{ "asd", "qwe" };
    const dt = data_type.StructType{
        .field_names = field_names,
        .field_types = &.{
            data_type.DataType{ .i32 = {} },
            data_type.DataType{ .i32 = {} },
        },
    };

    const asd = try builder.Int32Builder.from_slice(&.{ 1, 2, 3, 69 }, false, alloc);
    const asd_a = arr.Array{ .i32 = asd };
    const qwe = try builder.Int32Builder.from_slice(&.{ 1131, 0, 0, 0 }, false, alloc);
    const qwe_a = arr.Array{ .i32 = qwe };

    const arr0 = try builder.StructBuilder.from_slice(field_names, &.{ asd_a, qwe_a }, asd.len, false, alloc);
    const arr1 = try builder.StructBuilder.from_slice(field_names, &.{ qwe_a, asd_a }, asd.len, false, alloc);

    const asd_concat = arr.Array{ .i32 = try builder.Int32Builder.from_slice(&.{ 1, 2, 3, 69, 1131, 0, 0, 0 }, false, alloc) };
    const qwe_concat = arr.Array{ .i32 = try builder.Int32Builder.from_slice(&.{ 1131, 0, 0, 0, 1, 2, 3, 69 }, false, alloc) };
    const expected = try builder.StructBuilder.from_slice(field_names, &.{ asd_concat, qwe_concat }, asd.len * 2, false, alloc);

    const result = try concat_struct(dt, &.{ arr0, arr1 }, alloc, alloc);

    try equals.equals_struct(&result, &expected);
}

test "concat_struct nullable" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const field_names = &.{ "asd", "qwe" };
    const dt = data_type.StructType{
        .field_names = field_names,
        .field_types = &.{
            data_type.DataType{ .i32 = {} },
            data_type.DataType{ .i32 = {} },
        },
    };

    const asd = try builder.Int32Builder.from_slice(&.{ 1, 2, 3, 69 }, false, alloc);
    const asd_a = arr.Array{ .i32 = asd };
    const qwe = try builder.Int32Builder.from_slice(&.{ 1131, 0, 0, 0 }, false, alloc);
    const qwe_a = arr.Array{ .i32 = qwe };

    const arr0 = try builder.StructBuilder.from_slice_opt(field_names, &.{ asd_a, qwe_a }, &.{ false, true, false, true }, alloc);
    const arr1 = try builder.StructBuilder.from_slice_opt(field_names, &.{ qwe_a, asd_a }, &.{ false, false, true, false }, alloc);

    const asd_concat = arr.Array{ .i32 = try builder.Int32Builder.from_slice(&.{ 0, 2, 0, 69, 1131, 0, 0, 0 }, false, alloc) };
    const qwe_concat = arr.Array{ .i32 = try builder.Int32Builder.from_slice(&.{ 1131, 0, 0, 0, 1, 2, 3, 1131 }, false, alloc) };
    const expected = try builder.StructBuilder.from_slice_opt(field_names, &.{ asd_concat, qwe_concat }, &.{ false, true, false, true, false, false, true, false }, alloc);

    const result = try concat_struct(dt, &.{ arr0, arr1 }, alloc, alloc);

    try equals.equals_struct(&result, &expected);
}

test "concat_struct empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const field_names = &.{ "asd", "qwe" };
    const dt = data_type.StructType{
        .field_names = field_names,
        .field_types = &.{
            data_type.DataType{ .i32 = {} },
            data_type.DataType{ .i32 = {} },
        },
    };

    const asd = try builder.Int32Builder.from_slice(&.{}, false, alloc);
    const asd_a = arr.Array{ .i32 = asd };
    const qwe = try builder.Int32Builder.from_slice(&.{}, false, alloc);
    const qwe_a = arr.Array{ .i32 = qwe };

    const arr0 = try builder.StructBuilder.from_slice(field_names, &.{ asd_a, qwe_a }, 0, false, alloc);
    const arr1 = try builder.StructBuilder.from_slice(field_names, &.{ qwe_a, asd_a }, 0, false, alloc);

    const asd_concat = arr.Array{ .i32 = try builder.Int32Builder.from_slice(&.{}, false, alloc) };
    const qwe_concat = arr.Array{ .i32 = try builder.Int32Builder.from_slice(&.{}, false, alloc) };
    const expected = try builder.StructBuilder.from_slice(field_names, &.{ asd_concat, qwe_concat }, 0, false, alloc);

    const result = try concat_struct(dt, &.{ arr0, arr1 }, alloc, alloc);

    try equals.equals_struct(&result, &expected);
}
