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

const Error = error{
    OutOfMemory,
};

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
