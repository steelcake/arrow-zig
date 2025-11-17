const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const ArenaAllocator = std.heap.ArenaAllocator;

const arr = @import("./array.zig");
const bitmap = @import("./bitmap.zig");
const get = @import("./get.zig");

const OffsetLen = struct {
    null_count: u32,
    offset: u32,
    len: u32,
};

fn slice_impl(validity: ?[]const u8, base: OffsetLen, offset: u32, len: u32) OffsetLen {
    if (base.len < offset + len) {
        std.debug.panic("len is {} but offset+new_len is {}", .{ base.len, offset + len });
    }

    var out = OffsetLen{ .offset = base.offset + offset, .len = len, .null_count = 0 };
    if (base.null_count > 0) {
        const v = validity orelse unreachable;
        out.null_count = bitmap.count_nulls(v, out.offset, out.len);
    }

    return out;
}

pub fn slice_null(array: *const arr.NullArray, offset: u32, len: u32) arr.NullArray {
    const offset_len = slice_impl(null, .{ .offset = 0, .len = array.len, .null_count = 0 }, offset, len);
    return arr.NullArray{ .len = offset_len.len };
}

pub fn slice_primitive(comptime T: type, array: *const arr.PrimitiveArray(T), offset: u32, len: u32) arr.PrimitiveArray(T) {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.PrimitiveArray(T){
        .values = array.values,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_binary(comptime index_type: arr.IndexType, array: *const arr.GenericBinaryArray(index_type), offset: u32, len: u32) arr.GenericBinaryArray(index_type) {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.GenericBinaryArray(index_type){
        .data = array.data,
        .offsets = array.offsets,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_utf8(comptime index_type: arr.IndexType, array: *const arr.GenericUtf8Array(index_type), offset: u32, len: u32) arr.GenericUtf8Array(index_type) {
    return .{ .inner = slice_binary(index_type, &array.inner, offset, len) };
}

pub fn slice_bool(array: *const arr.BoolArray, offset: u32, len: u32) arr.BoolArray {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.BoolArray{
        .values = array.values,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_binary_view(array: *const arr.BinaryViewArray, offset: u32, len: u32) arr.BinaryViewArray {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.BinaryViewArray{
        .views = array.views,
        .buffers = array.buffers,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_utf8_view(array: *const arr.Utf8ViewArray, offset: u32, len: u32) arr.Utf8ViewArray {
    return .{ .inner = slice_binary_view(&array.inner, offset, len) };
}

pub fn slice_fixed_size_binary(array: *const arr.FixedSizeBinaryArray, offset: u32, len: u32) arr.FixedSizeBinaryArray {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.FixedSizeBinaryArray{
        .data = array.data,
        .byte_width = array.byte_width,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_list(comptime index_type: arr.IndexType, array: *const arr.GenericListArray(index_type), offset: u32, len: u32) arr.GenericListArray(index_type) {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.GenericListArray(index_type){
        .inner = array.inner,
        .offsets = array.offsets,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_list_view(comptime index_type: arr.IndexType, array: *const arr.GenericListViewArray(index_type), offset: u32, len: u32) arr.GenericListViewArray(index_type) {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.GenericListViewArray(index_type){
        .inner = array.inner,
        .sizes = array.sizes,
        .offsets = array.offsets,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_fixed_size_list(array: *const arr.FixedSizeListArray, offset: u32, len: u32) arr.FixedSizeListArray {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.FixedSizeListArray{
        .inner = array.inner,
        .item_width = array.item_width,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_struct(array: *const arr.StructArray, offset: u32, len: u32) arr.StructArray {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.StructArray{
        .field_names = array.field_names,
        .field_values = array.field_values,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_map(array: *const arr.MapArray, offset: u32, len: u32) arr.MapArray {
    const offset_len = slice_impl(array.validity, .{ .offset = array.offset, .len = array.len, .null_count = array.null_count }, offset, len);
    return arr.MapArray{
        .entries = array.entries,
        .offsets = array.offsets,
        .keys_are_sorted = array.keys_are_sorted,
        .validity = array.validity,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .null_count = offset_len.null_count,
    };
}

pub fn slice_union(array: *const arr.UnionArray, offset: u32, len: u32) arr.UnionArray {
    const offset_len = slice_impl(null, .{ .offset = array.offset, .len = array.len, .null_count = 0 }, offset, len);
    return arr.UnionArray{
        .type_ids = array.type_ids,
        .children = array.children,
        .type_id_set = array.type_id_set,
        .field_names = array.field_names,
        .len = offset_len.len,
        .offset = offset_len.offset,
    };
}

pub fn slice_sparse_union(array: *const arr.SparseUnionArray, offset: u32, len: u32) arr.SparseUnionArray {
    return arr.SparseUnionArray{ .inner = slice_union(&array.inner, offset, len) };
}

pub fn slice_dense_union(array: *const arr.DenseUnionArray, offset: u32, len: u32) arr.DenseUnionArray {
    return arr.DenseUnionArray{ .inner = slice_union(&array.inner, offset, len), .offsets = array.offsets };
}

pub fn slice_run_end_encoded(array: *const arr.RunEndArray, offset: u32, len: u32) arr.RunEndArray {
    const offset_len = slice_impl(null, .{ .offset = array.offset, .len = array.len, .null_count = 0 }, offset, len);
    return arr.RunEndArray{
        .values = array.values,
        .run_ends = array.run_ends,
        .len = offset_len.len,
        .offset = offset_len.offset,
    };
}

pub fn slice_decimal(comptime int: arr.DecimalInt, array: *const arr.DecimalArray(int), offset: u32, len: u32) arr.DecimalArray(int) {
    return .{ .inner = slice_primitive(int.to_type(), &array.inner, offset, len), .params = array.params };
}

pub fn slice_date(comptime backing_t: arr.IndexType, array: *const arr.DateArray(backing_t), offset: u32, len: u32) arr.DateArray(backing_t) {
    return .{ .inner = slice_primitive(backing_t.to_type(), &array.inner, offset, len) };
}

pub fn slice_time(comptime backing_t: arr.IndexType, array: *const arr.TimeArray(backing_t), offset: u32, len: u32) arr.TimeArray(backing_t) {
    return .{ .inner = slice_primitive(backing_t.to_type(), &array.inner, offset, len), .unit = array.unit };
}

pub fn slice_timestamp(array: *const arr.TimestampArray, offset: u32, len: u32) arr.TimestampArray {
    return .{ .inner = slice_primitive(i64, &array.inner, offset, len), .ts = array.ts };
}

pub fn slice_duration(array: *const arr.DurationArray, offset: u32, len: u32) arr.DurationArray {
    return .{ .inner = slice_primitive(i64, &array.inner, offset, len), .unit = array.unit };
}

pub fn slice_interval(comptime interval_t: arr.IntervalType, array: *const arr.IntervalArray(interval_t), offset: u32, len: u32) arr.IntervalArray(interval_t) {
    return .{ .inner = slice_primitive(interval_t.to_type(), &array.inner, offset, len) };
}

pub fn slice_dict(array: *const arr.DictArray, offset: u32, len: u32) arr.DictArray {
    const offset_len = slice_impl(null, .{ .offset = array.offset, .len = array.len, .null_count = 0 }, offset, len);
    return arr.DictArray{
        .values = array.values,
        .keys = array.keys,
        .len = offset_len.len,
        .offset = offset_len.offset,
        .is_ordered = array.is_ordered,
    };
}

pub fn slice(array: *const arr.Array, offset: u32, len: u32) arr.Array {
    return switch (array.*) {
        .null => |*a| .{ .null = slice_null(a, offset, len) },
        .i8 => |*a| .{ .i8 = slice_primitive(i8, a, offset, len) },
        .i16 => |*a| .{ .i16 = slice_primitive(i16, a, offset, len) },
        .i32 => |*a| .{ .i32 = slice_primitive(i32, a, offset, len) },
        .i64 => |*a| .{ .i64 = slice_primitive(i64, a, offset, len) },
        .u8 => |*a| .{ .u8 = slice_primitive(u8, a, offset, len) },
        .u16 => |*a| .{ .u16 = slice_primitive(u16, a, offset, len) },
        .u32 => |*a| .{ .u32 = slice_primitive(u32, a, offset, len) },
        .u64 => |*a| .{ .u64 = slice_primitive(u64, a, offset, len) },
        .f16 => |*a| .{ .f16 = slice_primitive(f16, a, offset, len) },
        .f32 => |*a| .{ .f32 = slice_primitive(f32, a, offset, len) },
        .f64 => |*a| .{ .f64 = slice_primitive(f64, a, offset, len) },
        .binary => |*a| .{ .binary = slice_binary(.i32, a, offset, len) },
        .large_binary => |*a| .{ .large_binary = slice_binary(.i64, a, offset, len) },
        .utf8 => |*a| .{ .utf8 = slice_utf8(.i32, a, offset, len) },
        .large_utf8 => |*a| .{ .large_utf8 = slice_utf8(.i64, a, offset, len) },
        .bool => |*a| .{ .bool = slice_bool(a, offset, len) },
        .binary_view => |*a| .{ .binary_view = slice_binary_view(a, offset, len) },
        .utf8_view => |*a| .{ .utf8_view = slice_utf8_view(a, offset, len) },
        .decimal32 => |*a| .{ .decimal32 = slice_decimal(.i32, a, offset, len) },
        .decimal64 => |*a| .{ .decimal64 = slice_decimal(.i64, a, offset, len) },
        .decimal128 => |*a| .{ .decimal128 = slice_decimal(.i128, a, offset, len) },
        .decimal256 => |*a| .{ .decimal256 = slice_decimal(.i256, a, offset, len) },
        .fixed_size_binary => |*a| .{ .fixed_size_binary = slice_fixed_size_binary(a, offset, len) },
        .date32 => |*a| .{ .date32 = slice_date(.i32, a, offset, len) },
        .date64 => |*a| .{ .date64 = slice_date(.i64, a, offset, len) },
        .time32 => |*a| .{ .time32 = slice_time(.i32, a, offset, len) },
        .time64 => |*a| .{ .time64 = slice_time(.i64, a, offset, len) },
        .timestamp => |*a| .{ .timestamp = slice_timestamp(a, offset, len) },
        .duration => |*a| .{ .duration = slice_duration(a, offset, len) },
        .interval_year_month => |*a| .{ .interval_year_month = slice_interval(.year_month, a, offset, len) },
        .interval_day_time => |*a| .{ .interval_day_time = slice_interval(.day_time, a, offset, len) },
        .interval_month_day_nano => |*a| .{ .interval_month_day_nano = slice_interval(.month_day_nano, a, offset, len) },
        .list => |*a| .{ .list = slice_list(.i32, a, offset, len) },
        .large_list => |*a| .{ .large_list = slice_list(.i64, a, offset, len) },
        .list_view => |*a| .{ .list_view = slice_list_view(.i32, a, offset, len) },
        .large_list_view => |*a| .{ .large_list_view = slice_list_view(.i64, a, offset, len) },
        .fixed_size_list => |*a| .{ .fixed_size_list = slice_fixed_size_list(a, offset, len) },
        .struct_ => |*a| .{ .struct_ = slice_struct(a, offset, len) },
        .map => |*a| .{ .map = slice_map(a, offset, len) },
        .dense_union => |*a| .{ .dense_union = slice_dense_union(a, offset, len) },
        .sparse_union => |*a| .{ .sparse_union = slice_sparse_union(a, offset, len) },
        .run_end_encoded => |*a| .{ .run_end_encoded = slice_run_end_encoded(a, offset, len) },
        .dict => |*a| .{ .dict = slice_dict(a, offset, len) },
    };
}

/// Remove top level offset from run_end_encoded array. This is necessary to use inner arrays without slicing them according to top level offset
///
/// This function is needed instead of just slicing inner arrays like other data types because
/// top level offset/len of run_end_encoded isn't in terms of the length of run_ends array but absolute.
pub fn normalize_run_end_encoded(array: *const arr.RunEndArray, lift: u32, alloc: Allocator) error{OutOfMemory}!arr.RunEndArray {
    if (array.len == 0) {
        const run_ends = try alloc.create(arr.Array);
        const values = try alloc.create(arr.Array);
        run_ends.* = slice(array.run_ends, 0, 0);
        values.* = slice(array.values, 0, 0);
        return arr.RunEndArray{
            .len = 0,
            .offset = 0,
            .run_ends = run_ends,
            .values = values,
        };
    }

    return switch (array.run_ends.*) {
        .i16 => |*a| try normalize_run_end_encoded_inner_impl(i16, array, a, lift, alloc),
        .i32 => |*a| try normalize_run_end_encoded_inner_impl(i32, array, a, lift, alloc),
        .i64 => |*a| try normalize_run_end_encoded_inner_impl(i64, array, a, lift, alloc),
        else => unreachable,
    };
}

fn normalize_run_end_encoded_inner_impl(
    comptime T: type,
    array: *const arr.RunEndArray,
    run_ends: *const arr.PrimitiveArray(T),
    lift: u32,
    alloc: Allocator,
) error{OutOfMemory}!arr.RunEndArray {
    std.debug.assert(run_ends.null_count == 0);
    std.debug.assert(array.len > 0);

    const run_ends_v = run_ends.values[run_ends.offset .. run_ends.offset + run_ends.len];

    const offset_re: T = @intCast(array.offset);
    const len_re: T = @intCast(array.len);

    // find start
    var idx: u32 = 0;
    const inner_start: u32 = while (idx < run_ends_v.len) : (idx += 1) {
        const run_end = run_ends_v[idx];

        if (run_end > offset_re) {
            break idx;
        }
    } else {
        @panic("offset out of range\n");
    };

    const inner_end: u32 = while (idx < run_ends_v.len) : (idx += 1) {
        const run_end = run_ends_v[idx];

        if (run_end - offset_re >= len_re) {
            break idx + 1;
        }
    } else {
        @panic("len out of range/n");
    };

    const new_values = try alloc.create(arr.Array);
    new_values.* = slice(array.values, inner_start, inner_end - inner_start);

    if (run_ends_v[inner_start] == offset_re and run_ends_v[inner_end] - run_ends_v[inner_start] == len_re) {
        const new_run_ends = try alloc.create(arr.Array);
        new_run_ends.* = slice(array.run_ends, inner_start, inner_end - inner_start);
        return arr.RunEndArray{
            .run_ends = new_run_ends,
            .values = new_values,
            .len = array.len,
            .offset = 0,
        };
    }

    const new_run_ends = try alloc.alloc(T, inner_end - inner_start);
    const src_run_ends = run_ends_v[inner_start..inner_end];
    idx = 0;
    const lift_re: T = @intCast(lift);
    while (idx < new_run_ends.len) : (idx += 1) {
        new_run_ends[idx] = src_run_ends[idx] - offset_re + lift_re;
    }
    new_run_ends[new_run_ends.len - 1] = len_re + lift_re;

    const new_run_ends_arr = try alloc.create(arr.Array);
    new_run_ends_arr.* = @unionInit(arr.Array, @typeName(T), .{
        .values = new_run_ends,
        .len = inner_end - inner_start,
        .offset = 0,
        .null_count = 0,
        .validity = null,
    });

    return arr.RunEndArray{
        .len = array.len,
        .offset = 0,
        .run_ends = new_run_ends_arr,
        .values = new_values,
    };
}
