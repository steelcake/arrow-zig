const std = @import("std");
const assert = std.debug.assert;

const arr = @import("./array.zig");
const bitmap = @import("./bitmap.zig");
const slice = @import("./slice.zig").slice;
const get = @import("./get.zig");

fn assert_equal(left: anytype, right: anytype) void {
    if (left != right) {
        std.debug.panic("left != right. left = {any}, right = {any}", .{ left, right });
    }
}

pub fn equals_null(l: *const arr.NullArray, r: *const arr.NullArray) void {
    assert(l.len == r.len);
}

fn equals_impl(comptime array_t: type, l: *const array_t, r: *const array_t, comptime equals_fn: fn (l: *const array_t, r: *const array_t, li: u32, ri: u32) void) void {
    assert_equal(l.len, r.len);
    assert_equal(l.null_count, r.null_count);

    if (l.len == 0) {
        return;
    }

    if (l.null_count > 0) {
        const lv = l.validity orelse unreachable;
        const rv = r.validity orelse unreachable;

        var li: u32 = l.offset;
        var ri: u32 = r.offset;
        for (0..l.len) |_| {
            const lvalid = bitmap.get(lv.ptr, li);
            const rvalid = bitmap.get(rv.ptr, ri);

            assert(lvalid == rvalid);

            if (lvalid) {
                equals_fn(l, r, li, ri);
            }

            li += 1;
            ri += 1;
        }
    } else {
        var li: u32 = l.offset;
        var ri: u32 = r.offset;
        for (0..l.len) |_| {
            equals_fn(l, r, li, ri);

            li += 1;
            ri += 1;
        }
    }
}

fn bool_impl(l: *const arr.BoolArray, r: *const arr.BoolArray, li: u32, ri: u32) void {
    assert(get.get_bool(l.values.ptr, li) == get.get_bool(r.values.ptr, ri));
}

pub fn equals_bool(l: *const arr.BoolArray, r: *const arr.BoolArray) void {
    equals_impl(arr.BoolArray, l, r, bool_impl);
}

fn PrimitiveImpl(comptime T: type) type {
    return struct {
        fn eq(l: *const arr.PrimitiveArray(T), r: *const arr.PrimitiveArray(T), li: u32, ri: u32) void {
            assert_equal(get.get_primitive(T, l.values.ptr, li), get.get_primitive(T, r.values.ptr, ri));
        }
    };
}

pub fn equals_primitive(comptime T: type, l: *const arr.PrimitiveArray(T), r: *const arr.PrimitiveArray(T)) void {
    equals_impl(arr.PrimitiveArray(T), l, r, PrimitiveImpl(T).eq);
}

fn BinaryImpl(comptime index_type: arr.IndexType) type {
    return struct {
        fn eq(l: *const arr.GenericBinaryArray(index_type), r: *const arr.GenericBinaryArray(index_type), li: u32, ri: u32) void {
            const lvalue = get.get_binary(index_type, l.data.ptr, l.offsets.ptr, li);
            const rvalue = get.get_binary(index_type, r.data.ptr, r.offsets.ptr, ri);

            assert(std.mem.eql(u8, lvalue, rvalue));
        }
    };
}

pub fn equals_binary(comptime index_type: arr.IndexType, l: *const arr.GenericBinaryArray(index_type), r: *const arr.GenericBinaryArray(index_type)) void {
    equals_impl(arr.GenericBinaryArray(index_type), l, r, BinaryImpl(index_type).eq);
}

pub fn equals_utf8(comptime index_type: arr.IndexType, l: *const arr.GenericUtf8Array(index_type), r: *const arr.GenericUtf8Array(index_type)) void {
    equals_binary(index_type, &l.inner, &r.inner);
}

pub fn equals_decimal(comptime int: arr.DecimalInt, left: *const arr.DecimalArray(int), right: *const arr.DecimalArray(int)) void {
    assert(left.params.scale == right.params.scale or left.params.precision == right.params.precision);
    equals_primitive(int.to_type(), &left.inner, &right.inner);
}

fn binary_view_impl(l: *const arr.BinaryViewArray, r: *const arr.BinaryViewArray, li: u32, ri: u32) void {
    const lvalue = get.get_binary_view(l.buffers.ptr, l.views.ptr, li);
    const rvalue = get.get_binary_view(r.buffers.ptr, r.views.ptr, ri);

    if (!std.mem.eql(u8, lvalue, rvalue)) {
        std.debug.panic("{any} != {any}", .{ lvalue, rvalue });
    }
}

pub fn equals_binary_view(l: *const arr.BinaryViewArray, r: *const arr.BinaryViewArray) void {
    equals_impl(arr.BinaryViewArray, l, r, binary_view_impl);
}

pub fn equals_utf8_view(l: *const arr.Utf8ViewArray, r: *const arr.Utf8ViewArray) void {
    equals_binary_view(&l.inner, &r.inner);
}

fn fixed_size_binary_impl(l: *const arr.FixedSizeBinaryArray, r: *const arr.FixedSizeBinaryArray, li: u32, ri: u32) void {
    const lvalue = get.get_fixed_size_binary(l.data.ptr, l.byte_width, li);
    const rvalue = get.get_fixed_size_binary(r.data.ptr, r.byte_width, ri);

    assert(std.mem.eql(u8, lvalue, rvalue));
}

pub fn equals_fixed_size_binary(l: *const arr.FixedSizeBinaryArray, r: *const arr.FixedSizeBinaryArray) void {
    assert(l.byte_width == r.byte_width);
    equals_impl(arr.FixedSizeBinaryArray, l, r, fixed_size_binary_impl);
}

pub fn equals_date(comptime backing_t: arr.IndexType, l: *const arr.DateArray(backing_t), r: *const arr.DateArray(backing_t)) void {
    equals_primitive(backing_t.to_type(), &l.inner, &r.inner);
}

pub fn equals_time(comptime backing_t: arr.IndexType, l: *const arr.TimeArray(backing_t), r: *const arr.TimeArray(backing_t)) void {
    assert(l.unit == r.unit);
    equals_primitive(backing_t.to_type(), &l.inner, &r.inner);
}

pub fn equals_timestamp(l: *const arr.TimestampArray, r: *const arr.TimestampArray) void {
    if (l.ts.timezone) |ltz| {
        if (r.ts.timezone) |rtz| {
            assert(std.mem.eql(u8, ltz, rtz));
        } else {
            std.debug.panic("not eq", .{});
        }
    } else if (r.ts.timezone != null) {
        std.debug.panic("not eq", .{});
    }

    assert(l.ts.unit == r.ts.unit);

    equals_primitive(i64, &l.inner, &r.inner);
}

pub fn equals_duration(l: *const arr.DurationArray, r: *const arr.DurationArray) void {
    assert(l.unit == r.unit);
    equals_primitive(i64, &l.inner, &r.inner);
}

fn interval_day_time_impl(l: *const arr.PrimitiveArray([2]i32), r: *const arr.PrimitiveArray([2]i32), li: u32, ri: u32) void {
    const lvalue = get.get_primitive([2]i32, l.values.ptr, li);
    const rvalue = get.get_primitive([2]i32, r.values.ptr, ri);

    assert(lvalue[0] == rvalue[0] and lvalue[1] == rvalue[1]);
}

fn interval_month_day_nano_impl(l: *const arr.PrimitiveArray(arr.MonthDayNano), r: *const arr.PrimitiveArray(arr.MonthDayNano), li: u32, ri: u32) void {
    const lvalue = get.get_primitive(arr.MonthDayNano, l.values.ptr, li);
    const rvalue = get.get_primitive(arr.MonthDayNano, r.values.ptr, ri);

    assert(lvalue.months == rvalue.months and lvalue.days == rvalue.days and lvalue.nanoseconds == rvalue.nanoseconds);
}

pub fn equals_interval_month_day_nano(l: *const arr.IntervalMonthDayNanoArray, r: *const arr.IntervalMonthDayNanoArray) void {
    equals_impl(arr.PrimitiveArray(arr.MonthDayNano), &l.inner, &r.inner, interval_month_day_nano_impl);
}

pub fn equals_interval_day_time(l: *const arr.IntervalDayTimeArray, r: *const arr.IntervalDayTimeArray) void {
    equals_impl(arr.PrimitiveArray([2]i32), &l.inner, &r.inner, interval_day_time_impl);
}

pub fn equals_interval_year_month(l: *const arr.IntervalYearMonthArray, r: *const arr.IntervalYearMonthArray) void {
    equals_primitive(i32, &l.inner, &r.inner);
}

fn ListImpl(comptime index_type: arr.IndexType) type {
    return struct {
        fn eq(l: *const arr.GenericListArray(index_type), r: *const arr.GenericListArray(index_type), li: u32, ri: u32) void {
            const lvalue = get.get_list(index_type, l.inner, l.offsets.ptr, li);
            const rvalue = get.get_list(index_type, r.inner, r.offsets.ptr, ri);

            equals(&lvalue, &rvalue);
        }
    };
}

pub fn equals_list(comptime index_type: arr.IndexType, l: *const arr.GenericListArray(index_type), r: *const arr.GenericListArray(index_type)) void {
    assert(@intFromEnum(l.inner.*) == @intFromEnum(r.inner.*));

    equals_impl(arr.GenericListArray(index_type), l, r, ListImpl(index_type).eq);
}

fn ListViewImpl(comptime index_type: arr.IndexType) type {
    return struct {
        fn eq(l: *const arr.GenericListViewArray(index_type), r: *const arr.GenericListViewArray(index_type), li: u32, ri: u32) void {
            const lvalue = get.get_list_view(index_type, l.inner, l.offsets.ptr, l.sizes.ptr, li);
            const rvalue = get.get_list_view(index_type, r.inner, r.offsets.ptr, r.sizes.ptr, ri);
            equals(&lvalue, &rvalue);
        }
    };
}

pub fn equals_list_view(comptime index_type: arr.IndexType, l: *const arr.GenericListViewArray(index_type), r: *const arr.GenericListViewArray(index_type)) void {
    assert(@intFromEnum(l.inner.*) == @intFromEnum(r.inner.*));

    equals_impl(arr.GenericListViewArray(index_type), l, r, ListViewImpl(index_type).eq);
}

fn fixed_size_list_impl(l: *const arr.FixedSizeListArray, r: *const arr.FixedSizeListArray, li: u32, ri: u32) void {
    const lvalue = get.get_fixed_size_list(l.inner, l.item_width, li);
    const rvalue = get.get_fixed_size_list(r.inner, r.item_width, ri);

    equals(&lvalue, &rvalue);
}

pub fn equals_fixed_size_list(l: *const arr.FixedSizeListArray, r: *const arr.FixedSizeListArray) void {
    assert(@intFromEnum(l.inner.*) == @intFromEnum(r.inner.*));

    assert(l.item_width == r.item_width);

    equals_impl(arr.FixedSizeListArray, l, r, fixed_size_list_impl);
}

fn struct_impl(l: *const arr.StructArray, r: *const arr.StructArray, li: u32, ri: u32) void {
    for (0..l.field_names.len) |field_index| {
        const larr = &l.field_values[field_index];
        const rarr = &r.field_values[field_index];
        equals(&slice(larr, li, 1), &slice(rarr, ri, 1));
    }
}

pub fn equals_struct(l: *const arr.StructArray, r: *const arr.StructArray) void {
    assert(l.field_names.len == r.field_names.len);

    for (0..l.field_names.len) |i| {
        assert(std.mem.eql(u8, l.field_names[i], r.field_names[i]));
    }

    equals_impl(arr.StructArray, l, r, struct_impl);
}

fn map_impl(l: *const arr.MapArray, r: *const arr.MapArray, li: u32, ri: u32) void {
    const larr = get.get_map(l.entries, l.offsets.ptr, li);
    const rarr = get.get_map(r.entries, r.offsets.ptr, ri);

    equals_struct(&larr, &rarr);
}

pub fn equals_map(l: *const arr.MapArray, r: *const arr.MapArray) void {
    equals_impl(arr.MapArray, l, r, map_impl);
}

fn equals_field_names(l_field_names: []const [:0]const u8, r_field_names: []const [:0]const u8) void {
    assert_equal(l_field_names.len, r_field_names.len);
    for (0..l_field_names.len) |i| {
        if (!std.mem.eql(u8, l_field_names[i], r_field_names[i])) {
            std.debug.panic("left field_name: {any}, right field_name: {any}", .{ l_field_names[i], r_field_names[i] });
        }
    }
}

pub fn equals_sparse_union(l: *const arr.SparseUnionArray, r: *const arr.SparseUnionArray) void {
    assert(l.inner.len == r.inner.len);

    assert(std.mem.eql(i8, l.inner.type_id_set, r.inner.type_id_set));

    equals_field_names(l.inner.field_names, r.inner.field_names);

    var li: u32 = l.inner.offset;
    var ri: u32 = r.inner.offset;
    for (0..l.inner.len) |_| {
        const ltype_id = l.inner.type_ids.ptr[li];
        const rtype_id = r.inner.type_ids.ptr[ri];

        assert(ltype_id == rtype_id);

        const child_idx = for (0..l.inner.children.len) |i| {
            if (l.inner.type_id_set.ptr[i] == ltype_id) {
                break i;
            }
        } else {
            std.debug.panic("left type_ids: {any}, right type_ids: {any}, looking for: {any}", .{ l.inner.type_id_set, r.inner.type_id_set, ltype_id });
        };

        const lval = slice(&l.inner.children.ptr[child_idx], li, 1);
        const rval = slice(&r.inner.children.ptr[child_idx], ri, 1);
        equals(&lval, &rval);

        li += 1;
        ri += 1;
    }
}

pub fn equals_dense_union(l: *const arr.DenseUnionArray, r: *const arr.DenseUnionArray) void {
    assert(l.inner.len == r.inner.len);

    assert(std.mem.eql(i8, l.inner.type_id_set, r.inner.type_id_set));

    equals_field_names(l.inner.field_names, r.inner.field_names);

    var li: u32 = l.inner.offset;
    var ri: u32 = r.inner.offset;
    for (0..l.inner.len) |_| {
        const ltype_id = l.inner.type_ids.ptr[li];
        const rtype_id = r.inner.type_ids.ptr[ri];

        assert(ltype_id == rtype_id);

        const child_idx = for (0..l.inner.children.len) |i| {
            if (l.inner.type_id_set.ptr[i] == ltype_id) {
                break i;
            }
        } else {
            std.debug.panic("left type_ids: {any}, right type_ids: {any}, looking for: {any}", .{ l.inner.type_id_set, r.inner.type_id_set, ltype_id });
        };

        const loffset: u32 = @bitCast(l.offsets.ptr[li]);
        const roffset: u32 = @bitCast(r.offsets.ptr[ri]);

        const lval = slice(&l.inner.children.ptr[child_idx], loffset, 1);
        const rval = slice(&r.inner.children.ptr[child_idx], roffset, 1);
        equals(&lval, &rval);

        li += 1;
        ri += 1;
    }
}

fn dict_impl(comptime keys_t: type, l: *const keys_t, r: *const keys_t, l_values: *const arr.Array, r_values: *const arr.Array) void {
    assert(l.len == r.len and r.null_count == l.null_count);

    if (l.len == 0) {
        return;
    }

    if (l.null_count > 0) {
        const lv = l.validity orelse unreachable;
        const rv = r.validity orelse unreachable;

        var li: u32 = l.offset;
        var ri: u32 = r.offset;
        for (0..l.len) |_| {
            const lvalid = bitmap.get(lv.ptr, li);
            const rvalid = bitmap.get(rv.ptr, ri);

            assert(lvalid == rvalid);

            if (lvalid) {
                const larr = &slice(l_values, @intCast(l.values.ptr[li]), 1);
                const rarr = &slice(r_values, @intCast(r.values.ptr[ri]), 1);
                equals(larr, rarr);
            }

            li += 1;
            ri += 1;
        }
    } else {
        var li: u32 = l.offset;
        var ri: u32 = r.offset;
        for (0..l.len) |_| {
            const larr = &slice(l_values, @intCast(l.values.ptr[li]), 1);
            const rarr = &slice(r_values, @intCast(r.values.ptr[ri]), 1);
            equals(larr, rarr);

            li += 1;
            ri += 1;
        }
    }
}

pub fn equals_dict(l: *const arr.DictArray, r: *const arr.DictArray) void {
    assert(@intFromEnum(l.values.*) == @intFromEnum(r.values.*));

    assert(@intFromEnum(l.keys.*) == @intFromEnum(r.keys.*));

    const l_keys = slice(l.keys, l.offset, l.len);
    const r_keys = slice(r.keys, r.offset, r.len);

    switch (l_keys) {
        .i8 => |*lk| dict_impl(arr.Int8Array, lk, &r_keys.i8, l.values, r.values),
        .i16 => |*lk| dict_impl(arr.Int16Array, lk, &r_keys.i16, l.values, r.values),
        .i32 => |*lk| dict_impl(arr.Int32Array, lk, &r_keys.i32, l.values, r.values),
        .i64 => |*lk| dict_impl(arr.Int64Array, lk, &r_keys.i64, l.values, r.values),
        .u8 => |*lk| dict_impl(arr.UInt8Array, lk, &r_keys.u8, l.values, r.values),
        .u16 => |*lk| dict_impl(arr.UInt16Array, lk, &r_keys.u16, l.values, r.values),
        .u32 => |*lk| dict_impl(arr.UInt32Array, lk, &r_keys.u32, l.values, r.values),
        .u64 => |*lk| dict_impl(arr.UInt64Array, lk, &r_keys.u64, l.values, r.values),
        else => unreachable,
    }
}

fn ReeIter(comptime RunEndT: type) type {
    const T = RunEndT;

    return struct {
        const Self = @This();

        run_end_idx: u32,
        array: *const arr.RunEndArray,
        run_ends: *const arr.PrimitiveArray(T),
        prev: T,

        const Item = struct {
            val: arr.Array,
            count: u32,
        };

        fn init(array: *const arr.RunEndArray) Self {
            const run_ends = &@field(array.run_ends, @typeName(T));

            var run_end_idx: u32 = run_ends.offset;
            while (run_end_idx < run_ends.offset + run_ends.len) : (run_end_idx += 1) {
                if (run_ends.values[run_end_idx] > array.offset) {
                    break;
                }
            } else {
                std.debug.assert(run_ends.values[run_end_idx] == array.offset);
                std.debug.assert(array.len == 0);
            }

            return .{
                .run_end_idx = run_end_idx,
                .array = array,
                .run_ends = run_ends,
                .prev = @intCast(array.offset),
            };
        }

        fn next(self: *Self) ?Item {
            if (self.prev >= self.array.offset + self.array.len) {
                return null;
            }

            const re: T = @min(self.run_ends.values[self.run_end_idx], @as(T, @intCast(self.array.len + self.array.offset)));
            const count: T = re - self.prev;

            const out = Item{
                .count = @intCast(count),
                .val = slice(self.array.values, self.run_end_idx - self.run_ends.offset, 1),
            };

            self.prev = re;
            self.run_end_idx += 1;

            return out;
        }
    };
}

fn equals_run_end_encoded_impl(comptime RunEndT: type, l: *const arr.RunEndArray, r: *const arr.RunEndArray) void {
    const Iter = ReeIter(RunEndT);

    var l_iter = Iter.init(l);
    var r_iter = Iter.init(r);

    var l_item = l_iter.next() orelse unreachable;
    var r_item = r_iter.next() orelse unreachable;

    while (true) {
        equals(&l_item.val, &r_item.val);

        if (l_item.count == r_item.count) {
            l_item = l_iter.next() orelse {
                std.debug.assert(r_iter.next() == null);
                return;
            };
            r_item = r_iter.next() orelse unreachable;
        } else if (l_item.count < r_item.count) {
            r_item.count -= l_item.count;
            l_item = l_iter.next() orelse {
                std.log.warn("{any}\n{any}\n", .{ r_item, l_item });
                std.log.warn("{any}\n{any}\n{any}\n{any}\n", .{ r.*, l.*, r.run_ends.*, l.run_ends.* });
                unreachable;
            };
        } else if (l_item.count > r_item.count) {
            l_item.count -= r_item.count;
            r_item = r_iter.next() orelse unreachable;
        }
    }
}

pub fn equals_run_end_encoded(l: *const arr.RunEndArray, r: *const arr.RunEndArray) void {
    assert(@intFromEnum(l.values.*) == @intFromEnum(r.values.*));
    assert(@intFromEnum(l.run_ends.*) == @intFromEnum(r.run_ends.*));

    assert(l.len == r.len);

    if (l.len == 0) {
        return;
    }

    switch (l.run_ends.*) {
        .i16 => equals_run_end_encoded_impl(i16, l, r),
        .i32 => equals_run_end_encoded_impl(i32, l, r),
        .i64 => equals_run_end_encoded_impl(i64, l, r),
        else => unreachable,
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
pub fn equals(left: *const arr.Array, right: *const arr.Array) void {
    assert(@intFromEnum(left.*) == @intFromEnum(right.*));

    switch (left.*) {
        .null => |*l| equals_null(l, &right.null),
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
        .interval_year_month => |*l| equals_interval_year_month(l, &right.interval_year_month),
        .interval_day_time => |*l| equals_interval_day_time(l, &right.interval_day_time),
        .interval_month_day_nano => |*l| equals_interval_month_day_nano(l, &right.interval_month_day_nano),
        .list => |*l| equals_list(.i32, l, &right.list),
        .large_list => |*l| equals_list(.i64, l, &right.large_list),
        .list_view => |*l| equals_list_view(.i32, l, &right.list_view),
        .large_list_view => |*l| equals_list_view(.i64, l, &right.large_list_view),
        .fixed_size_list => |*l| equals_fixed_size_list(l, &right.fixed_size_list),
        .struct_ => |*l| equals_struct(l, &right.struct_),
        .map => |*l| equals_map(l, &right.map),
        .dense_union => |*l| equals_dense_union(l, &right.dense_union),
        .sparse_union => |*l| equals_sparse_union(l, &right.sparse_union),
        .run_end_encoded => |*l| equals_run_end_encoded(l, &right.run_end_encoded),
        .dict => |*l| equals_dict(l, &right.dict),
    }
}
