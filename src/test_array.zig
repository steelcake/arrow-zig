const std = @import("std");
const Allocator = std.mem.Allocator;

const arr = @import("./array.zig");
const slice = @import("./slice.zig");
const builder = @import("./builder.zig");

pub fn make_array(id: u8, allocator: Allocator) !arr.Array {
    return switch (id) {
        0 => slice.slice(&.{ .i8 = try builder.Int8Builder.from_slice_opt(&.{ 111, 1, -1, 69, null, -69, null }, allocator) }, 1, 6),
        1 => .{ .i16 = try builder.Int16Builder.from_slice_opt(&.{ 1, -1, 69, null, -69, null }, allocator) },
        2 => .{ .i32 = try builder.Int32Builder.from_slice_opt(&.{ 1, -1, 69, null, -69, null }, allocator) },
        3 => .{ .i64 = try builder.Int64Builder.from_slice_opt(&.{ 1, -1, 69, null, -69, null }, allocator) },
        4 => .{ .u8 = try builder.UInt8Builder.from_slice(&.{ 1, 1, 69, 69 }, false, allocator) },
        5 => .{ .u16 = try builder.UInt16Builder.from_slice_opt(&.{ 1, 1, null, null, 69, 69 }, allocator) },
        6 => try make_u32(allocator),
        7 => try make_u64(allocator),
        8 => .{ .null = .{ .len = 69 } },
        9 => .{ .bool = try builder.BoolBuilder.from_slice_opt(&.{ true, false, null }, allocator) },
        10 => .{ .f32 = try builder.Float32Builder.from_slice_opt(&.{ 1, -1, 69, null, -69, null }, allocator) },
        11 => .{ .f64 = try builder.Float64Builder.from_slice_opt(&.{ 1, -1, 69, null, -69, null }, allocator) },
        12 => .{ .binary = try builder.BinaryBuilder.from_slice_opt(&.{ "hello", "world", null }, allocator) },
        13 => .{ .large_binary = try builder.LargeBinaryBuilder.from_slice_opt(&.{ "hello", "world", null }, allocator) },
        14 => .{ .utf8 = try builder.Utf8Builder.from_slice_opt(&.{ "hello", "world", null }, allocator) },
        15 => .{ .large_utf8 = try builder.LargeUtf8Builder.from_slice_opt(&.{ "hello", "world", null }, allocator) },
        16 => .{ .decimal128 = try builder.Decimal128Builder.from_slice(.{ .precision = 31, .scale = -31 }, &.{ 1, 2, 3, 4, 69 }, true, allocator) },
        17 => .{ .decimal256 = try builder.Decimal256Builder.from_slice(.{ .precision = 31, .scale = -31 }, &.{ 69, -69 }, false, allocator) },
        18 => .{ .date32 = try builder.Date32Builder.from_slice(&.{ 69, 69, 11, 15 }, false, allocator) },
        19 => .{ .date64 = try builder.Date64Builder.from_slice(&.{ 69, 69, 11, 15 }, false, allocator) },
        20 => .{ .time32 = try builder.Time32Builder.from_slice_opt(.second, &.{ 69, 11, null, null }, allocator) },
        21 => .{ .time64 = try builder.Time64Builder.from_slice_opt(.nanosecond, &.{ 69, 11, null, null }, allocator) },
        22 => .{ .timestamp = try builder.TimestampBuilder.from_slice_opt(.{ .unit = .second, .timezone = "Africa/Abidjan" }, &.{ 123, null }, allocator) },
        23 => .{ .interval_year_month = try builder.IntervalYearMonthBuilder.from_slice_opt(&.{ 9, null }, allocator) },
        24 => .{ .interval_day_time = try builder.IntervalDayTimeBuilder.from_slice_opt(&.{ null, .{ 69, 11 } }, allocator) },
        25 => .{ .interval_month_day_nano = try builder.IntervalMonthDayNanoBuilder.from_slice_opt(&.{ null, null, null, null, .{ .days = 69, .months = 69, .nanoseconds = 1131 } }, allocator) },
        26 => try make_list(.i32, allocator),
        27 => try make_list(.i64, allocator),
        28 => try make_struct(allocator),
        29 => try make_dense_union(allocator),
        30 => try make_sparse_union(allocator),
        31 => .{ .fixed_size_binary = try builder.FixedSizeBinaryBuilder.from_slice_opt(4, &.{ "anan", "zaaa", null, "xddd" }, allocator) },
        32 => try make_fixed_size_list(allocator),
        33 => try make_map(allocator),
        34 => .{ .duration = try builder.DurationBuilder.from_slice(.nanosecond, &.{ 69, 69, 11, 15 }, false, allocator) },
        35 => .{ .binary_view = try builder.BinaryViewBuilder.from_slice_opt(&.{ "hello", "world", null }, allocator) },
        36 => .{ .utf8_view = try builder.Utf8ViewBuilder.from_slice_opt(&.{ "hello", "world", null }, allocator) },
        else => unreachable,
    };
}

pub const NUM_ARRAYS = 37;

fn make_map(allocator: Allocator) !arr.Array {
    const keys = try builder.Utf8Builder.from_slice(&.{ "joe", "blogs", "foo" }, false, allocator);
    const values = try builder.UInt32Builder.from_slice(&.{ 1, 2, 4 }, true, allocator);

    const field_names = try allocator.alloc([:0]const u8, 2);
    field_names[0] = "keys";
    field_names[1] = "values";

    const field_values = try allocator.alloc(arr.Array, 2);
    field_values[0] = .{ .utf8 = keys };
    field_values[1] = .{ .u32 = values };

    const entries = try allocator.create(arr.StructArray);
    entries.* = try builder.StructBuilder.from_slice(field_names, field_values, 3, false, allocator);

    const array = try builder.MapBuilder.from_slice_opt(false, &.{ 1, 2, 0, null }, entries, allocator);

    return .{ .map = array };
}

fn make_fixed_size_list(allocator: Allocator) !arr.Array {
    const inner = try allocator.create(arr.Array);
    inner.* = .{ .u16 = try builder.UInt16Builder.from_slice_opt(&.{ 1, null, 2, 69, null, null }, allocator) };
    const array = try builder.FixedSizeListBuilder.from_slice_opt(3, &.{ true, false }, inner, allocator);
    return .{ .fixed_size_list = array };
}

fn make_sparse_union(allocator: Allocator) !arr.Array {
    const num_children = 2;

    const field_names = try allocator.alloc([:0]const u8, num_children);
    field_names[0] = "ft";
    field_names[1] = "mint";

    const type_id_set = try allocator.alloc(i8, num_children);
    type_id_set[0] = 0;
    type_id_set[1] = 1;

    const type_ids: []const i8 = &.{
        0, 1, 0,
    };

    const children = try allocator.alloc(arr.Array, num_children);
    children[0] = .{ .f32 = try builder.Float32Builder.from_slice_opt(&.{ 69.69, null, null }, allocator) };
    children[1] = .{ .u32 = try builder.UInt32Builder.from_slice_opt(&.{ null, 699, null }, allocator) };

    const array = try builder.SparseUnionBuilder.from_slice(field_names, type_id_set, type_ids, children, allocator);
    return .{ .sparse_union = array };
}

fn make_dense_union(allocator: Allocator) !arr.Array {
    const num_children = 2;

    const field_names = try allocator.alloc([:0]const u8, num_children);
    field_names[0] = "ft";
    field_names[1] = "mint";

    const type_id_set = try allocator.alloc(i8, num_children);
    type_id_set[0] = 0;
    type_id_set[1] = 1;

    const type_ids: []const builder.TypeIdOffset = &.{
        .{ .type_id = 0, .offset = 0 },
        .{ .type_id = 1, .offset = 0 },
        .{ .type_id = 0, .offset = 1 },
    };

    const children = try allocator.alloc(arr.Array, num_children);
    children[0] = .{ .f32 = try builder.Float32Builder.from_slice_opt(&.{ 69.69, null }, allocator) };
    children[1] = .{ .u32 = try builder.UInt32Builder.from_slice(&.{699}, false, allocator) };

    const array = try builder.DenseUnionBuilder.from_slice(field_names, type_id_set, type_ids, children, allocator);
    return .{ .dense_union = array };
}

fn make_struct(allocator: Allocator) !arr.Array {
    const len = 3;

    const field_names = try allocator.alloc([:0]const u8, len);
    field_names[0] = "a";
    field_names[1] = "b";
    field_names[2] = "c";

    const field_values = try allocator.alloc(arr.Array, len);
    field_values[0] = slice.slice(&try make_u32(allocator), 0, 2);
    field_values[1] = slice.slice(&try make_u64(allocator), 0, 2);
    field_values[2] = slice.slice(&try make_list(.i32, allocator), 0, 2);

    const array = try builder.StructBuilder.from_slice_opt(
        field_names,
        field_values,
        &.{ true, false },
        allocator,
    );

    return .{ .struct_ = array };
}

fn make_u32(allocator: Allocator) !arr.Array {
    const array = try builder.UInt32Builder.from_slice_opt(&.{ 1, 1, null, null, 69, 69 }, allocator);
    return .{ .u32 = array };
}

fn make_u64(allocator: Allocator) !arr.Array {
    const array = try builder.UInt64Builder.from_slice_opt(&.{ 1, 1, null, null, 69, 69 }, allocator);
    return .{ .u64 = array };
}

fn make_list(comptime index_t: arr.IndexType, allocator: Allocator) !arr.Array {
    var inner_b = try builder.UInt16Builder.with_capacity(7, true, allocator);
    var list_b = try builder.GenericListBuilder(index_t).with_capacity(4, true, allocator);

    try list_b.append_null();

    try inner_b.append_value(5);
    try inner_b.append_null();
    try inner_b.append_value(69);
    try list_b.append_item(3);

    try list_b.append_null();

    try inner_b.append_value(5);
    try inner_b.append_null();
    try inner_b.append_value(69);
    try inner_b.append_value(11);
    try list_b.append_item(4);

    const inner = try allocator.create(arr.Array);
    inner.* = .{ .u16 = try inner_b.finish() };

    const list = try list_b.finish(inner);

    const sliced_list = slice.slice_list(index_t, &list, 1, 2);

    return switch (index_t) {
        .i32 => .{ .list = sliced_list },
        .i64 => .{ .large_list = sliced_list },
    };
}
