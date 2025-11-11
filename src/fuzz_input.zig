const std = @import("std");
const Allocator = std.mem.Allocator;
const Prng = std.Random.DefaultPrng;

const fuzzin = @import("fuzzin");
const FuzzInput = fuzzin.FuzzInput;
const Error = fuzzin.Error;

const arr = @import("./array.zig");
const bitmap = @import("./bitmap.zig");
const length = @import("./length.zig");
const slice_array_impl = @import("./slice.zig").slice;
const dt_mod = @import("./data_type.zig");

pub fn array(
    input: *FuzzInput,
    dt: *const dt_mod.DataType,
    len: u32,
    alloc: Allocator,
) Error!arr.Array {
    switch (dt.*) {
        .null => {
            return .{ .null = .{
                .len = len,
            } };
        },
        .i8 => {
            return .{ .i8 = {} };
        },
        .i16 => {
            return .{ .i16 = {} };
        },
        .i32 => {
            return .{ .i32 = {} };
        },
        .i64 => {
            return .{ .i64 = {} };
        },
        .u8 => {
            return .{ .u8 = {} };
        },
        .u16 => {
            return .{ .u16 = {} };
        },
        .u32 => {
            return .{ .u32 = {} };
        },
        .u64 => {
            return .{ .u64 = {} };
        },
        .f16 => {
            return .{ .f16 = {} };
        },
        .f32 => {
            return .{ .f32 = {} };
        },
        .f64 => {
            return .{ .f64 = {} };
        },
        .binary => {
            return .{ .binary = {} };
        },
        .large_binary => {
            return .{ .large_binary = {} };
        },
        .utf8 => {
            return .{ .utf8 = {} };
        },
        .large_utf8 => {
            return .{ .large_utf8 = {} };
        },
        .bool => {
            return .{ .bool = {} };
        },
        .binary_view => {
            return .{ .binary_view = {} };
        },
        .utf8_view => {
            return .{ .utf8_view = {} };
        },
        .decimal32 => |*a| {
            return .{ .decimal32 = a.params };
        },
        .decimal64 => |*a| {
            return .{ .decimal64 = a.params };
        },
        .decimal128 => |*a| {
            return .{ .decimal128 = a.params };
        },
        .decimal256 => |*a| {
            return .{ .decimal256 = a.params };
        },
        .fixed_size_binary => |*a| {
            return .{ .fixed_size_binary = a.byte_width };
        },
        .date32 => {
            return .{ .date32 = {} };
        },
        .date64 => {
            return .{ .date64 = {} };
        },
        .time32 => |*a| {
            return .{ .time32 = a.unit };
        },
        .time64 => |*a| {
            return .{ .time64 = a.unit };
        },
        .timestamp => |*a| {
            return .{ .timestamp = a.ts };
        },
        .duration => |*a| {
            return .{ .duration = a.unit };
        },
        .interval_year_month => {
            return .{ .interval_year_month = {} };
        },
        .interval_day_time => {
            return .{ .interval_day_time = {} };
        },
        .interval_month_day_nano => {
            return .{ .interval_month_day_nano = {} };
        },
        .list => |*a| {
            const inner = try alloc.create(DataType);
            inner.* = try get_data_type(a.inner, alloc);
            return .{ .list = inner };
        },
        .large_list => |*a| {
            const inner = try alloc.create(DataType);
            inner.* = try get_data_type(a.inner, alloc);
            return .{ .large_list = inner };
        },
        .list_view => |*a| {
            const inner = try alloc.create(DataType);
            inner.* = try get_data_type(a.inner, alloc);
            return .{ .list_view = inner };
        },
        .large_list_view => |*a| {
            const inner = try alloc.create(DataType);
            inner.* = try get_data_type(a.inner, alloc);
            return .{ .large_list_view = inner };
        },
        .fixed_size_list => |*a| {
            const fsl_type = try alloc.create(FixedSizeListType);
            fsl_type.* = .{
                .inner = try get_data_type(a.inner, alloc),
                .item_width = a.item_width,
            };
            return .{ .fixed_size_list = fsl_type };
        },
        .struct_ => |*a| {
            const field_types = try alloc.alloc(DataType, a.field_values.len);
            for (a.field_values, 0..) |*field, idx| {
                field_types[idx] = try get_data_type(field, alloc);
            }

            const struct_type = try alloc.create(StructType);
            struct_type.* = StructType{ .field_names = a.field_names, .field_types = field_types };
            return .{ .struct_ = struct_type };
        },
        .map => |*a| {
            const key: MapKeyType = switch (a.entries.field_values[0]) {
                .binary => .binary,
                .large_binary => .large_binary,
                .utf8 => .utf8,
                .large_utf8 => .large_utf8,
                .binary_view => .binary_view,
                .utf8_view => .utf8_view,
                .fixed_size_binary => |*k| .{ .fixed_size_binary = k.byte_width },
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

            const value = try get_data_type(&a.entries.field_values[1], alloc);

            const map_type = try alloc.create(MapType);
            map_type.* = .{
                .key = key,
                .value = value,
            };

            return .{ .map = map_type };
        },
        .dense_union => |*a| {
            return .{ .dense_union = try get_union_type(&a.inner, alloc) };
        },
        .sparse_union => |*a| {
            return .{ .sparse_union = try get_union_type(&a.inner, alloc) };
        },
        .run_end_encoded => |*a| {
            const run_end: RunEndType = switch (a.run_ends.*) {
                .i16 => .i16,
                .i32 => .i32,
                .i64 => .i64,
                else => unreachable,
            };

            const value = try get_data_type(a.values, alloc);

            const ree_type = try alloc.create(RunEndEncodedType);
            ree_type.* = .{
                .run_end = run_end,
                .value = value,
            };

            return .{ .run_end_encoded = ree_type };
        },
        .dict => |*a| {
            const key: DictKeyType = switch (a.keys.*) {
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

            const value = try get_data_type(a.values, alloc);

            const dict_type = try alloc.create(DictType);
            dict_type.* = .{
                .key = key,
                .value = value,
            };

            return .{ .dict = dict_type };
        },
    }
}

pub fn decimal_array(
    comptime decimal_t: arr.DecimalInt,
    input: *FuzzInput,
    params: arr.DecimalParams,
    len: u32,
    alloc: Allocator,
) Error!arr.DecimalArray(decimal_t) {
    const inner = try primitive_array(decimal_t.to_type(), input, len, alloc);
    return .{ .params = params, .inner = inner };
}

pub fn bool_array(input: *FuzzInput, len: u32, alloc: Allocator) Error!arr.BoolArray {
    const offset: u32 = try input.int(u8);
    const total_len: u32 = len + offset;

    var prng = Prng.init(try input.int(u64));
    const rand = prng.random();
    const bitmap_len = (total_len + 7) / 8;
    const values = try fuzzin.allocate(u8, bitmap_len, alloc);
    @memset(values, 0);
    {
        var idx: u32 = 0;
        while (idx < total_len) : (idx += 1) {
            if (rand.boolean()) {
                bitmap.set(values.ptr, idx);
            }
        }
    }

    var a = arr.BoolArray{
        .len = len,
        .offset = offset,
        .values = values,
        .validity = null,
        .null_count = 0,
    };

    if (try validity(input, offset, len, alloc)) |v| {
        a.validity = v.validity;
        a.null_count = v.null_count;
    }

    return a;
}

pub fn utf8_array(
    comptime index_t: arr.IndexType,
    input: *FuzzInput,
    len: u32,
    alloc: Allocator,
) Error!arr.GenericUtf8Array(index_t) {
    return .{ .inner = try binary_array(input, index_t, len, alloc) };
}

pub fn binary_array(
    comptime index_t: arr.IndexType,
    input: *FuzzInput,
    len: u32,
    alloc: Allocator,
) Error!arr.GenericBinaryArray(index_t) {
    const I = index_t.to_type();

    const offset: u32 = try input.int(u8);
    const total_len: u32 = len + offset;

    const sizes = try input.bytes(total_len);

    const offsets = try fuzzin.allocate(I, total_len + 1, alloc);
    {
        var start_offset: I = 0;
        for (0..total_len) |idx| {
            offsets.ptr[idx] = start_offset;
            start_offset +%= sizes.ptr[idx];
        }
        offsets.ptr[total_len] = start_offset;
    }

    var prng = Prng.init(try input.int(u64));
    const rand = prng.random();

    const data_len = offsets[total_len];

    const data = try fuzzin.allocate(u8, @intCast(data_len), alloc);
    rand.bytes(data);

    var a = arr.GenericBinaryArray(index_t){
        .len = len,
        .offset = offset,
        .data = data,
        .offsets = offsets,
        .validity = null,
        .null_count = 0,
    };

    if (try validity(input, offset, len, alloc)) |v| {
        a.validity = v.validity;
        a.null_count = v.null_count;
    }

    return a;
}

pub fn primitive_array(
    comptime T: type,
    input: *FuzzInput,
    len: u32,
    alloc: Allocator,
) Error!arr.PrimitiveArray(T) {
    const offset: u32 = try input.int(u8);
    const total_len = len + offset;

    var prng = Prng.init(try input.int(u64));
    const rand = prng.random();
    const values = try fuzzin.allocate(T, total_len, alloc);

    switch (T) {
        f16, f32, f64 => {
            for (0..total_len) |idx| {
                values.ptr[idx] = @floatCast(rand.float(f64));
            }
        },
        else => {
            const values_raw: []u8 = @ptrCast(values);
            rand.bytes(values_raw);
        },
    }

    var a = arr.PrimitiveArray(T){
        .len = len,
        .offset = offset,
        .values = values,
        .validity = null,
        .null_count = 0,
    };

    if (try validity(input, offset, len, alloc)) |v| {
        a.validity = v.validity;
        a.null_count = v.null_count;
    }

    return a;
}

pub fn null_array(len: u32) arr.NullArray {
    return .{ .len = len };
}

pub fn validity(input: *FuzzInput, offset: u32, len: u32, alloc: Allocator) Error!?struct {
    validity: []const u8,
    null_count: u32,
} {
    const has_validity = try input.boolean();

    const total_len = offset + len;

    if (!has_validity) {
        return null;
    }

    const v_len = (total_len + 7) / 8;
    const v = try fuzzin.allocate(u8, v_len, alloc);
    @memset(v, 0);

    var prng = Prng.init(try input.int(u64));
    const rand = prng.random();

    var idx: u32 = 0;
    while (idx < total_len) : (idx += 1) {
        if (rand.boolean()) {
            bitmap.set(v.ptr, idx);
        }
    }

    return .{ .validity = v, .null_count = bitmap.count_nulls(v, offset, len) };
}

pub fn data_type(
    input: *FuzzInput,
    alloc: Allocator,
    max_depth: u8,
) Error!dt_mod.DataType {
    return try data_type_impl(input, alloc, max_depth, 0);
}

fn data_type_impl(
    input: *FuzzInput,
    alloc: Allocator,
    max_depth: u8,
    depth: u8,
) Error!dt_mod.DataType {
    if (max_depth >= depth + 1) {
        return try data_type_flat(input, alloc);
    }

    const kind = (try input.int(u8)) % 44;

    return switch (kind) {
        0 => .{ .null = {} },
        1 => .{ .i8 = {} },
        2 => .{ .i16 = {} },
        3 => .{ .i32 = {} },
        4 => .{ .i64 = {} },
        5 => .{ .u8 = {} },
        6 => .{ .u16 = {} },
        7 => .{ .u32 = {} },
        8 => .{ .u64 = {} },
        9 => .{ .f16 = {} },
        10 => .{ .f32 = {} },
        11 => .{ .f64 = {} },
        12 => .{ .binary = {} },
        13 => .{ .utf8 = {} },
        14 => .{ .bool = {} },
        15 => .{ .decimal32 = try decimal_params(.i32, input) },
        16 => .{ .decimal64 = try decimal_params(.i64, input) },
        17 => .{ .decimal128 = try decimal_params(.i128, input) },
        18 => .{ .decimal256 = try decimal_params(.i256, input) },
        19 => .{ .date32 = {} },
        20 => .{ .date64 = {} },
        21 => .{ .time32 = try time_unit(.i32, input) },
        22 => .{ .time64 = try time_unit(.i64, input) },
        23 => .{ .timestamp = try timestamp(input, alloc) },
        24 => .{ .interval_year_month = {} },
        25 => .{ .interval_day_time = {} },
        26 => .{ .interval_month_day_nano = {} },
        27 => .{
            .list = try make_ptr(
                data_type.DataType,
                try data_type_impl(input, alloc, max_depth, depth + 1),
                alloc,
            ),
        },
        28 => .{
            .struct_ = try make_ptr(
                dt_mod.StructType,
                try struct_type(input, alloc, max_depth, depth),
                alloc,
            ),
        },
        29 => .{
            .dense_union = try make_ptr(
                dt_mod.UnionType,
                try union_type(input, alloc, max_depth, depth),
                alloc,
            ),
        },
        30 => .{
            .sparse_union = try make_ptr(
                dt_mod.UnionType,
                try union_type(input, alloc, max_depth, depth),
                alloc,
            ),
        },
        31 => .{ .fixed_size_binary = try fixed_size_binary_width(input) },
        32 => .{
            .fixed_size_list = try make_ptr(
                dt_mod.FixedSizeListType,
                try fixed_size_list_type(input, alloc, max_depth, depth),
                alloc,
            ),
        },
        33 => .{
            .map = try make_ptr(
                dt_mod.MapType,
                try map_type(input, alloc, max_depth, depth),
                alloc,
            ),
        },
        34 => .{ .duration = try timestamp_unit(input) },
        35 => .{ .large_binary = {} },
        36 => .{ .large_utf8 = {} },
        37 => .{
            .large_list = try make_ptr(
                dt_mod.DataType,
                try data_type_impl(input, alloc, max_depth, depth + 1),
                alloc,
            ),
        },
        38 => .{
            .run_end_encoded = try make_ptr(
                dt_mod.RunEndEncodedType,
                try run_end_encoded_type(input, alloc, max_depth, depth),
                alloc,
            ),
        },
        39 => .{ .binary_view = {} },
        40 => .{ .utf8_view = {} },
        41 => .{
            .list_view = try make_ptr(
                dt_mod.DataType,
                try data_type_impl(input, alloc, max_depth, depth + 1),
                alloc,
            ),
        },
        42 => .{
            .large_list_view = try make_ptr(
                dt_mod.DataType,
                try data_type_impl(input, alloc, max_depth, depth + 1),
                alloc,
            ),
        },
        43 => .{
            .dict = try make_ptr(
                dt_mod.DictType,
                try dict_type(input, alloc, max_depth, depth),
                alloc,
            ),
        },
        else => unreachable,
    };
}

pub fn data_type_flat(input: *FuzzInput) Error!dt_mod.DataType {
    return switch (try input.int(u8) % 17) {
        0 => .{ .null = {} },
        1 => .{ .i8 = {} },
        2 => .{ .i16 = {} },
        3 => .{ .i32 = {} },
        4 => .{ .i64 = {} },
        5 => .{ .u8 = {} },
        6 => .{ .u16 = {} },
        7 => .{ .u32 = {} },
        8 => .{ .u64 = {} },
        9 => .{ .f16 = {} },
        10 => .{ .f32 = {} },
        11 => .{ .f64 = {} },
        12 => .{ .binary = {} },
        13 => .{ .bool = {} },
        14 => .{ .fixed_size_binary = try fixed_size_binary_width(input) },
        15 => .{ .large_binary = {} },
        16 => .{ .binary_view = {} },
        else => unreachable,
    };
}

fn decimal_params(
    comptime decimal_t: arr.DecimalInt,
    input: *FuzzInput,
) Error!arr.DecimalParams {
    const max_precision = switch (decimal_t) {
        .i32 => 9,
        .i64 => 19,
        .i128 => 38,
        .i256 => 76,
    };

    return .{
        .scale = try input.int(i8),
        .precision = (try input.int(u8)) % max_precision + 1,
    };
}

pub fn time_unit(
    comptime backing_t: arr.IndexType,
    input: *FuzzInput,
) Error!arr.TimeArray(backing_t).Unit {
    const unit_bit = (try input.int(u8)) % 2 == 0;

    return switch (backing_t) {
        .i32 => if (unit_bit) .second else .millisecond,
        .i64 => if (unit_bit) .microsecond else .nanosecond,
    };
}

fn rand_bytes_zero_sentinel(rand: std.Random, out: [:0]u8) void {
    rand.bytes(out);

    for (0..out.len) |i| {
        if (out.ptr[i] == 0) {
            out.ptr[i] = 1;
        }
    }
}

pub fn uniq_name(
    existing_names: []const []const u8,
    rand: std.Random,
    alloc: Allocator,
) Error![:0]const u8 {
    const name_len = rand.int(u8) % 30 + 1;
    const name = try fuzzin.allocate_sentinel(
        u8,
        0,
        name_len,
        alloc,
    );

    namegen: while (true) {
        rand_bytes_zero_sentinel(rand, name);

        for (existing_names) |other_name| {
            if (std.mem.eql(u8, name, other_name)) {
                continue :namegen;
            }
        }
        break;
    }

    return name;
}

fn make_ptr(comptime T: type, v: T, alloc: Allocator) Error!*const T {
    const ptr = fuzzin.create(T, alloc);
    ptr.* = v;
    return ptr;
}

pub fn timestamp_unit(input: *FuzzInput) Error!arr.TimestampUnit {
    const unit_int: u8 = (try input.int(u8)) % 4;
    return switch (unit_int) {
        0 => .second,
        1 => .millisecond,
        2 => .microsecond,
        3 => .nanosecond,
        else => unreachable,
    };
}

pub fn timestamp(input: *FuzzInput, alloc: Allocator) Error!arr.Timestamp {
    const unit = try timestamp_unit(input);
    var ts = arr.Timestamp{
        .unit = unit,
        .timezone = null,
    };

    const timezone_int = try input.int(u8);
    const has_timezone = timezone_int % 2 == 0;

    var prng = Prng.init(try input.int(u64));
    const rand = prng.random();

    if (has_timezone) {
        const tz_len = try input.int(u8) % 40 + 1;
        const tz = try fuzzin.allocate_sentinel(u8, 0, tz_len, alloc);
        rand_bytes_zero_sentinel(rand, tz);
        ts.timezone = tz;
    }

    return ts;
}

pub fn struct_type(
    input: *FuzzInput,
    alloc: Allocator,
    max_depth: u8,
    depth: u8,
) Error!dt_mod.StructType {
    const num_fields = (try input.int(u8)) % 5 + 1;

    const field_names = try fuzzin.allocate([:0]const u8, num_fields, alloc);

    var prng = Prng.init(try input.int(u64));
    const rand = prng.random();

    for (0..num_fields) |field_idx| {
        const field_name = try uniq_name(field_names[0..field_idx], rand, alloc);
        field_names[field_idx] = field_name;
    }

    const field_types = try fuzzin.allocate(data_type.DataType, num_fields, alloc);

    for (0..num_fields) |field_idx| {
        field_types[field_idx] = try data_type_impl(
            input,
            alloc,
            max_depth,
            depth + 1,
        );
    }

    return .{
        .field_names = field_names,
        .field_types = field_types,
    };
}

pub fn union_type(
    input: *FuzzInput,
    alloc: Allocator,
    max_depth: u8,
    depth: u8,
) Error!dt_mod.UnionType {
    const num_children = (try input.int(u8)) % 5 + 1;

    const type_id_set = try fuzzin.allocate(i8, num_children, alloc);
    for (0..num_children) |child_idx| {
        type_id_set[child_idx] = @intCast(child_idx);
    }

    const field_names = try fuzzin.allocate([:0]const u8, num_children, alloc);

    var prng = try Prng.init(try input.int(u64));
    const rand = prng.random();

    for (0..num_children) |child_idx| {
        const field_name = try uniq_name(field_names[0..child_idx], rand, alloc);
        field_names[child_idx] = field_name;
    }

    const field_types = try fuzzin.allocate(data_type.DataType, num_children, alloc);

    for (0..num_children) |field_idx| {
        field_types[field_idx] = try data_type_impl(input, alloc, max_depth, depth + 1);
    }

    return .{
        .field_names = field_names,
        .field_types = field_types,
        .type_id_set = type_id_set,
    };
}

pub fn fixed_size_list_type(
    input: *FuzzInput,
    alloc: Allocator,
    max_depth: u8,
    depth: u8,
) Error!dt_mod.FixedSizeListType {
    const item_width = (try input.int(u8)) % 10 + 1;
    return .{
        .inner = try data_type_impl(input, alloc, max_depth, depth + 1),
        .item_width = item_width,
    };
}

pub fn fixed_size_binary_width(self: *FuzzInput) Error!i32 {
    return (try self.int(u8)) % 69 + 1;
}

pub fn map_type(input: *FuzzInput, alloc: Allocator, max_depth: u8, depth: u8) Error!dt_mod.MapType {
    const value = try data_type_impl(input, alloc, max_depth, depth + 1);

    return .{
        .key = .binary,
        .value = value,
    };
}

pub fn dict_type(input: *FuzzInput, alloc: Allocator, max_depth: u8, depth: u8) Error!dt_mod.DictType {
    const value = try data_type_impl(input, alloc, max_depth, depth + 1);

    return .{
        .key = .i32,
        .value = value,
    };
}

pub fn run_end_encoded_type(
    input: *FuzzInput,
    alloc: Allocator,
    max_depth: u8,
    depth: u8,
) Error!dt_mod.RunEndEncodedType {
    const value = try data_type_impl(input, alloc, max_depth, depth + 1);

    return .{
        .run_end = .i32,
        .value = value,
    };
}
