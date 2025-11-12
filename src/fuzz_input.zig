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
const validate = @import("./validate.zig");

pub fn array(
    input: *FuzzInput,
    dt: *const dt_mod.DataType,
    len: u32,
    alloc: Allocator,
) Error!arr.Array {
    const a: arr.Array = switch (dt.*) {
        .null => .{ .null = null_array(len) },
        .i8 => .{ .i8 = try primitive_array(i8, input, len, alloc) },
        .i16 => .{ .i16 = try primitive_array(i16, input, len, alloc) },
        .i32 => .{ .i32 = try primitive_array(i32, input, len, alloc) },
        .i64 => .{ .i64 = try primitive_array(i64, input, len, alloc) },
        .u8 => .{ .u8 = try primitive_array(u8, input, len, alloc) },
        .u16 => .{ .u16 = try primitive_array(u16, input, len, alloc) },
        .u32 => .{ .u32 = try primitive_array(u32, input, len, alloc) },
        .u64 => .{ .u64 = try primitive_array(u64, input, len, alloc) },
        .f16 => .{ .f16 = try primitive_array(f16, input, len, alloc) },
        .f32 => .{ .f32 = try primitive_array(f32, input, len, alloc) },
        .f64 => .{ .f64 = try primitive_array(f64, input, len, alloc) },
        .binary => .{ .binary = try binary_array(.i32, input, len, alloc) },
        .large_binary => .{ .large_binary = try binary_array(.i64, input, len, alloc) },
        .utf8 => .{ .utf8 = try utf8_array(.i32, input, len, alloc) },
        .large_utf8 => .{ .large_utf8 = try utf8_array(.i64, input, len, alloc) },
        .bool => .{ .bool = try bool_array(input, len, alloc) },
        .binary_view => .{ .binary_view = try binary_view_array(input, len, alloc) },
        .utf8_view => .{ .utf8_view = try utf8_view_array(input, len, alloc) },
        .decimal32 => |a| .{ .decimal32 = try decimal_array(.i32, input, a, len, alloc) },
        .decimal64 => |a| .{ .decimal64 = try decimal_array(.i64, input, a, len, alloc) },
        .decimal128 => |a| .{ .decimal128 = try decimal_array(.i128, input, a, len, alloc) },
        .decimal256 => |a| .{ .decimal256 = try decimal_array(.i256, input, a, len, alloc) },
        .fixed_size_binary => |a| .{ .fixed_size_binary = try fixed_size_binary_array(input, a, len, alloc) },
        .date32 => .{ .date32 = try date_array(.i32, input, len, alloc) },
        .date64 => .{ .date64 = try date_array(.i64, input, len, alloc) },
        .time32 => |a| .{ .time32 = try time_array(.i32, input, a, len, alloc) },
        .time64 => |a| .{ .time64 = try time_array(.i64, input, a, len, alloc) },
        .timestamp => |a| .{ .timestamp = try timestamp_array(input, a, len, alloc) },
        .duration => |a| .{ .duration = try duration_array(input, a, len, alloc) },
        .interval_year_month => .{ .interval_year_month = try interval_array(.year_month, input, len, alloc) },
        .interval_day_time => .{
            .interval_day_time = try interval_array(.day_time, input, len, alloc),
        },
        .interval_month_day_nano => .{
            .interval_month_day_nano = try interval_array(.month_day_nano, input, len, alloc),
        },
        .list => |a| .{ .list = try list_array(.i32, input, a, len, alloc) },
        .large_list => |a| .{ .large_list = try list_array(.i64, input, a, len, alloc) },
        .list_view => |a| .{ .list_view = try list_view_array(.i32, input, a, len, alloc) },
        .large_list_view => |a| .{ .large_list_view = try list_view_array(.i64, input, a, len, alloc) },
        .fixed_size_list => |a| .{ .fixed_size_list = try fixed_size_list_array(input, a, len, alloc) },
        .struct_ => |a| .{ .struct_ = try struct_array(input, a, len, alloc) },
        .map => |a| .{ .map = try map_array(input, a, len, alloc) },
        .dense_union => |a| .{ .dense_union = try dense_union_array(input, a.*, len, alloc) },
        .sparse_union => |a| .{ .sparse_union = try sparse_union_array(input, a.*, len, alloc) },
        .run_end_encoded => |a| .{ .run_end_encoded = try run_end_encoded_array(input, a, len, alloc) },
        .dict => |a| .{ .dict = try dict_array(input, a, len, alloc) },
    };

    validate.validate_array(&a) catch unreachable;

    return a;
}

pub fn dict_array(
    input: *FuzzInput,
    dt: *const dt_mod.DictType,
    len: u32,
    alloc: Allocator,
) Error!arr.DictArray {
    const offset: u32 = try input.int(u8);
    const total_len: u32 = len + offset;

    const num_values = @as(u32, try input.int(u8)) + 1;

    const keys_offset = try input.int(u8);
    const keys_total_len = keys_offset + total_len;

    std.debug.assert(dt.key == .u32);

    const keys_data = try input.int_slice(u32, keys_total_len, alloc);
    for (0..keys_total_len) |idx| {
        keys_data.ptr[idx] %= num_values;
    }

    const keys = try fuzzin.create(arr.Array, alloc);
    keys.* = .{ .i32 = .{
        .values = @ptrCast(keys_data),
        .len = total_len,
        .offset = keys_offset,
        .validity = null,
        .null_count = 0,
    } };

    const values = try fuzzin.create(arr.Array, alloc);
    values.* = try array(input, &dt.value, num_values, alloc);

    return arr.DictArray{
        .len = len,
        .offset = offset,
        .keys = keys,
        .values = values,
        .is_ordered = false,
    };
}

pub fn run_end_encoded_array(
    input: *FuzzInput,
    dt: *const dt_mod.RunEndEncodedType,
    len: u32,
    alloc: Allocator,
) Error!arr.RunEndArray {
    const offset: u32 = try input.int(u8);
    const total_len: u32 = len + offset;

    const run_ends_len = @as(u32, try input.int(u8)) + 1;
    const run_ends_offset: u32 = try input.int(u8);
    const run_ends_total_len: u32 = run_ends_len + run_ends_offset;

    std.debug.assert(dt.run_end == .i32);

    const run_ends_values = try input.int_slice(i32, run_ends_total_len, alloc);
    var run_end: i32 = 0;
    const tl: i32 = @intCast(total_len);
    for (run_ends_values) |*x| {
        run_end += @as(i32, @bitCast(@as(u32, @bitCast(x.*)) % 512));
        run_end = @min(tl, run_end);
        x.* = run_end;
    }
    const last_re = &run_ends_values[run_ends_values.len - 1];
    last_re.* = @max(last_re.*, @as(i32, @intCast(total_len)));

    const values = try fuzzin.create(arr.Array, alloc);
    values.* = try array(input, &dt.value, run_ends_len, alloc);
    const run_ends = try fuzzin.create(arr.Array, alloc);
    run_ends.* = .{
        .i32 = .{
            .values = run_ends_values,
            .len = run_ends_len,
            .offset = run_ends_offset,
            .validity = null,
            .null_count = 0,
        },
    };

    return .{
        .len = len,
        .offset = offset,
        .run_ends = run_ends,
        .values = values,
    };
}

pub fn map_array(
    input: *FuzzInput,
    dt: *const dt_mod.MapType,
    len: u32,
    alloc: Allocator,
) Error!arr.MapArray {
    const offset: u32 = try input.int(u8);
    const total_len: u32 = len + offset;

    const sizes = try input.bytes(total_len);

    var entries_len: u32 = 0;
    for (sizes) |sz| {
        entries_len += sz;
    }

    const entries_offset: u32 = try input.int(u8);
    const entries_total_len: u32 = entries_len + entries_offset;

    const field_names = try fuzzin.allocate([:0]const u8, 2, alloc);
    field_names[0] = "keys";
    field_names[1] = "values";

    const field_values = try fuzzin.allocate(arr.Array, 2, alloc);

    var keys = try binary_array(.i32, input, entries_total_len, alloc);
    keys.null_count = 0;
    keys.validity = null;
    field_values[0] = .{ .binary = keys };
    field_values[1] = try array(input, &dt.value, entries_total_len, alloc);

    const entries = try fuzzin.create(arr.StructArray, alloc);
    entries.* = arr.StructArray{
        .field_names = field_names,
        .field_values = field_values,
        .len = entries_len,
        .offset = entries_offset,
        .validity = null,
        .null_count = 0,
    };

    const offsets = try fuzzin.allocate(i32, total_len + 1, alloc);
    {
        var start_offset: i32 = 0;
        for (0..total_len) |idx| {
            offsets.ptr[idx] = start_offset;
            start_offset +%= sizes.ptr[idx];
        }
        offsets.ptr[total_len] = start_offset;
    }

    var a = arr.MapArray{
        .len = len,
        .offset = offset,
        .offsets = offsets,
        .entries = entries,
        .keys_are_sorted = false,
        .validity = null,
        .null_count = 0,
    };

    if (try validity(input, offset, len, alloc)) |v| {
        a.validity = v.validity;
        a.null_count = v.null_count;
    }

    return a;
}

pub fn fixed_size_list_array(
    input: *FuzzInput,
    dt: *const dt_mod.FixedSizeListType,
    len: u32,
    alloc: Allocator,
) Error!arr.FixedSizeListArray {
    const offset: u32 = try input.int(u8);
    const total_len: u32 = len + offset;

    const item_width: u32 = @intCast(dt.item_width);

    const inner = try fuzzin.create(arr.Array, alloc);
    inner.* = try array(input, &dt.inner, item_width * total_len, alloc);

    var a = arr.FixedSizeListArray{
        .len = len,
        .offset = offset,
        .validity = null,
        .null_count = 0,
        .inner = inner,
        .item_width = dt.item_width,
    };

    if (try validity(input, offset, len, alloc)) |v| {
        a.validity = v.validity;
        a.null_count = v.null_count;
    }

    return a;
}

pub fn sparse_union_array(
    input: *FuzzInput,
    dt: dt_mod.UnionType,
    len: u32,
    alloc: Allocator,
) Error!arr.SparseUnionArray {
    const offset: u32 = try input.int(u8);
    const total_len: u32 = len + offset;

    const num_children = dt.field_names.len;

    const children = try fuzzin.allocate(arr.Array, num_children, alloc);

    for (0..num_children) |child_idx| {
        children[child_idx] = try array(
            input,
            &dt.field_types[child_idx],
            total_len,
            alloc,
        );
    }

    const type_ids = try fuzzin.allocate(i8, total_len, alloc);

    for (try input.bytes(total_len), 0..) |b, idx| {
        const child_idx = b % num_children;
        type_ids[idx] = dt.type_id_set[child_idx];
    }

    return .{
        .inner = arr.UnionArray{
            .offset = offset,
            .len = len,
            .field_names = dt.field_names,
            .children = children,
            .type_ids = type_ids,
            .type_id_set = dt.type_id_set,
        },
    };
}

pub fn dense_union_array(
    input: *FuzzInput,
    dt: dt_mod.UnionType,
    len: u32,
    alloc: Allocator,
) Error!arr.DenseUnionArray {
    const offset: u32 = try input.int(u8);
    const total_len: u32 = len + offset;

    const num_children = dt.field_names.len;

    const children = try fuzzin.allocate(arr.Array, num_children, alloc);

    const offsets = try fuzzin.allocate(i32, total_len, alloc);
    const type_ids = try fuzzin.allocate(i8, total_len, alloc);

    const current_offsets = try fuzzin.allocate(i32, num_children, alloc);
    @memset(current_offsets, 0);

    for (try input.bytes(total_len), 0..) |b, idx| {
        const child_idx = b % num_children;
        type_ids[idx] = dt.type_id_set[child_idx];
        const current_offset = current_offsets[child_idx];
        current_offsets[child_idx] = current_offset + 1;
        offsets[idx] = current_offset;
    }

    for (0..num_children) |child_idx| {
        children[child_idx] = try array(
            input,
            &dt.field_types[child_idx],
            @as(u32, @intCast(current_offsets[child_idx])),
            alloc,
        );
    }

    return .{
        .offsets = offsets,
        .inner = arr.UnionArray{
            .offset = offset,
            .len = len,
            .field_names = dt.field_names,
            .children = children,
            .type_ids = type_ids,
            .type_id_set = dt.type_id_set,
        },
    };
}

pub fn struct_array(
    input: *FuzzInput,
    dt: *const dt_mod.StructType,
    len: u32,
    alloc: Allocator,
) Error!arr.StructArray {
    const offset: u32 = try input.int(u8);
    const total_len: u32 = len + offset;

    const num_fields = dt.field_names.len;

    const field_values = try fuzzin.allocate(arr.Array, num_fields, alloc);

    for (0..num_fields) |field_idx| {
        const field_dt = &dt.field_types[field_idx];
        field_values[field_idx] = try array(input, field_dt, total_len, alloc);
    }

    var a = arr.StructArray{
        .len = len,
        .offset = offset,
        .field_names = dt.field_names,
        .field_values = field_values,
        .validity = null,
        .null_count = 0,
    };

    if (try validity(input, offset, len, alloc)) |v| {
        a.validity = v.validity;
        a.null_count = v.null_count;
    }

    return a;
}

pub fn list_view_array(
    comptime index_t: arr.IndexType,
    input: *FuzzInput,
    inner_dt: *const dt_mod.DataType,
    len: u32,
    alloc: Allocator,
) Error!arr.GenericListViewArray(index_t) {
    const I = index_t.to_type();
    const U = switch (index_t) {
        .i32 => u32,
        .i64 => u64,
    };

    const offset: u32 = try input.int(u8);
    const total_len: u32 = len + offset;

    const sizes_b = try input.bytes(total_len);

    const sizes = try fuzzin.allocate(I, total_len, alloc);
    for (0..total_len) |idx| {
        sizes.ptr[idx] = sizes_b.ptr[idx] % 10;
    }

    var total_size: I = 0;
    for (0..total_len) |idx| {
        total_size += sizes.ptr[idx];
    }

    const offsets = try input.int_slice(I, total_len, alloc);
    if (total_len == 1) {
        offsets[0] = 0;
    } else {
        for (0..total_len) |idx| {
            offsets.ptr[idx] = @bitCast(
                @as(U, @bitCast(offsets.ptr[idx])) % @as(
                    U,
                    @bitCast(total_size -% sizes.ptr[idx] +% 1),
                ),
            );
        }
    }

    const inner_len = total_size;

    const inner = try fuzzin.create(arr.Array, alloc);
    inner.* = try array(input, inner_dt, @intCast(inner_len), alloc);

    var a = arr.GenericListViewArray(index_t){
        .len = len,
        .offset = offset,
        .inner = inner,
        .offsets = offsets,
        .sizes = sizes,
        .validity = null,
        .null_count = 0,
    };

    if (try validity(input, offset, len, alloc)) |v| {
        a.validity = v.validity;
        a.null_count = v.null_count;
    }

    return a;
}

pub fn list_array(
    comptime index_t: arr.IndexType,
    input: *FuzzInput,
    inner_dt: *const dt_mod.DataType,
    len: u32,
    alloc: Allocator,
) Error!arr.GenericListArray(index_t) {
    const I = index_t.to_type();

    const offset: u32 = try input.int(u8);
    const total_len: u32 = len + offset;

    const sizes_b = try input.bytes(total_len);

    const sizes = try fuzzin.allocate(u8, sizes_b.len, alloc);
    for (0..total_len) |idx| {
        sizes.ptr[idx] = sizes_b.ptr[idx] % 10;
    }

    const offsets = try fuzzin.allocate(I, total_len + 1, alloc);
    {
        var start_offset: I = 0;
        for (0..total_len) |idx| {
            offsets.ptr[idx] = start_offset;
            start_offset +%= sizes.ptr[idx];
        }
        offsets.ptr[total_len] = start_offset;
    }

    const inner_len = offsets[total_len];

    const inner = try fuzzin.create(arr.Array, alloc);
    inner.* = try array(input, inner_dt, @intCast(inner_len), alloc);

    var a = arr.GenericListArray(index_t){
        .len = len,
        .offset = offset,
        .inner = inner,
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

pub fn utf8_view_array(input: *FuzzInput, len: u32, alloc: Allocator) Error!arr.Utf8ViewArray {
    return .{ .inner = try binary_view_array(input, len, alloc) };
}

pub fn binary_view_array(input: *FuzzInput, len: u32, alloc: Allocator) Error!arr.BinaryViewArray {
    const offset: u32 = try input.int(u8);
    const total_len: u32 = len + offset;

    const max_str_len = 40;
    const num_buffers = (try input.int(u8)) % 5 + 1;
    const buffer_len = @max(max_str_len + 5, (try input.int(u16)) % (1 << 8) + 1);

    var prng = Prng.init(try input.int(u64));
    const rand = prng.random();

    const buffers = try fuzzin.allocate([]const u8, num_buffers, alloc);
    for (0..num_buffers) |buffer_idx| {
        const buffer = try fuzzin.allocate(u8, buffer_len, alloc);
        rand.bytes(buffer);
        buffers[buffer_idx] = buffer;
    }

    const views = try input.auto_slice(arr.BinaryView, total_len, alloc, 64, 0);
    for (0..views.len) |view_idx| {
        var view = views.ptr[view_idx];
        const view_len = @as(u32, @bitCast(view.length)) % (max_str_len + 1);
        view.length = @bitCast(view_len);

        if (view_len > 12) {
            const buffer_idx = @as(u32, @bitCast(view.buffer_idx)) % num_buffers;
            view.buffer_idx = @bitCast(buffer_idx);
            const max_offset = buffer_len - view_len;
            const voffset = @as(u32, @bitCast(view.offset)) % max_offset;
            view.offset = @intCast(voffset);
            view.prefix = std.mem.readVarInt(
                i32,
                buffers[buffer_idx][voffset .. voffset + 4],
                .little,
            );
        }

        views.ptr[view_idx] = view;
    }

    var a = arr.BinaryViewArray{
        .len = len,
        .offset = offset,
        .buffers = buffers,
        .views = views,
        .validity = null,
        .null_count = 0,
    };

    if (try validity(input, offset, len, alloc)) |v| {
        a.validity = v.validity;
        a.null_count = v.null_count;
    }

    return a;
}

pub fn duration_array(
    input: *FuzzInput,
    unit: arr.TimestampUnit,
    len: u32,
    alloc: Allocator,
) Error!arr.DurationArray {
    return .{
        .unit = unit,
        .inner = try primitive_array(
            i64,
            input,
            len,
            alloc,
        ),
    };
}

pub fn fixed_size_binary_array(
    input: *FuzzInput,
    byte_width: i32,
    len: u32,
    alloc: Allocator,
) Error!arr.FixedSizeBinaryArray {
    const offset: u32 = try input.int(u8);
    const total_len: u32 = len + offset;

    var prng = Prng.init(try input.int(u64));
    const rand = prng.random();

    const bw: u32 = @intCast(byte_width);

    const data = try fuzzin.allocate(u8, bw * total_len, alloc);
    rand.bytes(data);

    var a = arr.FixedSizeBinaryArray{
        .len = len,
        .offset = offset,
        .validity = null,
        .null_count = 0,
        .data = data,
        .byte_width = byte_width,
    };

    if (try validity(input, offset, len, alloc)) |v| {
        a.validity = v.validity;
        a.null_count = v.null_count;
    }

    return a;
}

pub fn interval_array(
    comptime interval_t: arr.IntervalType,
    input: *FuzzInput,
    len: u32,
    alloc: Allocator,
) Error!arr.IntervalArray(interval_t) {
    return .{
        .inner = try primitive_array(
            interval_t.to_type(),
            input,
            len,
            alloc,
        ),
    };
}

pub fn timestamp_array(
    input: *FuzzInput,
    ts: arr.Timestamp,
    len: u32,
    alloc: Allocator,
) Error!arr.TimestampArray {
    return .{
        .ts = ts,
        .inner = try primitive_array(
            i64,
            input,
            len,
            alloc,
        ),
    };
}

pub fn time_array(
    comptime backing_t: arr.IndexType,
    input: *FuzzInput,
    unit: arr.TimeArray(backing_t).Unit,
    len: u32,
    alloc: Allocator,
) Error!arr.TimeArray(backing_t) {
    return .{
        .unit = unit,
        .inner = try primitive_array(
            backing_t.to_type(),
            input,
            len,
            alloc,
        ),
    };
}

pub fn date_array(
    comptime backing_t: arr.IndexType,
    input: *FuzzInput,
    len: u32,
    alloc: Allocator,
) Error!arr.DateArray(backing_t) {
    return .{
        .inner = try primitive_array(
            backing_t.to_type(),
            input,
            len,
            alloc,
        ),
    };
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
    return .{ .inner = try binary_array(index_t, input, len, alloc) };
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
        return try data_type_flat(input);
    }

    const kind = (try input.int(u8)) % 44;

    const dt: dt_mod.DataType = switch (kind) {
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
                dt_mod.DataType,
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

    validate.validate_data_type(&dt) catch unreachable;

    return dt;
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
    existing_names: []const [:0]const u8,
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
    const ptr = try fuzzin.create(T, alloc);
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

    const field_types = try fuzzin.allocate(dt_mod.DataType, num_fields, alloc);

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

    var prng = Prng.init(try input.int(u64));
    const rand = prng.random();

    for (0..num_children) |child_idx| {
        const field_name = try uniq_name(field_names[0..child_idx], rand, alloc);
        field_names[child_idx] = field_name;
    }

    const field_types = try fuzzin.allocate(dt_mod.DataType, num_children, alloc);

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

pub fn slice(input: *FuzzInput, ar: *const arr.Array) Error!arr.Array {
    const array_len = length.length(ar);
    if (array_len == 0) {
        return ar.*;
    }
    const slice0_len = (try input.int(u8)) % array_len;
    const slice0_offset = (try input.int(u8)) % (array_len - slice0_len);
    const slice0 = slice_array_impl(ar, slice0_offset, slice0_len);
    return slice0;
}
