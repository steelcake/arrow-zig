const std = @import("std");
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const arr = @import("./array.zig");
pub const abi = @cImport(@cInclude("arrow_abi.h"));

pub const FFI_Array = struct {
    schema: abi.ArrowSchema,
    array: abi.ArrowArray,

    fn release(self: *FFI_Array) void {
        const schema_release = self.schema.release orelse unreachable;
        const array_release = self.array.release orelse unreachable;
        schema_release(&self.schema);
        array_release(&self.array);
    }
};

fn validity_size(size: u32) u32 {
    return (size + 7) / 8;
}

fn import_buffer(comptime T: type, buf: ?*const anyopaque, size: u32) []const T {
    const buf_ptr = buf orelse return &.{};
    const ptr: [*]const T = @ptrCast(@alignCast(buf_ptr));
    return ptr[0..size];
}

fn import_validity(flags: i64, buf: ?*const anyopaque, size: u32, allocator: Allocator) !?[]const u8 {
    if (flags & abi.ARROW_FLAG_NULLABLE == 0) {
        return null;
    }
    const byte_size = validity_size(size);
    if (buf) |b| {
        return import_buffer(u8, b, byte_size);
    } else {
        const b = try allocator.alloc(u8, byte_size);
        @memset(b, 0);
        return b;
    }
}

fn import_primitive_impl(comptime T: type, comptime ArrT: type, array: *const FFI_Array, allocator: Allocator) !ArrT {
    const buffers = array.array.buffers.?;
    if (array.array.n_buffers != 2) {
        return error.InvalidFFIArray;
    }

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = try import_validity(array.schema.flags, buffers[0], size, allocator);

    return .{
        .values = import_buffer(T, buffers[1], size),
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
    };
}

fn import_primitive(comptime T: type, comptime ArrT: type, array: *const FFI_Array, allocator: Allocator) !arr.Array {
    const arr_ptr = try allocator.create(ArrT);
    arr_ptr.* = try import_primitive_impl(T, ArrT, array, allocator);
    return arr.Array.from(arr_ptr);
}

fn import_binary(comptime IndexT: type, comptime ArrT: type, array: *const FFI_Array, allocator: Allocator) !arr.Array {
    const buffers = array.array.buffers.?;
    if (array.array.n_buffers != 3) {
        return error.InvalidFFIArray;
    }

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = try import_validity(array.schema.flags, buffers[0], size, allocator);

    const arr_ptr = try allocator.create(ArrT);

    const DataT = comptime switch (ArrT) {
        arr.BinaryArray, arr.LargeBinaryArray => ArrT,
        arr.Utf8Array => arr.BinaryArray,
        arr.LargeUtf8Array => arr.LargeBinaryArray,
        else => @compileError("unexpected array type"),
    };

    const data = DataT{
        .data = import_buffer(u8, buffers[2], size),
        .offsets = import_buffer(IndexT, buffers[1], size + 1),
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
    };

    switch (ArrT) {
        arr.BinaryArray, arr.LargeBinaryArray => {
            arr_ptr.* = data;
        },
        arr.Utf8Array, arr.LargeUtf8Array => {
            arr_ptr.* = .{
                .inner = data,
            };
        },
        else => @compileError("unexpected array type"),
    }

    return arr.Array.from(arr_ptr);
}

fn import_binary_view(comptime ArrT: type, array: *const FFI_Array, allocator: Allocator) !arr.Array {
    const buffers = array.array.buffers.?;
    if (array.array.n_buffers <= 2) {
        return error.InvalidFFIArray;
    }

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = try import_validity(array.schema.flags, buffers[0], size, allocator);

    const arr_ptr = try allocator.create(ArrT);

    const num_data_buffers: u32 = @intCast(array.array.n_buffers - 2);
    const data_buffers = try allocator.alloc([*]const u8, num_data_buffers);

    for (0..num_data_buffers) |i| {
        data_buffers[i] = @ptrCast(array.array.buffers[i + 2].?);
    }

    const data = arr.BinaryViewArray{
        .views = import_buffer(arr.BinaryView, buffers[1], size),
        .buffers = data_buffers,
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
    };

    switch (ArrT) {
        arr.BinaryViewArray => {
            arr_ptr.* = data;
        },
        arr.Utf8ViewArray => {
            arr_ptr.* = .{
                .inner = data,
            };
        },
        else => @compileError("unexpected array type"),
    }

    return arr.Array.from(arr_ptr);
}

fn import_decimal_impl(comptime T: type, comptime ArrT: type, params: arr.DecimalParams, array: *const FFI_Array, allocator: Allocator) !arr.Array {
    const buffers = array.array.buffers.?;
    if (array.array.n_buffers != 2) {
        return error.InvalidFFIArray;
    }

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = try import_validity(array.schema.flags, buffers[0], size, allocator);

    const arr_ptr = try allocator.create(ArrT);
    arr_ptr.* = ArrT{
        .inner = .{
            .values = import_buffer(T, buffers[1], size),
            .validity = validity,
            .len = len,
            .offset = offset,
            .null_count = null_count,
        },
        .params = params,
    };

    return arr.Array.from(arr_ptr);
}

fn import_decimal(format: []const u8, array: *const FFI_Array, allocator: Allocator) !arr.Array {
    if (format[1] != ':') {
        return error.InvalidFormatStr;
    }

    var precision: ?u8 = null;
    var scale: ?i8 = null;

    var it = std.mem.splitSequence(u8, format[2..], ",");
    while (it.next()) |s| {
        if (precision == null) {
            precision = try std.fmt.parseInt(u8, s, 10);
        } else if (scale == null) {
            scale = try std.fmt.parseInt(i8, s, 10);
        } else {
            if (it.next() != null) {
                return error.InvalidFormatStr;
            }

            const params = arr.DecimalParams{
                .precision = precision.?,
                .scale = scale.?,
            };

            if (std.mem.eql(u8, s, "32")) {
                return import_decimal_impl(i32, arr.Decimal32Array, params, array, allocator);
            } else if (std.mem.eql(u8, s, "64")) {
                return import_decimal_impl(i64, arr.Decimal64Array, params, array, allocator);
            } else if (std.mem.eql(u8, s, "128")) {
                return import_decimal_impl(i128, arr.Decimal128Array, params, array, allocator);
            } else if (std.mem.eql(u8, s, "256")) {
                return import_decimal_impl(i256, arr.Decimal256Array, params, array, allocator);
            } else {
                return error.UnsupportedDecimalWidth;
            }
        }
    }

    return import_decimal_impl(i128, arr.Decimal128Array, .{
        .precision = precision.?,
        .scale = scale.?,
    }, array, allocator);
}

fn import_fixed_size_binary(format: []const u8, array: *const FFI_Array, allocator: Allocator) !arr.Array {
    if (format[1] != ':') {
        return error.InvalidFormatStr;
    }

    const byte_width = try std.fmt.parseInt(u32, format[2..], 10);

    const buffers = array.array.buffers.?;
    if (array.array.n_buffers != 2) {
        return error.InvalidFFIArray;
    }

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = try import_validity(array.schema.flags, buffers[0], size, allocator);

    const arr_ptr = try allocator.create(arr.FixedSizeBinaryArray);
    arr_ptr.* = .{
        .data = import_buffer(u8, buffers[1], size * byte_width),
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
        .byte_width = byte_width,
    };

    return arr.Array.from(arr_ptr);
}

fn import_date(comptime ArrT: type, array: *const FFI_Array, allocator: Allocator) !arr.Array {
    const T = comptime switch (ArrT) {
        arr.Date32Array => i32,
        arr.Date64Array => i64,
        else => @compileError("unknown array type"),
    };

    const InnerT = comptime switch (ArrT) {
        arr.Date32Array => arr.Int32Array,
        arr.Date64Array => arr.Int64Array,
        else => @compileError("unknown array type"),
    };

    const inner = try import_primitive_impl(T, InnerT, array, allocator);

    const arr_ptr = try allocator.create(ArrT);

    arr_ptr.* = ArrT{
        .inner = inner,
    };

    return arr.Array.from(arr_ptr);
}

fn import_time(comptime ArrT: type, unit: anytype, array: *const FFI_Array, allocator: Allocator) !arr.Array {
    const T = comptime switch (ArrT) {
        arr.Time32Array => i32,
        arr.Time64Array => i64,
        else => @compileError("unknown array type"),
    };

    const InnerT = comptime switch (ArrT) {
        arr.Time32Array => arr.Int32Array,
        arr.Time64Array => arr.Int64Array,
        else => @compileError("unknown array type"),
    };

    const inner = try import_primitive_impl(T, InnerT, array, allocator);

    const arr_ptr = try allocator.create(ArrT);

    arr_ptr.* = ArrT{
        .inner = inner,
        .unit = unit,
    };

    return arr.Array.from(arr_ptr);
}

fn import_timestamp(format: []const u8, unit: arr.TimestampUnit, array: *const FFI_Array, allocator: Allocator) !arr.Array {
    if (format[3] != ':') {
        return error.InvalidFormatStr;
    }

    const timezone = if (format.len >= 4)
        format[4..]
    else
        null;

    const inner = try import_primitive_impl(i64, arr.Int64Array, array, allocator);

    const arr_ptr = try allocator.create(arr.TimestampArray);

    arr_ptr.* = arr.TimestampArray{
        .inner = inner,
        .ts = arr.Timestamp{
            .unit = unit,
            .timezone = timezone,
        },
    };

    return arr.Array.from(arr_ptr);
}

fn import_duration(unit: arr.TimestampUnit, array: *const FFI_Array, allocator: Allocator) !arr.Array {
    const inner = try import_primitive_impl(i64, arr.Int64Array, array, allocator);

    const arr_ptr = try allocator.create(arr.DurationArray);

    arr_ptr.* = arr.DurationArray{
        .inner = inner,
        .unit = unit,
    };

    return arr.Array.from(arr_ptr);
}

fn import_interval(comptime ArrT: type, comptime T: type, array: *const FFI_Array, allocator: Allocator) !arr.Array {
    const buffers = array.array.buffers.?;
    if (array.array.n_buffers != 2) {
        return error.InvalidFFIArray;
    }

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = try import_validity(array.schema.flags, buffers[0], size, allocator);

    const arr_ptr = try allocator.create(ArrT);

    arr_ptr.* = .{ .inner = .{
        .values = import_buffer(T, buffers[1], size),
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
    } };

    return arr.Array.from(arr_ptr);
}

fn import_list(comptime ArrT: type, comptime IndexT: type, array: *const FFI_Array, allocator: Allocator) !arr.Array {
    const buffers = array.array.buffers.?;
    if (array.array.n_buffers != 2) {
        return error.InvalidFFIArray;
    }

    if (array.array.n_children != 1 or array.schema.n_children != 1) {
        return error.InvalidFFIArray;
    }

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = try import_validity(array.schema.flags, buffers[0], size, allocator);

    const child = FFI_Array{
        .array = array.array.children[0].*,
        .schema = array.schema.children[0].*,
    };

    const inner = try import_array(&child, allocator);

    const arr_ptr = try allocator.create(ArrT);
    arr_ptr.* = .{
        .inner = inner,
        .offsets = import_buffer(IndexT, buffers[1], size + 1),
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
    };

    return arr.Array.from(arr_ptr);
}

fn import_list_view(comptime ArrT: type, comptime IndexT: type, array: *const FFI_Array, allocator: Allocator) !arr.Array {
    const buffers = array.array.buffers.?;
    if (array.array.n_buffers != 3) {
        return error.InvalidFFIArray;
    }

    if (array.array.n_children != 1 or array.schema.n_children != 1) {
        return error.InvalidFFIArray;
    }

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = try import_validity(array.schema.flags, buffers[0], size, allocator);

    const child = FFI_Array{
        .array = array.array.children[0].*,
        .schema = array.schema.children[0].*,
    };

    const inner = try import_array(&child, allocator);

    const arr_ptr = try allocator.create(ArrT);
    arr_ptr.* = .{
        .inner = inner,
        .offsets = import_buffer(IndexT, buffers[1], size),
        .sizes = import_buffer(IndexT, buffers[2], size),
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
    };

    return arr.Array.from(arr_ptr);
}

fn import_fixed_size_list(format: []const u8, array: *const FFI_Array, allocator: Allocator) !arr.Array {
    const buffers = array.array.buffers.?;
    if (array.array.n_buffers != 1) {
        return error.InvalidFFIArray;
    }

    if (array.array.n_children != 1 or array.schema.n_children != 1) {
        return error.InvalidFFIArray;
    }

    if (format.len <= 3 or format[2] != ':') {
        return error.InvalidFormatStr;
    }

    const item_width_s = format[3..];
    const item_width = try std.fmt.parseInt(i32, item_width_s, 10);

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = try import_validity(array.schema.flags, buffers[0], size, allocator);

    const child = FFI_Array{
        .array = array.array.children[0].*,
        .schema = array.schema.children[0].*,
    };

    const inner = try import_array(&child, allocator);

    const arr_ptr = try allocator.create(arr.FixedSizeListArray);
    arr_ptr.* = .{
        .inner = inner,
        .item_width = item_width,
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
    };

    return arr.Array.from(arr_ptr);
}

fn import_struct(array: *const FFI_Array, allocator: Allocator) !arr.Array {
    const buffers = array.array.buffers.?;

    if (array.array.n_buffers != 1) {
        return error.InvalidFFIArray;
    }
    if (array.array.n_children != array.schema.n_children) {
        return error.InvalidFFIArray;
    }

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = try import_validity(array.schema.flags, buffers[0], size, allocator);

    const n_fields: u32 = @intCast(array.array.n_children);

    const field_names = try allocator.alloc([:0]const u8, n_fields);
    for (0..n_fields) |i| {
        field_names[i] = std.mem.span(array.schema.children[i].*.name);
    }

    const field_values = try allocator.alloc(arr.Array, n_fields);
    for (0..n_fields) |i| {
        const child = FFI_Array{
            .array = array.array.children[i].*,
            .schema = array.schema.children[i].*,
        };
        field_values[i] = try import_array(&child, allocator);
    }

    const arr_ptr = try allocator.create(arr.StructArray);

    arr_ptr.* = .{
        .field_values = field_values,
        .field_names = field_names,
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
    };

    return arr.Array.from(arr_ptr);
}

fn import_map(array: *const FFI_Array, allocator: Allocator) !arr.Array {
    const buffers = array.array.buffers.?;
    if (array.array.n_buffers != 2) {
        return error.InvalidFFIArray;
    }

    if (array.array.n_children != 1 or array.schema.n_children != 1) {
        return error.InvalidFFIArray;
    }

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = try import_validity(array.schema.flags, buffers[0], size, allocator);

    const child = FFI_Array{
        .array = array.array.children[0].*,
        .schema = array.schema.children[0].*,
    };

    const entries = try import_array(&child, allocator);

    const arr_ptr = try allocator.create(arr.MapArray);
    arr_ptr.* = .{
        .entries = entries.to(.struct_).*,
        .validity = validity,
        .offsets = import_buffer(i32, buffers[1], size + 1),
        .len = len,
        .offset = offset,
        .null_count = null_count,
        .keys_are_sorted = (array.schema.flags & abi.ARROW_FLAG_MAP_KEYS_SORTED) != 0,
    };

    return arr.Array.from(arr_ptr);
}

fn import_union(comptime ArrT: type, format: []const u8, array: *const FFI_Array, allocator: Allocator) !arr.Array {
    if (format[3] != ':') {
        return error.InvalidFormatStr;
    }

    var it = std.mem.splitSequence(u8, format[3..], ",");
    const num_type_ids = if (format.len > 3) std.mem.count(u8, format[3..], ",") + 1 else 0;

    const type_id_set = try allocator.alloc(i8, num_type_ids);

    for (0..num_type_ids) |i| {
        type_id_set[i] = try std.fmt.parseInt(i8, it.next().?, 10);
    }
    std.debug.assert(it.next() == null);

    const buffers = array.array.buffers.?;

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;

    const n_fields: u32 = @intCast(array.array.n_children);

    std.debug.assert(n_fields == num_type_ids);

    const children = try allocator.alloc(arr.Array, n_fields);
    for (0..n_fields) |i| {
        const child = FFI_Array{
            .array = array.array.children[i].*,
            .schema = array.schema.children[i].*,
        };
        children[i] = try import_array(&child, allocator);
    }

    const type_ids = import_buffer(i8, buffers[0], size);

    const arr_ptr = try allocator.create(ArrT);

    switch (ArrT) {
        arr.DenseUnionArray => {
            const offsets = import_buffer(i32, buffers[1], size);
            arr_ptr.* = .{
                .type_ids = type_ids,
                .type_id_set = type_id_set,
                .offsets = offsets,
                .children = children,
                .len = len,
                .offset = offset,
            };
        },
        arr.SparseUnionArray => {
            arr_ptr.* = .{
                .type_ids = type_ids,
                .type_id_set = type_id_set,
                .children = children,
                .len = len,
                .offset = offset,
            };
        },
        else => @compileError("unexpected array type"),
    }

    return arr.Array.from(arr_ptr);
}

pub fn import_run_end(array: *const FFI_Array, allocator: Allocator) !arr.Array {
    if (array.array.n_children != 2 or array.schema.n_children != 2) {
        return error.InvalidFFIArray;
    }

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const null_count: u32 = @intCast(array.array.null_count);

    const run_ends_ffi = FFI_Array{
        .array = array.array.children[0].*,
        .schema = array.schema.children[0].*,
    };
    const run_ends = try import_array(&run_ends_ffi, allocator);

    const values_ffi = FFI_Array{
        .array = array.array.children[1].*,
        .schema = array.schema.children[1].*,
    };
    const values = try import_array(&values_ffi, allocator);

    const arr_ptr = try allocator.create(arr.RunEndArray);
    arr_ptr.* = .{
        .run_ends = run_ends,
        .values = values,
        .len = len,
        .offset = offset,
        .null_count = null_count,
    };

    return arr.Array.from(arr_ptr);
}

pub fn import_array(array: *const FFI_Array, allocator: Allocator) FFIError!arr.Array {
    if (array.array.dictionary != null) {
        const keys = try import_array(array, allocator);

        const dict_ffi = FFI_Array{
            .array = array.array.dictionary.?.*,
            .schema = array.schema.dictionary.?.*,
        };

        const values = try import_array(&dict_ffi, allocator);

        const values_are_ordered = array.schema.flags & abi.ARROW_FLAG_DICTIONARY_ORDERED != 0;

        const dict_ptr = try allocator.create(arr.DictArray);

        const len: u32 = @intCast(array.array.length);
        const offset: u32 = @intCast(array.array.offset);
        const null_count: u32 = @intCast(array.array.null_count);

        dict_ptr.* = .{
            .values = values,
            .keys = keys,
            .values_are_ordered = values_are_ordered,
            .len = len,
            .offset = offset,
            .null_count = null_count,
        };

        return arr.Array.from(dict_ptr);
    }

    const format_str = array.schema.format orelse return error.InvalidFFIArray;
    const format: []const u8 = std.mem.span(format_str);
    if (format.len == 0) {
        return error.InvalidFFIArray;
    }
    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    switch (format[0]) {
        'n' => {
            const null_arr = try allocator.create(arr.NullArray);
            null_arr.* = arr.NullArray{
                .len = len,
            };

            return arr.Array.from(null_arr);
        },
        'b' => {
            const buffers = array.array.buffers.?;
            if (array.array.n_buffers != 2) {
                return error.InvalidFFIArray;
            }

            const validity = try import_validity(array.schema.flags, buffers[0], size, allocator);

            const bool_arr = try allocator.create(arr.BoolArray);
            bool_arr.* = arr.BoolArray{
                .values = import_buffer(u8, buffers[1], validity_size(size)),
                .validity = validity,
                .len = len,
                .offset = offset,
                .null_count = null_count,
            };

            return arr.Array.from(bool_arr);
        },
        'c' => {
            return import_primitive(i8, arr.Int8Array, array, allocator);
        },
        'C' => {
            return import_primitive(u8, arr.UInt8Array, array, allocator);
        },
        's' => {
            return import_primitive(i16, arr.Int16Array, array, allocator);
        },
        'S' => {
            return import_primitive(u16, arr.UInt16Array, array, allocator);
        },
        'i' => {
            return import_primitive(i32, arr.Int32Array, array, allocator);
        },
        'I' => {
            return import_primitive(u32, arr.UInt32Array, array, allocator);
        },
        'l' => {
            return import_primitive(i64, arr.Int64Array, array, allocator);
        },
        'L' => {
            return import_primitive(u64, arr.UInt64Array, array, allocator);
        },
        'e' => {
            return import_primitive(f16, arr.Float16Array, array, allocator);
        },
        'f' => {
            return import_primitive(f32, arr.Float32Array, array, allocator);
        },
        'g' => {
            return import_primitive(f64, arr.Float64Array, array, allocator);
        },
        'z' => {
            return import_binary(i32, arr.BinaryArray, array, allocator);
        },
        'Z' => {
            return import_binary(i64, arr.LargeBinaryArray, array, allocator);
        },
        'u' => {
            return import_binary(i32, arr.Utf8Array, array, allocator);
        },
        'U' => {
            return import_binary(i64, arr.LargeUtf8Array, array, allocator);
        },
        'v' => {
            return switch (format[1]) {
                'z' => import_binary_view(arr.BinaryViewArray, array, allocator),
                'u' => import_binary_view(arr.Utf8ViewArray, array, allocator),
                else => {
                    return error.InvalidFFIArray;
                },
            };
        },
        'd' => {
            return import_decimal(format, array, allocator);
        },
        'w' => {
            return import_fixed_size_binary(format, array, allocator);
        },
        't' => {
            return switch (format[1]) {
                'd' => switch (format[2]) {
                    'D' => import_date(arr.Date32Array, array, allocator),
                    'm' => import_date(arr.Date64Array, array, allocator),
                    else => error.InvalidFormatStr,
                },
                't' => switch (format[2]) {
                    's' => import_time(arr.Time32Array, .second, array, allocator),
                    'm' => import_time(arr.Time32Array, .millisecond, array, allocator),
                    'u' => import_time(arr.Time64Array, .microsecond, array, allocator),
                    'n' => import_time(arr.Time64Array, .nanosecond, array, allocator),
                    else => error.InvalidFormatStr,
                },
                's' => switch (format[2]) {
                    's' => import_timestamp(format, .second, array, allocator),
                    'm' => import_timestamp(format, .millisecond, array, allocator),
                    'u' => import_timestamp(format, .microsecond, array, allocator),
                    'n' => import_timestamp(format, .nanosecond, array, allocator),
                    else => error.InvalidFormatStr,
                },
                'D' => switch (format[2]) {
                    's' => import_duration(.second, array, allocator),
                    'm' => import_duration(.millisecond, array, allocator),
                    'u' => import_duration(.microsecond, array, allocator),
                    'n' => import_duration(.nanosecond, array, allocator),
                    else => error.InvalidFormatStr,
                },
                'i' => switch (format[2]) {
                    'M' => import_interval(arr.IntervalYearMonthArray, i32, array, allocator),
                    'D' => import_interval(arr.IntervalDayTimeArray, [2]i32, array, allocator),
                    'n' => import_interval(arr.IntervalMonthDayNanoArray, arr.MonthDayNano, array, allocator),
                    else => error.InvalidFormatStr,
                },
                else => error.InvalidFormatStr,
            };
        },
        '+' => {
            return switch (format[1]) {
                'l' => import_list(arr.ListArray, i32, array, allocator),
                'L' => import_list(arr.LargeListArray, i64, array, allocator),
                'v' => switch (format[2]) {
                    'l' => import_list_view(arr.ListViewArray, i32, array, allocator),
                    'L' => import_list_view(arr.LargeListViewArray, i64, array, allocator),
                    else => return error.InvalidFormatStr,
                },
                'w' => import_fixed_size_list(format, array, allocator),
                's' => import_struct(array, allocator),
                'm' => import_map(array, allocator),
                'u' => switch (format[2]) {
                    'd' => import_union(arr.DenseUnionArray, format, array, allocator),
                    's' => import_union(arr.SparseUnionArray, format, array, allocator),
                    else => return error.InvalidFormatStr,
                },
                'r' => import_run_end(array, allocator),
                else => return error.InvalidFormatStr,
            };
        },
        else => return error.InvalidFormatStr,
    }
}

const PrivateData = struct {
    ref_count: std.atomic.Value(i32),
    arena: ArenaAllocator,

    fn init(arena: ArenaAllocator) !*PrivateData {
        const self = try arena.child_allocator.create(PrivateData);
        self.* = .{
            .arena = arena,
            .ref_count = std.atomic.Value(i32).init(1),
        };
        return self;
    }

    fn deinit(self: *PrivateData) void {
        const backing_alloc = self.arena.child_allocator;
        self.arena.deinit();
        backing_alloc.destroy(self);
    }

    fn increment(self: *PrivateData) *PrivateData {
        _ = self.ref_count.fetchAdd(1, .monotonic);
        return self;
    }

    fn decrement(self: *PrivateData) void {
        if (self.ref_count.fetchSub(1, .release) == 1) {
            _ = self.ref_count.load(.acquire);

            self.deinit();
        }
    }
};

fn release_impl(comptime T: type, data: [*c]T) void {
    const ptr = data orelse unreachable;
    const obj = ptr.*;

    const n_children: usize = @intCast(obj.n_children);

    for (0..n_children) |i| {
        const child = obj.children[i];
        if (child.*.release) |r| {
            r(child);
            std.debug.assert(child.*.release == null);
        }
    }

    if (obj.dictionary) |dict| {
        if (dict.*.release) |r| {
            r(obj.dictionary);
            std.debug.assert(dict.*.release == null);
        }
    }

    const arc: *PrivateData = @ptrCast(@alignCast(ptr.*.private_data));
    arc.decrement();

    ptr.*.release = null;
}

fn release_array(array: [*c]abi.ArrowArray) callconv(.C) void {
    release_impl(abi.ArrowArray, array);
}

fn release_schema(schema: [*c]abi.ArrowSchema) callconv(.C) void {
    release_impl(abi.ArrowSchema, schema);
}

/// calling arena.deinit should free all memory relating to this array
///
/// arena should be allocated using arena.child_allocator
///
/// arena.child_allocator should be alive whenever the consumer of FFI_Array decides to call release functions on abi array or schema
///
/// Generally arena should be allocated inside a global allocator like std.heap.GeneralPurposeAlloc and it should have that global alloc as it's child alloc.
/// and array should be allocated using this arena.
///
/// Ownership of the arena is transferred to the FFI_Array from this point on so the caller should not use it.
pub fn export_array(array: arr.Array, arena: ArenaAllocator) FFIError!FFI_Array {
    const private_data = try PrivateData.init(arena);
    errdefer private_data.deinit();

    const out = try export_array_impl(array, private_data);

    return out;
}

fn export_array_impl(array: arr.Array, private_data: *PrivateData) FFIError!FFI_Array {
    switch (array.type_) {
        .null => {
            const a = array.to(.null);

            return .{
                .schema = .{
                    .format = "n",
                    .private_data = private_data.increment(),
                    .release = release_schema,
                },
                .array = .{
                    .length = a.len,
                    .offset = 0,
                    .private_data = private_data,
                    .release = release_array,
                },
            };
        },
        .i8 => {
            return export_primitive(array.to(.i8), private_data);
        },
        .i16 => {
            return export_primitive(array.to(.i16), private_data);
        },
        .i32 => {
            return export_primitive(array.to(.i32), private_data);
        },
        .i64 => {
            return export_primitive(array.to(.i64), private_data);
        },
        .u8 => {
            return export_primitive(array.to(.u8), private_data);
        },
        .u16 => {
            return export_primitive(array.to(.u16), private_data);
        },
        .u32 => {
            return export_primitive(array.to(.u32), private_data);
        },
        .u64 => {
            return export_primitive(array.to(.u64), private_data);
        },
        .f16 => {
            return export_primitive(array.to(.f16), private_data);
        },
        .f32 => {
            return export_primitive(array.to(.f32), private_data);
        },
        .f64 => {
            return export_primitive(array.to(.f64), private_data);
        },
        .binary => {
            return export_binary(array.to(.binary), private_data, "z");
        },
        .large_binary => {
            return export_binary(array.to(.large_binary), private_data, "Z");
        },
        .utf8 => {
            return export_binary(&array.to(.utf8).inner, private_data, "u");
        },
        .large_utf8 => {
            return export_binary(&array.to(.large_utf8).inner, private_data, "U");
        },
        .bool => {
            return export_primitive(array.to(.bool), private_data);
        },
        .binary_view => {
            return export_binary_view(array.to(.binary_view), private_data, "vz");
        },
        .utf8_view => {
            return export_binary_view(&array.to(.utf8_view).inner, private_data, "vu");
        },
        .decimal32 => {
            return export_decimal(array.to(.decimal32), private_data);
        },
        .decimal64 => {
            return export_decimal(array.to(.decimal64), private_data);
        },
        .decimal128 => {
            return export_decimal(array.to(.decimal128), private_data);
        },
        .decimal256 => {
            return export_decimal(array.to(.decimal256), private_data);
        },
        .fixed_size_binary => {
            return export_fixed_size_binary(array.to(.fixed_size_binary), private_data);
        },
        .date32 => {
            return export_date(array.to(.date32), private_data);
        },
        .date64 => {
            return export_date(array.to(.date64), private_data);
        },
        .time32 => {
            return export_time(array.to(.time32), private_data);
        },
        .time64 => {
            return export_time(array.to(.time64), private_data);
        },
        .timestamp => {
            return export_timestamp(array.to(.timestamp), private_data);
        },
        .duration => {
            return export_duration(array.to(.duration), private_data);
        },
        .interval_year_month => {
            return export_interval("tiM", array.to(.interval_year_month), private_data);
        },
        .interval_day_time => {
            return export_interval("tiD", array.to(.interval_day_time), private_data);
        },
        .interval_month_day_nano => {
            return export_interval("tin", array.to(.interval_month_day_nano), private_data);
        },
        .list => {
            return export_list(array.to(.list), private_data);
        },
        .large_list => {
            return export_list(array.to(.large_list), private_data);
        },
        .list_view => {
            return export_list_view(array.to(.list_view), private_data);
        },
        .large_list_view => {
            return export_list_view(array.to(.large_list_view), private_data);
        },
        .fixed_size_list => {
            return export_fixed_size_list(array.to(.fixed_size_list), private_data);
        },
        .struct_ => {
            return export_struct(array.to(.struct_), private_data);
        },
        .map => {
            return export_map(array.to(.map), private_data);
        },
        .dense_union => {
            return export_union(array.to(.dense_union), private_data);
        },
        .sparse_union => {
            return export_union(array.to(.sparse_union), private_data);
        },
        .run_end_encoded => {
            return export_run_end(array.to(.run_end_encoded), private_data);
        },
        .dict => {
            return export_dict(array.to(.dict), private_data);
        },
    }
}

fn export_dict(array: *const arr.DictArray, private_data: *PrivateData) !FFI_Array {
    var out = try export_array_impl(array.keys, private_data.increment());

    const allocator = private_data.arena.allocator();
    const dict_ptr = try allocator.create(FFI_Array);
    dict_ptr.* = try export_array_impl(array.values, private_data);
    out.array.dictionary = &dict_ptr.array;
    out.schema.dictionary = &dict_ptr.schema;

    if (array.values_are_ordered) {
        out.schema.flags |= abi.ARROW_FLAG_DICTIONARY_ORDERED;
    }

    return out;
}

fn export_run_end(array: *const arr.RunEndArray, private_data: *PrivateData) !FFI_Array {
    const allocator = private_data.arena.allocator();

    const children = try allocator.alloc(FFI_Array, 2);
    children[0] = try export_array_impl(array.run_ends, private_data.increment());
    children[0].schema.name = "run_ends";
    children[1] = try export_array_impl(array.values, private_data.increment());
    children[1].schema.name = "values";

    const array_children = try allocator.alloc([*c]abi.ArrowArray, 2);
    array_children[0] = &children[0].array;
    array_children[1] = &children[1].array;

    const schema_children = try allocator.alloc([*c]abi.ArrowSchema, 2);
    schema_children[0] = &children[0].schema;
    schema_children[1] = &children[1].schema;

    return .{
        .array = .{
            .n_children = 2,
            .children = array_children.ptr,
            .n_buffers = 0,
            .buffers = null,
            .offset = array.offset,
            .length = array.len,
            .null_count = array.null_count,
            .private_data = private_data.increment(),
            .release = release_array,
        },
        .schema = .{
            .n_children = 2,
            .children = schema_children.ptr,
            .format = "+r",
            .private_data = private_data,
            .release = release_schema,
        },
    };
}

fn export_union(array: anytype, private_data: *PrivateData) !FFI_Array {
    const format_base = switch (@TypeOf(array)) {
        *const arr.DenseUnionArray => "+ud:",
        *const arr.SparseUnionArray => "+us:",
        else => @compileError("unexpected array type"),
    };

    const n_fields = array.children.len;

    const allocator = private_data.arena.allocator();

    // size is calculated as base + 5  * num_type_ids + 1 because type ids are 8 bit integers so they can't
    // occupy more than 3 digits, a minus sign and a comma.
    // and need the last one byte to make it zero terminated.
    const format = try allocator.alloc(u8, 3 + 5 * array.type_id_set.len + 1);
    var write_idx: usize = 0;

    if (array.type_id_set.len > 0) {
        {
            const out = std.fmt.bufPrint(format[write_idx..], "{}", .{array.type_id_set[0]}) catch unreachable;
            write_idx += out.len;
        }
        for (1..array.type_id_set.len) |i| {
            const out = std.fmt.bufPrint(format[write_idx..], ",{}", .{array.type_id_set[i]}) catch unreachable;
            write_idx += out.len;
        }
    }

    format[write_idx] = 0;

    const buffers = try allocator.alloc(?*const anyopaque, 2);
    buffers[0] = array.type_ids.ptr;
    buffers[1] = switch (@TypeOf(array)) {
        *const arr.DenseUnionArray => array.offsets.ptr,
        *const arr.SparseUnionArray => null,
        else => @compileError("unexpected array type"),
    };

    const children = try allocator.alloc(FFI_Array, n_fields);
    for (0..n_fields) |i| {
        children[i] = try export_array_impl(array.children[i], private_data.increment());
    }

    const array_children = try allocator.alloc([*c]abi.ArrowArray, n_fields);
    for (0..n_fields) |i| {
        array_children[i] = &children[i].array;
    }

    const schema_children = try allocator.alloc([*c]abi.ArrowSchema, n_fields);
    for (0..n_fields) |i| {
        schema_children[i] = &children[i].schema;
    }

    return .{
        .array = .{
            .n_children = @as(i64, @intCast(n_fields)),
            .children = array_children.ptr,
            .n_buffers = if (buffers[1] == null) 1 else 2,
            .buffers = buffers.ptr,
            .offset = array.offset,
            .length = array.len,
            .null_count = 0,
            .private_data = private_data.increment(),
            .release = release_array,
        },
        .schema = .{
            .n_children = @as(i64, @intCast(n_fields)),
            .children = schema_children.ptr,
            .format = format_base,
            .private_data = private_data,
            .release = release_schema,
        },
    };
}

fn export_map(array: *const arr.MapArray, private_data: *PrivateData) !FFI_Array {
    const allocator = private_data.arena.allocator();

    const buffers = try allocator.alloc(?*const anyopaque, 2);
    buffers[0] = if (array.validity) |v| v.ptr else null;
    buffers[1] = array.offsets.ptr;

    const child = try allocator.create(FFI_Array);
    child.* = try export_array_impl(arr.Array.from(&array.entries), private_data.increment());
    child.schema.name = "entries";

    const array_children = try allocator.alloc([*c]abi.ArrowArray, 1);
    array_children[0] = &child.array;

    const schema_children = try allocator.alloc([*c]abi.ArrowSchema, 1);
    schema_children[0] = &child.schema;

    var flags: i64 = 0;
    if (array.keys_are_sorted) {
        flags |= abi.ARROW_FLAG_MAP_KEYS_SORTED;
    }
    if (array.validity != null) {
        flags |= abi.ARROW_FLAG_NULLABLE;
    }

    return .{
        .array = .{
            .n_children = 1,
            .children = array_children.ptr,
            .n_buffers = 2,
            .buffers = buffers.ptr,
            .offset = array.offset,
            .length = array.len,
            .null_count = array.null_count,
            .private_data = private_data.increment(),
            .release = release_array,
        },
        .schema = .{
            .n_children = 1,
            .children = schema_children.ptr,
            .format = "+m",
            .private_data = private_data,
            .release = release_schema,
            .flags = flags,
        },
    };
}

fn export_struct(array: *const arr.StructArray, private_data: *PrivateData) !FFI_Array {
    const n_fields = array.field_values.len;

    const allocator = private_data.arena.allocator();

    const buffers = try allocator.alloc(?*const anyopaque, 1);
    buffers[0] = if (array.validity) |v| v.ptr else null;

    const children = try allocator.alloc(FFI_Array, n_fields);
    for (0..n_fields) |i| {
        var out = try export_array_impl(array.field_values[i], private_data.increment());
        out.schema.name = array.field_names[i].ptr;
        children[i] = out;
    }

    const array_children = try allocator.alloc([*c]abi.ArrowArray, n_fields);
    for (0..n_fields) |i| {
        array_children[i] = &children[i].array;
    }

    const schema_children = try allocator.alloc([*c]abi.ArrowSchema, n_fields);
    for (0..n_fields) |i| {
        schema_children[i] = &children[i].schema;
    }

    return .{
        .array = .{
            .n_children = @as(i64, @intCast(n_fields)),
            .children = array_children.ptr,
            .n_buffers = 1,
            .buffers = buffers.ptr,
            .offset = array.offset,
            .length = array.len,
            .null_count = array.null_count,
            .private_data = private_data.increment(),
            .release = release_array,
        },
        .schema = .{
            .n_children = @as(i64, @intCast(n_fields)),
            .children = schema_children.ptr,
            .format = "+s",
            .private_data = private_data,
            .release = release_schema,
            .flags = if (array.validity != null) abi.ARROW_FLAG_NULLABLE else 0,
        },
    };
}

fn export_fixed_size_list(array: *const arr.FixedSizeListArray, private_data: *PrivateData) !FFI_Array {
    const allocator = private_data.arena.allocator();

    const format = try std.fmt.allocPrintZ(allocator, "+w:{}", .{array.item_width});

    const buffers = try allocator.alloc(?*const anyopaque, 1);
    buffers[0] = if (array.validity) |v| v.ptr else null;

    const child = try allocator.create(FFI_Array);
    child.* = try export_array_impl(array.inner, private_data.increment());

    const array_children = try allocator.alloc([*c]abi.ArrowArray, 1);
    array_children[0] = &child.array;

    const schema_children = try allocator.alloc([*c]abi.ArrowSchema, 1);
    schema_children[0] = &child.schema;

    return .{
        .array = .{
            .n_children = 1,
            .children = array_children.ptr,
            .n_buffers = 1,
            .buffers = buffers.ptr,
            .offset = array.offset,
            .length = array.len,
            .null_count = array.null_count,
            .private_data = private_data.increment(),
            .release = release_array,
        },
        .schema = .{
            .n_children = 1,
            .children = schema_children.ptr,
            .format = format,
            .private_data = private_data,
            .release = release_schema,
            .flags = if (array.validity != null) abi.ARROW_FLAG_NULLABLE else 0,
        },
    };
}

fn export_list_view(array: anytype, private_data: *PrivateData) !FFI_Array {
    const format = comptime switch (@TypeOf(array)) {
        *const arr.ListViewArray => "+vl",
        *const arr.LargeListViewArray => "+vL",
        else => @compileError("unexpected array type"),
    };

    const allocator = private_data.arena.allocator();
    const buffers = try allocator.alloc(?*const anyopaque, 3);
    buffers[0] = if (array.validity) |v| v.ptr else null;
    buffers[1] = array.offsets.ptr;
    buffers[2] = array.sizes.ptr;

    const child = try allocator.create(FFI_Array);
    child.* = try export_array_impl(array.inner, private_data.increment());

    const array_children = try allocator.alloc([*c]abi.ArrowArray, 1);
    array_children[0] = &child.array;

    const schema_children = try allocator.alloc([*c]abi.ArrowSchema, 1);
    schema_children[0] = &child.schema;

    return .{
        .array = .{
            .n_children = 1,
            .children = array_children.ptr,
            .n_buffers = 3,
            .buffers = buffers.ptr,
            .offset = array.offset,
            .length = array.len,
            .null_count = array.null_count,
            .private_data = private_data.increment(),
            .release = release_array,
        },
        .schema = .{
            .n_children = 1,
            .children = schema_children.ptr,
            .format = format,
            .private_data = private_data,
            .release = release_schema,
            .flags = if (array.validity != null) abi.ARROW_FLAG_NULLABLE else 0,
        },
    };
}

fn export_list(array: anytype, private_data: *PrivateData) !FFI_Array {
    const format = comptime switch (@TypeOf(array)) {
        *const arr.ListArray => "+l",
        *const arr.LargeListArray => "+L",
        else => @compileError("unexpected array type"),
    };

    const allocator = private_data.arena.allocator();
    const buffers = try allocator.alloc(?*const anyopaque, 2);
    buffers[0] = if (array.validity) |v| v.ptr else null;
    buffers[1] = array.offsets.ptr;

    const child = try allocator.create(FFI_Array);
    child.* = try export_array_impl(array.inner, private_data.increment());

    const array_children = try allocator.alloc([*c]abi.ArrowArray, 1);
    array_children[0] = &child.array;

    const schema_children = try allocator.alloc([*c]abi.ArrowSchema, 1);
    schema_children[0] = &child.schema;

    return .{
        .array = .{
            .n_children = 1,
            .children = array_children.ptr,
            .n_buffers = 2,
            .buffers = buffers.ptr,
            .offset = array.offset,
            .length = array.len,
            .null_count = array.null_count,
            .private_data = private_data.increment(),
            .release = release_array,
        },
        .schema = .{
            .n_children = 1,
            .children = schema_children.ptr,
            .format = format,
            .private_data = private_data,
            .release = release_schema,
            .flags = if (array.validity != null) abi.ARROW_FLAG_NULLABLE else 0,
        },
    };
}

fn export_interval(format: [:0]const u8, array: anytype, private_data: *PrivateData) !FFI_Array {
    return export_primitive_impl(format, &array.inner, private_data);
}

fn export_duration(dur_array: *const arr.DurationArray, private_data: *PrivateData) !FFI_Array {
    const format = switch (dur_array.unit) {
        .second => "tDs",
        .millisecond => "tDm",
        .microsecond => "tDu",
        .nanosecond => "tDn",
    };

    return export_primitive_impl(format, &dur_array.inner, private_data);
}

fn export_timestamp(timestamp_array: *const arr.TimestampArray, private_data: *PrivateData) !FFI_Array {
    const allocator = private_data.arena.allocator();

    const format_base = switch (timestamp_array.ts.unit) {
        .second => "tss:",
        .millisecond => "tsm:",
        .microsecond => "tsu:",
        .nanosecond => "tsn:",
    };

    const format = if (timestamp_array.ts.timezone) |tz|
        try std.fmt.allocPrintZ(allocator, "{s}{s}", .{ format_base, tz })
    else
        format_base;

    const out = try export_primitive_impl(format, &timestamp_array.inner, private_data);

    return out;
}

fn export_time(time_array: anytype, private_data: *PrivateData) !FFI_Array {
    const format = switch (@TypeOf(time_array)) {
        *const arr.Time32Array => switch (time_array.unit) {
            .second => "tts",
            .millisecond => "ttm",
        },
        *const arr.Time64Array => switch (time_array.unit) {
            .microsecond => "ttu",
            .nanosecond => "ttn",
        },
        else => @compileError("unexpected array type"),
    };

    return export_primitive_impl(format, &time_array.inner, private_data);
}

fn export_date(date_array: anytype, private_data: *PrivateData) !FFI_Array {
    const format = comptime switch (@TypeOf(date_array)) {
        *const arr.Date32Array => "tdD",
        *const arr.Date64Array => "tdm",
        else => @compileError("unexpected array type"),
    };

    return export_primitive_impl(format, &date_array.inner, private_data);
}

fn export_fixed_size_binary(array: *const arr.FixedSizeBinaryArray, private_data: *PrivateData) !FFI_Array {
    const allocator = private_data.arena.allocator();

    const format = try std.fmt.allocPrintZ(allocator, "w:{}", .{array.byte_width});

    const buffers = try allocator.alloc(?*const anyopaque, 2);

    buffers[0] = if (array.validity) |v| v.ptr else null;
    buffers[1] = array.data.ptr;

    return .{
        .array = .{
            .n_buffers = 2,
            .buffers = buffers.ptr,
            .offset = array.offset,
            .length = array.len,
            .null_count = array.null_count,
            .private_data = private_data.increment(),
            .release = release_array,
        },
        .schema = .{
            .format = format,
            .private_data = private_data,
            .release = release_schema,
            .flags = if (array.validity != null) abi.ARROW_FLAG_NULLABLE else 0,
        },
    };
}

fn export_decimal(dec_array: anytype, private_data: *PrivateData) !FFI_Array {
    const width = comptime switch (@TypeOf(dec_array)) {
        *const arr.Decimal32Array => "32",
        *const arr.Decimal64Array => "64",
        *const arr.Decimal128Array => "128",
        *const arr.Decimal256Array => "256",
        else => @compileError("unexpected array type"),
    };

    const allocator = private_data.arena.allocator();

    const format = try std.fmt.allocPrintZ(allocator, "d:{},{},{s}", .{ dec_array.params.precision, dec_array.params.scale, width });

    const out = try export_primitive_impl(format, &dec_array.inner, private_data);

    return out;
}

fn export_binary_view(array: *const arr.BinaryViewArray, private_data: *PrivateData, format: [:0]const u8) !FFI_Array {
    const n_buffers = array.buffers.len + 2;

    const allocator = private_data.arena.allocator();
    const buffers = try allocator.alloc(?*const anyopaque, n_buffers);
    buffers[0] = if (array.validity) |v| v.ptr else null;
    buffers[1] = array.views.ptr;

    for (array.buffers, 0..) |b, i| {
        buffers[i + 2] = b;
    }

    return .{
        .array = .{
            .n_buffers = @as(i64, @intCast(n_buffers)),
            .buffers = buffers.ptr,
            .offset = array.offset,
            .length = array.len,
            .null_count = array.null_count,
            .private_data = private_data.increment(),
            .release = release_array,
        },
        .schema = .{
            .format = format,
            .private_data = private_data,
            .release = release_schema,
            .flags = if (array.validity != null) abi.ARROW_FLAG_NULLABLE else 0,
        },
    };
}

fn export_binary(array: anytype, private_data: *PrivateData, format: [:0]const u8) !FFI_Array {
    const allocator = private_data.arena.allocator();
    const buffers = try allocator.alloc(?*const anyopaque, 3);
    buffers[0] = if (array.validity) |v| v.ptr else null;
    buffers[1] = array.offsets.ptr;
    buffers[2] = array.data.ptr;

    return .{
        .array = .{
            .n_buffers = 3,
            .buffers = buffers.ptr,
            .offset = array.offset,
            .length = array.len,
            .null_count = array.null_count,
            .private_data = private_data.increment(),
            .release = release_array,
        },
        .schema = .{
            .format = format,
            .private_data = private_data,
            .release = release_schema,
            .flags = if (array.validity != null) abi.ARROW_FLAG_NULLABLE else 0,
        },
    };
}

fn export_primitive_impl(format: [:0]const u8, array: anytype, private_data: *PrivateData) !FFI_Array {
    const allocator = private_data.arena.allocator();
    const buffers = try allocator.alloc(?*const anyopaque, 2);
    buffers[0] = if (array.validity) |v| v.ptr else null;
    buffers[1] = array.values.ptr;

    return .{
        .array = .{
            .n_buffers = 2,
            .buffers = buffers.ptr,
            .offset = array.offset,
            .length = array.len,
            .null_count = array.null_count,
            .private_data = private_data.increment(),
            .release = release_array,
        },
        .schema = .{
            .format = format,
            .private_data = private_data,
            .release = release_schema,
            .flags = if (array.validity != null) abi.ARROW_FLAG_NULLABLE else 0,
        },
    };
}

fn export_primitive(array: anytype, private_data: *PrivateData) !FFI_Array {
    const format = comptime switch (@TypeOf(array)) {
        *const arr.Int8Array => "c",
        *const arr.UInt8Array => "C",
        *const arr.Int16Array => "s",
        *const arr.UInt16Array => "S",
        *const arr.Int32Array => "i",
        *const arr.UInt32Array => "I",
        *const arr.Int64Array => "l",
        *const arr.UInt64Array => "L",
        *const arr.Float16Array => "e",
        *const arr.Float32Array => "f",
        *const arr.Float64Array => "g",
        *const arr.BoolArray => "b",
        else => @compileError("unexpected array type"),
    };

    return export_primitive_impl(format, array, private_data);
}

pub const FFIError = error{
    InvalidFFIArray,
    InvalidFormatStr,
    OutOfMemory,
    InvalidCharacter,
    Overflow,
    UnsupportedDecimalWidth,
};

test "roundtrip" {
    const original_data = [_]i32{ 3, 2, 1, 4, 5, 6 };

    var export_arena = ArenaAllocator.init(std.testing.allocator);
    const export_alloc = export_arena.allocator();
    const typed = export_alloc.create(arr.Int32Array) catch unreachable;
    const values = export_alloc.alloc(i32, 6) catch unreachable;
    @memcpy(values, &original_data);

    typed.* = .{
        .len = 5,
        .offset = 1,
        .validity = null,
        .values = values,
        .null_count = 0,
    };

    const array = arr.Array.from(typed);

    // use 'catch unreachable' up to here beacuse we don't want everything to leak
    // and just using defer isn't feasible because all of that ownership is handed to the consumer
    // of FFI_Array
    //
    // would have to handle this in a more complete way in a real application.
    var ffi_array = export_array(array, export_arena) catch unreachable;
    defer ffi_array.release();

    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const roundtrip_array = try import_array(&ffi_array, allocator);

    const roundtrip_typed = roundtrip_array.to(.i32);

    try std.testing.expectEqualDeep(
        original_data[1..],
        roundtrip_typed.values[roundtrip_typed.offset .. roundtrip_typed.offset + roundtrip_typed.len],
    );
}
