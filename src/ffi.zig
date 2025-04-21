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

fn import_primitive_impl(comptime T: type, comptime ArrT: type, array: *const FFI_Array) !ArrT {
    const buffers = array.array.buffers.?;
    if (array.array.n_buffers != 2) {
        return error.InvalidFFIArray;
    }

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const byte_size = validity_size(size);
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = if (buffers[0]) |b| import_buffer(u8, b, byte_size) else null;

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
    arr_ptr.* = try import_primitive_impl(T, ArrT, array);
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
    const byte_size = validity_size(size);
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = if (buffers[0]) |b| import_buffer(u8, b, byte_size) else null;

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
    const byte_size = validity_size(size);
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = if (buffers[0]) |b| import_buffer(u8, b, byte_size) else null;

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
    const byte_size = validity_size(size);
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = if (buffers[0]) |b| import_buffer(u8, b, byte_size) else null;

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
    const byte_size = validity_size(size);
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = if (buffers[0]) |b| import_buffer(u8, b, byte_size) else null;

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

    const inner = try import_primitive_impl(T, InnerT, array);

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

    const inner = try import_primitive_impl(T, InnerT, array);

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

    const inner = try import_primitive_impl(i64, arr.Int64Array, array);

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
    const inner = try import_primitive_impl(i64, arr.Int64Array, array);

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
    const byte_size = validity_size(size);
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = if (buffers[0]) |b| import_buffer(u8, b, byte_size) else null;

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
    const byte_size = validity_size(size);
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = if (buffers[0]) |b| import_buffer(u8, b, byte_size) else null;

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

pub fn import_array(array: *const FFI_Array, allocator: Allocator) FFIError!arr.Array {
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

            const byte_size = validity_size(size);
            const validity = if (buffers[0]) |b| import_buffer(u8, b, byte_size) else null;

            const bool_arr = try allocator.create(arr.BoolArray);
            bool_arr.* = arr.BoolArray{
                .values = import_buffer(u8, buffers[1], byte_size),
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
                // 'v' => {},
                // 'w' => {},
                // 's' => {},
                // 'm' => {},
                // 'u' => {},
                // 'r' => {},
                else => return error.InvalidFormatStr,
            };
        },
        else => return error.InvalidFormatStr,
    }
}

fn release_array(array: [*c]abi.ArrowArray) callconv(.C) void {
    const ptr = array orelse unreachable;
    const arena: *ArenaAllocator = @ptrCast(@alignCast(ptr.*.private_data));
    const backing_alloc = arena.*.child_allocator;
    arena.deinit();
    backing_alloc.destroy(arena);
}

// This is no-op because actual releasing of memory happens
// when release_array is called
fn release_schema(_: [*c]abi.ArrowSchema) callconv(.C) void {}

/// calling arena.deinit should free all memory relating to this array
///
/// arena should be allocated using arena.child_allocator
///
/// arena.child_allocator should be alive whenever the consumer of FFI_Array decides to call release functions on abi array or schema
///
/// Generally arena should be allocated inside a global allocator like std.heap.GeneralPurposeAlloc and it should have that global alloc as it's child alloc.
/// and array should be allocated using this arena.
pub fn export_array(array: arr.Array, arena: *ArenaAllocator) FFIError!FFI_Array {
    switch (array.type_) {
        .null => {
            const a = array.to(.null);

            return .{
                .schema = .{
                    .format = "n",
                    .private_data = arena,
                    .release = release_schema,
                },
                .array = .{
                    .length = a.len,
                    .offset = 0,
                    .private_data = arena,
                    .release = release_array,
                },
            };
        },
        .i8 => {
            return export_primitive(array.to(.i8), arena);
        },
        .i16 => {
            return export_primitive(array.to(.i16), arena);
        },
        .i32 => {
            return export_primitive(array.to(.i32), arena);
        },
        .i64 => {
            return export_primitive(array.to(.i64), arena);
        },
        .u8 => {
            return export_primitive(array.to(.u8), arena);
        },
        .u16 => {
            return export_primitive(array.to(.u16), arena);
        },
        .u32 => {
            return export_primitive(array.to(.u32), arena);
        },
        .u64 => {
            return export_primitive(array.to(.u64), arena);
        },
        .f16 => {
            return export_primitive(array.to(.f16), arena);
        },
        .f32 => {
            return export_primitive(array.to(.f32), arena);
        },
        .f64 => {
            return export_primitive(array.to(.f64), arena);
        },
        .binary => {
            return export_binary(array.to(.binary), arena, "z");
        },
        .large_binary => {
            return export_binary(array.to(.large_binary), arena, "Z");
        },
        .utf8 => {
            return export_binary(&array.to(.utf8).inner, arena, "u");
        },
        .large_utf8 => {
            return export_binary(&array.to(.large_utf8).inner, arena, "U");
        },
        .bool => {
            return export_primitive(array.to(.bool), arena);
        },
        .binary_view => {
            return export_binary_view(array.to(.binary_view), arena, "vz");
        },
        .utf8_view => {
            return export_binary_view(&array.to(.utf8_view).inner, arena, "vu");
        },
        .decimal32 => {
            return export_decimal(array.to(.decimal32), arena);
        },
        .decimal64 => {
            return export_decimal(array.to(.decimal64), arena);
        },
        .decimal128 => {
            return export_decimal(array.to(.decimal128), arena);
        },
        .decimal256 => {
            return export_decimal(array.to(.decimal256), arena);
        },
        .fixed_size_binary => {
            return export_fixed_size_binary(array.to(.fixed_size_binary), arena);
        },
        .date32 => {
            return export_date(array.to(.date32), arena);
        },
        .date64 => {
            return export_date(array.to(.date64), arena);
        },
        .time32 => {
            return export_time(array.to(.time32), arena);
        },
        .time64 => {
            return export_time(array.to(.time64), arena);
        },
        .timestamp => {
            return export_timestamp(array.to(.timestamp), arena);
        },
        .duration => {
            return export_duration(array.to(.duration), arena);
        },
        .interval_year_month => {
            return export_interval("tiM", array.to(.interval_year_month), arena);
        },
        .interval_day_time => {
            return export_interval("tiD", array.to(.interval_day_time), arena);
        },
        .interval_month_day_nano => {
            return export_interval("tin", array.to(.interval_month_day_nano), arena);
        },
        .list => {
            return export_list(array.to(.list), arena);
        },
        .large_list => {
            return export_list(array.to(.large_list), arena);
        },
        else => unreachable,
    }
}

fn export_list(array: anytype, arena: *ArenaAllocator) !FFI_Array {
    const format = comptime switch (@TypeOf(array)) {
        *const arr.ListArray => "+l",
        *const arr.LargeListArray => "+L",
        else => @compileError("unexpected array type"),
    };

    const allocator = arena.allocator();
    const buffers = try allocator.alloc(?*const anyopaque, 2);
    buffers[0] = if (array.validity) |v| v.ptr else null;
    buffers[1] = array.offsets.ptr;

    const child = try allocator.create(FFI_Array);
    child.* = try export_array(array.inner, arena);

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
            .private_data = arena,
            .release = release_array,
        },
        .schema = .{
            .n_children = 1,
            .children = schema_children.ptr,
            .format = format,
            .private_data = arena,
            .release = release_schema,
        },
    };
}

fn export_interval(format: [:0]const u8, array: anytype, arena: *ArenaAllocator) !FFI_Array {
    return export_primitive_impl(format, &array.inner, arena);
}

fn export_duration(dur_array: *const arr.DurationArray, arena: *ArenaAllocator) !FFI_Array {
    const format = switch (dur_array.unit) {
        .second => "tDs",
        .millisecond => "tDm",
        .microsecond => "tDu",
        .nanosecond => "tDn",
    };

    return export_primitive_impl(format, &dur_array.inner, arena);
}

fn export_timestamp(timestamp_array: *const arr.TimestampArray, arena: *ArenaAllocator) !FFI_Array {
    const allocator = arena.allocator();

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

    const out = try export_primitive_impl(format, &timestamp_array.inner, arena);

    return out;
}

fn export_time(time_array: anytype, arena: *ArenaAllocator) !FFI_Array {
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

    return export_primitive_impl(format, &time_array.inner, arena);
}

fn export_date(date_array: anytype, arena: *ArenaAllocator) !FFI_Array {
    const format = comptime switch (@TypeOf(date_array)) {
        *const arr.Date32Array => "tdD",
        *const arr.Date64Array => "tdm",
        else => @compileError("unexpected array type"),
    };

    return export_primitive_impl(format, &date_array.inner, arena);
}

fn export_fixed_size_binary(array: *const arr.FixedSizeBinaryArray, arena: *ArenaAllocator) !FFI_Array {
    const allocator = arena.allocator();

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
            .private_data = arena,
            .release = release_array,
        },
        .schema = .{
            .format = format,
            .private_data = arena,
            .release = release_schema,
        },
    };
}

fn export_decimal(dec_array: anytype, arena: *ArenaAllocator) !FFI_Array {
    const width = comptime switch (@TypeOf(dec_array)) {
        *const arr.Decimal32Array => "32",
        *const arr.Decimal64Array => "64",
        *const arr.Decimal128Array => "128",
        *const arr.Decimal256Array => "256",
        else => @compileError("unexpected array type"),
    };

    const allocator = arena.allocator();

    const format = try std.fmt.allocPrintZ(allocator, "d:{},{},{s}", .{ dec_array.params.precision, dec_array.params.scale, width });

    const out = try export_primitive_impl(format, &dec_array.inner, arena);

    return out;
}

fn export_binary_view(array: *const arr.BinaryViewArray, arena: *ArenaAllocator, format: [:0]const u8) !FFI_Array {
    const n_buffers = array.buffers.len + 2;

    const allocator = arena.allocator();
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
            .private_data = arena,
            .release = release_array,
        },
        .schema = .{
            .format = format,
            .private_data = arena,
            .release = release_schema,
        },
    };
}

fn export_binary(array: anytype, arena: *ArenaAllocator, format: [:0]const u8) !FFI_Array {
    const allocator = arena.allocator();
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
            .private_data = arena,
            .release = release_array,
        },
        .schema = .{
            .format = format,
            .private_data = arena,
            .release = release_schema,
        },
    };
}

fn export_primitive_impl(format: [:0]const u8, array: anytype, arena: *ArenaAllocator) !FFI_Array {
    const allocator = arena.allocator();
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
            .private_data = arena,
            .release = release_array,
        },
        .schema = .{
            .format = format,
            .private_data = arena,
            .release = release_schema,
        },
    };
}

fn export_primitive(array: anytype, arena: *ArenaAllocator) !FFI_Array {
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

    return export_primitive_impl(format, array, arena);
}

pub const FFIError = error{
    InvalidFFIArray,
    InvalidFormatStr,
    OutOfMemory,
    InvalidCharacter,
    Overflow,
};

test "roundtrip" {
    const original_data = [_]i32{ 3, 2, 1, 4, 5, 6 };

    const export_arena = std.testing.allocator.create(std.heap.ArenaAllocator) catch unreachable;
    export_arena.* = ArenaAllocator.init(std.testing.allocator);
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
