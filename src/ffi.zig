const std = @import("std");
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const arr = @import("./array.zig");
pub const abi = @import("./abi.zig");

pub const Error = error{OutOfMemory};

pub const FFI_Array = struct {
    schema: abi.ArrowSchema,
    array: abi.ArrowArray,

    pub fn release(self: *FFI_Array) void {
        const schema_release = self.schema.release orelse unreachable;
        const array_release = self.array.release orelse unreachable;
        schema_release(&self.schema);
        array_release(&self.array);
    }

    fn get_child(array: *const FFI_Array, index: usize) FFI_Array {
        const array_children = array.array.children orelse unreachable;
        const schema_children = array.schema.children orelse unreachable;

        return .{
            .array = (array_children[index] orelse unreachable).*,
            .schema = (schema_children[index] orelse unreachable).*,
        };
    }
};

fn validity_size(size: u32) u32 {
    return (size + 7) / 8;
}

fn import_buffer(comptime T: type, buf: ?*const anyopaque, size: u32) []const T {
    const buf_ptr = buf orelse if (size == 0) {
        return &.{};
    } else {
        unreachable;
    };
    const ptr: [*]const T = @ptrCast(@alignCast(buf_ptr));
    return ptr[0..size];
}

fn import_validity(flags: abi.Flags, buf: ?*const anyopaque, size: u32) ?[]const u8 {
    if (!flags.nullable) {
        return null;
    }
    const byte_size = validity_size(size);
    if (buf) |b| {
        return import_buffer(u8, b, byte_size);
    } else {
        return &.{};
    }
}

fn import_primitive(comptime T: type, array: *const FFI_Array) arr.PrimitiveArr(T) {
    const buffers = array.array.buffers orelse unreachable;

    std.debug.assert(array.array.n_buffers == 2);

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = import_validity(array.schema.flags, buffers[0], size);

    return .{
        .values = import_buffer(T, buffers[1], size),
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
    };
}

fn import_binary(comptime IndexT: type, array: *const FFI_Array) arr.BinaryArr(IndexT) {
    const buffers = array.array.buffers orelse unreachable;

    std.debug.assert(array.array.n_buffers == 3);

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = import_validity(array.schema.flags, buffers[0], size);

    return .{
        .data = import_buffer(u8, buffers[2], size),
        .offsets = import_buffer(IndexT, buffers[1], size + 1),
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
    };
}

fn import_binary_view(array: *const FFI_Array) arr.BinaryViewArray {
    const buffers = array.array.buffers orelse unreachable;

    std.debug.assert(array.array.n_buffers > 2);

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = import_validity(array.schema.flags, buffers[0], size);

    const num_data_buffers: u32 = @intCast(array.array.n_buffers - 2);
    const data_buffers = @as([*]const [*]const u8, @ptrCast(&buffers[2]))[0..num_data_buffers];

    return .{
        .views = import_buffer(arr.BinaryView, buffers[1], size),
        .buffers = data_buffers,
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
    };
}

fn import_decimal(format: []const u8, array: *const FFI_Array, allocator: Allocator) Error!arr.Array {
    std.debug.assert(format[1] == ':');

    var precision: ?u8 = null;
    var scale: ?i8 = null;

    var it = std.mem.splitSequence(u8, format[2..], ",");
    while (it.next()) |s| {
        if (precision == null) {
            precision = std.fmt.parseInt(u8, s, 10) catch unreachable;
        } else if (scale == null) {
            scale = std.fmt.parseInt(i8, s, 10) catch unreachable;
        } else {
            std.debug.assert(it.next() == null);

            const params = arr.DecimalParams{
                .precision = precision orelse unreachable,
                .scale = scale orelse unreachable,
            };

            if (std.mem.eql(u8, s, "32")) {
                const inner = import_primitive(i32, array);
                return try arr.Array.from(.decimal32, arr.Decimal32Array{ .params = params, .inner = inner }, allocator);
            } else if (std.mem.eql(u8, s, "64")) {
                const inner = import_primitive(i64, array);
                return try arr.Array.from(.decimal64, arr.Decimal64Array{ .params = params, .inner = inner }, allocator);
            } else if (std.mem.eql(u8, s, "128")) {
                const inner = import_primitive(i128, array);
                return try arr.Array.from(.decimal128, arr.Decimal128Array{ .params = params, .inner = inner }, allocator);
            } else if (std.mem.eql(u8, s, "256")) {
                const inner = import_primitive(i256, array);
                return try arr.Array.from(.decimal256, arr.Decimal256Array{ .params = params, .inner = inner }, allocator);
            } else {
                unreachable;
            }
        }
    }

    const params = arr.DecimalParams{
        .precision = precision orelse unreachable,
        .scale = scale orelse unreachable,
    };

    const inner = import_primitive(i128, array);
    return try arr.Array.from(.decimal128, arr.Decimal128Array{ .params = params, .inner = inner }, allocator);
}

fn import_fixed_size_binary(format: []const u8, array: *const FFI_Array) arr.FixedSizeBinaryArray {
    std.debug.assert(format[1] == ':');

    const byte_width = std.fmt.parseInt(u32, format[2..], 10) catch unreachable;

    const buffers = array.array.buffers orelse unreachable;

    std.debug.assert(array.array.n_buffers == 2);

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = import_validity(array.schema.flags, buffers[0], size);

    return .{
        .data = import_buffer(u8, buffers[1], size * byte_width),
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
        .byte_width = byte_width,
    };
}

fn import_timestamp(format: []const u8, unit: arr.TimestampUnit, array: *const FFI_Array) arr.TimestampArray {
    std.debug.assert(format[3] == ':');

    const timezone = if (format.len >= 4)
        format[4..]
    else
        null;

    const inner = import_primitive(i64, array);

    return .{
        .inner = inner,
        .ts = .{
            .unit = unit,
            .timezone = timezone,
        },
    };
}

fn import_list(comptime IndexT: type, array: *const FFI_Array, allocator: Allocator) Error!arr.ListArr(IndexT) {
    const buffers = array.array.buffers orelse unreachable;

    std.debug.assert(array.array.n_buffers == 2);

    std.debug.assert(array.array.n_children == 1);
    std.debug.assert(array.schema.n_children == 1);

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = import_validity(array.schema.flags, buffers[0], size);

    const child = array.get_child(0);
    const inner = try import_array(&child, allocator);

    return .{
        .inner = inner,
        .offsets = import_buffer(IndexT, buffers[1], size + 1),
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
    };
}

fn import_list_view(comptime IndexT: type, array: *const FFI_Array, allocator: Allocator) Error!arr.ListViewArr(IndexT) {
    const buffers = array.array.buffers orelse unreachable;

    std.debug.assert(array.array.n_buffers == 3);

    std.debug.assert(array.array.n_children == 1);
    std.debug.assert(array.schema.n_children == 1);

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = import_validity(array.schema.flags, buffers[0], size);

    const child = array.get_child(0);
    const inner = try import_array(&child, allocator);

    return .{
        .inner = inner,
        .offsets = import_buffer(IndexT, buffers[1], size),
        .sizes = import_buffer(IndexT, buffers[2], size),
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
    };
}

fn import_fixed_size_list(format: []const u8, array: *const FFI_Array, allocator: Allocator) Error!arr.FixedSizeListArray {
    const buffers = array.array.buffers orelse unreachable;

    std.debug.assert(array.array.n_buffers == 1);
    std.debug.assert(array.array.n_children == 1);
    std.debug.assert(array.schema.n_children == 1);

    std.debug.assert(format[2] == ':');
    std.debug.assert(format.len > 3);

    const item_width_s = format[3..];
    const item_width = std.fmt.parseInt(i32, item_width_s, 10) catch unreachable;

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = import_validity(array.schema.flags, buffers[0], size);

    const child = array.get_child(0);
    const inner = try import_array(&child, allocator);

    return .{
        .inner = inner,
        .item_width = item_width,
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
    };
}

fn import_struct(array: *const FFI_Array, allocator: Allocator) Error!arr.StructArray {
    const buffers = array.array.buffers orelse unreachable;

    std.debug.assert(array.array.n_buffers == 1);
    std.debug.assert(array.array.n_children == array.schema.n_children);

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = import_validity(array.schema.flags, buffers[0], size);

    const n_fields: u32 = @intCast(array.array.n_children);

    const schema_children = array.schema.children orelse unreachable;
    const field_names = try allocator.alloc([:0]const u8, n_fields);
    for (0..n_fields) |i| {
        const child = schema_children[i] orelse unreachable;
        const name = child.name orelse unreachable;
        field_names[i] = std.mem.span(name);
    }

    const field_values = try allocator.alloc(arr.Array, n_fields);
    for (0..n_fields) |i| {
        const child = array.get_child(i);
        field_values[i] = try import_array(&child, allocator);
    }

    return .{
        .field_values = field_values,
        .field_names = field_names,
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
    };
}

fn import_map(array: *const FFI_Array, allocator: Allocator) Error!arr.MapArray {
    const buffers = array.array.buffers orelse unreachable;

    std.debug.assert(array.array.n_buffers == 2);
    std.debug.assert(array.array.n_children == 1);
    std.debug.assert(array.schema.n_children == 1);

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = import_validity(array.schema.flags, buffers[0], size);

    const child = array.get_child(0);
    const entries = try import_array(&child, allocator);

    return .{
        .entries = entries.to(.struct_).*,
        .validity = validity,
        .offsets = import_buffer(i32, buffers[1], size + 1),
        .len = len,
        .offset = offset,
        .null_count = null_count,
        .keys_are_sorted = array.schema.flags.map_keys_sorted,
    };
}

fn parse_union_type_id_set(format: []const u8, allocator: Allocator) Error![]const i8 {
    std.debug.assert(format[3] == ':');

    var it = std.mem.splitSequence(u8, format[3..], ",");
    const num_type_ids = if (format.len > 3) std.mem.count(u8, format[3..], ",") + 1 else 0;

    const type_id_set = try allocator.alloc(i8, num_type_ids);

    for (0..num_type_ids) |i| {
        type_id_set[i] = std.fmt.parseInt(i8, it.next().?, 10) catch unreachable;
    }
    std.debug.assert(it.next() == null);

    return type_id_set;
}

fn import_union(format: []const u8, array: *const FFI_Array, allocator: Allocator) Error!arr.Array {
    const type_id_set = try parse_union_type_id_set(format, allocator);

    const buffers = array.array.buffers orelse unreachable;

    std.debug.assert(array.array.n_children == array.schema.n_children);
    std.debug.assert(array.array.null_count == 0);

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;

    const n_fields: u32 = @intCast(array.array.n_children);
    std.debug.assert(n_fields == type_id_set.len);

    const children = try allocator.alloc(arr.Array, n_fields);
    for (0..n_fields) |i| {
        const child = array.get_child(i);
        children[i] = try import_array(&child, allocator);
    }

    const type_ids = import_buffer(i8, buffers[0], size);

    switch (format[2]) {
        'd' => {
            std.debug.assert(array.array.n_buffers == 2);

            return arr.Array.from(.dense_union, arr.DenseUnionArray{
                .inner = .{
                    .type_id_set = type_id_set,
                    .children = children,
                    .type_ids = type_ids,
                    .len = len,
                    .offset = offset,
                },
                .offsets = import_buffer(i32, buffers[1], size),
            }, allocator);
        },
        's' => {
            std.debug.assert(array.array.n_buffers == 1);

            return arr.Array.from(.sparse_union, arr.SparseUnionArray{
                .inner = .{
                    .type_id_set = type_id_set,
                    .children = children,
                    .type_ids = type_ids,
                    .len = len,
                    .offset = offset,
                },
            }, allocator);
        },
        else => unreachable,
    }
}

fn import_run_end(array: *const FFI_Array, allocator: Allocator) Error!arr.RunEndArray {
    std.debug.assert(array.array.n_buffers == 0);
    std.debug.assert(array.array.null_count == 0);

    std.debug.assert(array.array.n_children == 2);
    std.debug.assert(array.schema.n_children == 2);

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);

    const run_ends_ffi = array.get_child(0);
    const run_ends = try import_array(&run_ends_ffi, allocator);

    const values_ffi = array.get_child(1);
    const values = try import_array(&values_ffi, allocator);

    return .{
        .run_ends = run_ends,
        .values = values,
        .len = len,
        .offset = offset,
    };
}

fn import_bool(array: *const FFI_Array) arr.BoolArray {
    const buffers = array.array.buffers orelse unreachable;

    std.debug.assert(array.array.n_buffers == 0);

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = import_validity(array.schema.flags, buffers[0], size);

    return .{
        .values = import_buffer(u8, buffers[1], validity_size(size)),
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
    };
}

fn import_dict(array: *const FFI_Array, allocator: Allocator) Error!arr.DictArray {
    const keys = try import_array(array, allocator);

    const array_dict = array.array.dictionary orelse unreachable;
    const schema_dict = array.schema.dictionary orelse unreachable;
    const dict_ffi = FFI_Array{
        .array = array_dict.*,
        .schema = schema_dict.*,
    };
    const values = try import_array(&dict_ffi, allocator);

    const is_ordered = array.schema.flags.dictionary_ordered;

    return .{
        .values = values,
        .keys = keys,
        .is_ordered = is_ordered,
    };
}

fn import_null(array: *const FFI_Array) arr.NullArray {
    std.debug.assert(array.array.n_buffers == 0);

    const len: u32 = @intCast(array.array.length);

    return .{
        .len = len,
    };
}

/// Imports array from FFI, only errors if an allocation fails.
///
/// Invokes runtime checked illegal behavior otherwise (unreachable, panic etc.)
pub fn import_array(array: *const FFI_Array, allocator: Allocator) Error!arr.Array {
    if (array.array.dictionary != null) {
        return arr.Array.from(.dict, try import_dict(array, allocator), allocator);
    }

    const format: []const u8 = std.mem.span(array.schema.format);
    std.debug.assert(format.len > 0);

    switch (format[0]) {
        'n' => {
            return arr.Array.from(.null, import_null(array), allocator);
        },
        'b' => {
            return arr.Array.from(.bool, import_bool(array), allocator);
        },
        'c' => {
            return arr.Array.from(.i8, import_primitive(i8, array), allocator);
        },
        'C' => {
            return arr.Array.from(.u8, import_primitive(u8, array), allocator);
        },
        's' => {
            return arr.Array.from(.i16, import_primitive(i16, array), allocator);
        },
        'S' => {
            return arr.Array.from(.u16, import_primitive(u16, array), allocator);
        },
        'i' => {
            return arr.Array.from(.i32, import_primitive(i32, array), allocator);
        },
        'I' => {
            return arr.Array.from(.u32, import_primitive(u32, array), allocator);
        },
        'l' => {
            return arr.Array.from(.i64, import_primitive(i64, array), allocator);
        },
        'L' => {
            return arr.Array.from(.u64, import_primitive(u64, array), allocator);
        },
        'e' => {
            return arr.Array.from(.f16, import_primitive(f16, array), allocator);
        },
        'f' => {
            return arr.Array.from(.f32, import_primitive(f32, array), allocator);
        },
        'g' => {
            return arr.Array.from(.f64, import_primitive(f64, array), allocator);
        },
        'z' => {
            return arr.Array.from(.binary, import_binary(i32, array), allocator);
        },
        'Z' => {
            return arr.Array.from(.large_binary, import_binary(i64, array), allocator);
        },
        'u' => {
            return arr.Array.from(.utf8, arr.Utf8Array{
                .inner = import_binary(i32, array),
            }, allocator);
        },
        'U' => {
            return arr.Array.from(.large_utf8, arr.LargeUtf8Array{
                .inner = import_binary(i64, array),
            }, allocator);
        },
        'v' => {
            return switch (format[1]) {
                'z' => arr.Array.from(.binary_view, import_binary_view(array), allocator),
                'u' => arr.Array.from(.utf8_view, arr.Utf8ViewArray{ .inner = import_binary_view(array) }, allocator),
                else => unreachable,
            };
        },
        'd' => {
            return import_decimal(format, array, allocator);
        },
        'w' => {
            return arr.Array.from(.fixed_size_binary, import_fixed_size_binary(format, array), allocator);
        },
        't' => {
            switch (format[1]) {
                'd' => return switch (format[2]) {
                    'D' => arr.Array.from(.date32, arr.Date32Array{ .inner = import_primitive(i32, array) }, allocator),
                    'm' => arr.Array.from(.date64, arr.Date64Array{ .inner = import_primitive(i64, array) }, allocator),
                    else => unreachable,
                },
                't' => return switch (format[2]) {
                    's' => arr.Array.from(.time32, arr.Time32Array{ .inner = import_primitive(i32, array), .unit = .second }, allocator),
                    'm' => arr.Array.from(.time32, arr.Time32Array{ .inner = import_primitive(i32, array), .unit = .millisecond }, allocator),
                    'u' => arr.Array.from(.time64, arr.Time64Array{ .inner = import_primitive(i64, array), .unit = .microsecond }, allocator),
                    'n' => arr.Array.from(.time64, arr.Time64Array{ .inner = import_primitive(i64, array), .unit = .nanosecond }, allocator),
                    else => unreachable,
                },
                's' => {
                    const unit: arr.TimestampUnit = switch (format[2]) {
                        's' => .second,
                        'm' => .millisecond,
                        'u' => .microsecond,
                        'n' => .nanosecond,
                        else => unreachable,
                    };

                    return arr.Array.from(.timestamp, import_timestamp(format, unit, array), allocator);
                },
                'D' => {
                    const unit: arr.TimestampUnit = switch (format[2]) {
                        's' => .second,
                        'm' => .millisecond,
                        'u' => .microsecond,
                        'n' => .nanosecond,
                        else => unreachable,
                    };

                    return arr.Array.from(.duration, arr.DurationArray{
                        .inner = import_primitive(i64, array),
                        .unit = unit,
                    }, allocator);
                },
                'i' => return switch (format[2]) {
                    'M' => arr.Array.from(.interval_year_month, arr.IntervalYearMonthArray{ .inner = import_primitive(i32, array) }, allocator),
                    'D' => arr.Array.from(.interval_day_time, arr.IntervalDayTimeArray{ .inner = import_primitive([2]i32, array) }, allocator),
                    'n' => arr.Array.from(.interval_month_day_nano, arr.IntervalMonthDayNanoArray{ .inner = import_primitive(arr.MonthDayNano, array) }, allocator),
                    else => unreachable,
                },
                else => unreachable,
            }
        },
        '+' => {
            return switch (format[1]) {
                'l' => arr.Array.from(.list, try import_list(i32, array, allocator), allocator),
                'L' => arr.Array.from(.large_list, try import_list(i64, array, allocator), allocator),
                'v' => switch (format[2]) {
                    'l' => arr.Array.from(.list_view, try import_list_view(i32, array, allocator), allocator),
                    'L' => arr.Array.from(.large_list_view, try import_list_view(i64, array, allocator), allocator),
                    else => unreachable,
                },
                'w' => arr.Array.from(.fixed_size_list, try import_fixed_size_list(format, array, allocator), allocator),
                's' => arr.Array.from(.struct_, try import_struct(array, allocator), allocator),
                'm' => arr.Array.from(.map, try import_map(array, allocator), allocator),
                'u' => import_union(format, array, allocator),
                'r' => arr.Array.from(.run_end_encoded, try import_run_end(array, allocator), allocator),
                else => unreachable,
            };
        },
        else => unreachable,
    }
}

const PrivateData = struct {
    ref_count: std.atomic.Value(i32),
    arena: ArenaAllocator,
    ffi: ?FFI_Array,

    fn init(arena: ArenaAllocator, ffi: ?FFI_Array) !*PrivateData {
        const self = try arena.child_allocator.create(PrivateData);
        self.* = .{
            .arena = arena,
            .ref_count = std.atomic.Value(i32).init(1),
            .ffi = ffi,
        };
        return self;
    }

    fn deinit(self: *PrivateData) void {
        const backing_alloc = self.arena.child_allocator;
        self.arena.deinit();
        if (self.ffi) |*ffi| {
            ffi.release();
        }

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

fn release_impl(comptime T: type, data: ?*T) void {
    const ptr = data orelse return;
    const obj = ptr.*;

    const n_children: usize = @intCast(obj.n_children);

    for (0..n_children) |i| {
        const child = obj.children.?[i].?;
        if (child.*.release) |r| {
            r(child);
            std.debug.assert(child.*.release == null);
        }
    }

    if (obj.dictionary) |dict| {
        if (dict.*.release) |r| {
            r(obj.dictionary.?);
            std.debug.assert(dict.*.release == null);
        }
    }

    const arc: *PrivateData = @ptrCast(@alignCast(ptr.*.private_data));
    arc.decrement();

    ptr.*.release = null;
}

fn release_array(array: ?*abi.ArrowArray) callconv(.C) void {
    release_impl(abi.ArrowArray, array);
}

fn release_schema(schema: ?*abi.ArrowSchema) callconv(.C) void {
    release_impl(abi.ArrowSchema, schema);
}

/// Arena should be initialized using a thread-safe and static lifetime allocator like std.heap.GeneralPurposeAlloc.
/// and all data related to the array should be allocated using this arena.
///
/// Ownership of the arena is transferred to the FFI_Array from this point (even if the function fails) so the caller should not use the arena including calling deinit.
///
/// If the caller is passing array that is imported from ffi, they should pass the associated FFI_Array into this function.
/// So when the consumer calls `release` it is propogated to this ffi array and everything is cleaned up properly.
///
/// All allocations are done using the passed arena allocator.
///
/// Errors only if an allocation fails.
pub fn export_array(params: struct { array: arr.Array, arena: ArenaAllocator, ffi_arr: ?FFI_Array = null }) Error!FFI_Array {
    const private_data = try PrivateData.init(params.arena, params.ffi_arr);
    errdefer private_data.deinit();

    const out = try export_array_impl(params.array, private_data);

    return out;
}

fn export_array_impl(array: arr.Array, private_data: *PrivateData) Error!FFI_Array {
    switch (array.type_) {
        .null => {
            return export_null(array.to(.null), private_data);
        },
        .i8 => {
            return export_primitive(i8, array.to(.i8), "c", private_data);
        },
        .i16 => {
            return export_primitive(i16, array.to(.i16), "s", private_data);
        },
        .i32 => {
            return export_primitive(i32, array.to(.i32), "i", private_data);
        },
        .i64 => {
            return export_primitive(i64, array.to(.i64), "l", private_data);
        },
        .u8 => {
            return export_primitive(u8, array.to(.u8), "C", private_data);
        },
        .u16 => {
            return export_primitive(u16, array.to(.u16), "S", private_data);
        },
        .u32 => {
            return export_primitive(u32, array.to(.u32), "I", private_data);
        },
        .u64 => {
            return export_primitive(u64, array.to(.u64), "L", private_data);
        },
        .f16 => {
            return export_primitive(f16, array.to(.f16), "e", private_data);
        },
        .f32 => {
            return export_primitive(f32, array.to(.f32), "f", private_data);
        },
        .f64 => {
            return export_primitive(f64, array.to(.f64), "g", private_data);
        },
        .binary => {
            return export_binary(i32, array.to(.binary), "z", private_data);
        },
        .large_binary => {
            return export_binary(i64, array.to(.large_binary), "Z", private_data);
        },
        .utf8 => {
            return export_binary(i32, &array.to(.utf8).inner, "u", private_data);
        },
        .large_utf8 => {
            return export_binary(i64, &array.to(.large_utf8).inner, "U", private_data);
        },
        .bool => {
            return export_bool(array.to(.bool), private_data);
        },
        .binary_view => {
            return export_binary_view(array.to(.binary_view), "vz", private_data);
        },
        .utf8_view => {
            return export_binary_view(&array.to(.utf8_view).inner, "vu", private_data);
        },
        .decimal32 => {
            const d_array = array.to(.decimal32);
            const format = try decimal_format(d_array.params, "32", private_data.arena.allocator());
            return export_primitive(i32, &d_array.inner, format, private_data);
        },
        .decimal64 => {
            const d_array = array.to(.decimal64);
            const format = try decimal_format(d_array.params, "64", private_data.arena.allocator());
            return export_primitive(i64, &d_array.inner, format, private_data);
        },
        .decimal128 => {
            const d_array = array.to(.decimal128);
            const format = try decimal_format(d_array.params, "128", private_data.arena.allocator());
            return export_primitive(i128, &d_array.inner, format, private_data);
        },
        .decimal256 => {
            const d_array = array.to(.decimal256);
            const format = try decimal_format(d_array.params, "256", private_data.arena.allocator());
            return export_primitive(i256, &d_array.inner, format, private_data);
        },
        .fixed_size_binary => {
            return export_fixed_size_binary(array.to(.fixed_size_binary), private_data);
        },
        .date32 => {
            return export_primitive(i32, &array.to(.date32).inner, "tdD", private_data);
        },
        .date64 => {
            return export_primitive(i64, &array.to(.date64).inner, "tdm", private_data);
        },
        .time32 => {
            const t_array = array.to(.time32);
            const format = switch (t_array.unit) {
                .second => "tts",
                .millisecond => "ttm",
            };
            return export_primitive(i32, &t_array.inner, format, private_data);
        },
        .time64 => {
            const t_array = array.to(.time64);
            const format = switch (t_array.unit) {
                .microsecond => "ttu",
                .nanosecond => "ttn",
            };
            return export_primitive(i64, &t_array.inner, format, private_data);
        },
        .timestamp => {
            return export_timestamp(array.to(.timestamp), private_data);
        },
        .duration => {
            const d_array = array.to(.duration);
            const format = switch (d_array.unit) {
                .second => "tDs",
                .millisecond => "tDm",
                .microsecond => "tDu",
                .nanosecond => "tDn",
            };
            return export_primitive(i64, &d_array.inner, format, private_data);
        },
        .interval_year_month => {
            return export_primitive(i32, &array.to(.interval_year_month).inner, "tiM", private_data);
        },
        .interval_day_time => {
            return export_primitive([2]i32, &array.to(.interval_day_time).inner, "tiD", private_data);
        },
        .interval_month_day_nano => {
            return export_primitive(arr.MonthDayNano, &array.to(.interval_month_day_nano).inner, "tin", private_data);
        },
        .list => {
            return export_list(i32, array.to(.list), private_data);
        },
        .large_list => {
            return export_list(i64, array.to(.large_list), private_data);
        },
        .list_view => {
            return export_list_view(i32, array.to(.list_view), private_data);
        },
        .large_list_view => {
            return export_list_view(i64, array.to(.large_list_view), private_data);
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
            const u_array = array.to(.dense_union);
            return export_union(&u_array.inner, u_array.offsets, "+ud:", private_data);
        },
        .sparse_union => {
            const u_array = array.to(.sparse_union);
            return export_union(&u_array.inner, null, "+us:", private_data);
        },
        .run_end_encoded => {
            return export_run_end(array.to(.run_end_encoded), private_data);
        },
        .dict => {
            return export_dict(array.to(.dict), private_data);
        },
    }
}

fn export_null(array: *const arr.NullArray, private_data: *PrivateData) FFI_Array {
    return .{
        .schema = .{
            .format = "n",
            .name = null,
            .metadata = null,
            .flags = .{},
            .n_children = 0,
            .children = null,
            .dictionary = null,
            .release = release_schema,
            .private_data = private_data.increment(),
        },
        .array = .{
            .length = array.len,
            .null_count = 0,
            .offset = 0,
            .n_buffers = 0,
            .n_children = 0,
            .buffers = null,
            .children = null,
            .dictionary = null,
            .release = release_array,
            .private_data = private_data,
        },
    };
}

fn export_dict(array: *const arr.DictArray, private_data: *PrivateData) Error!FFI_Array {
    var out = try export_array_impl(array.keys, private_data.increment());

    const allocator = private_data.arena.allocator();
    const dict_ptr = try allocator.create(FFI_Array);
    dict_ptr.* = try export_array_impl(array.values, private_data);
    out.array.dictionary = &dict_ptr.array;
    out.schema.dictionary = &dict_ptr.schema;
    out.schema.flags.dictionary_ordered = array.is_ordered;

    return out;
}

fn export_run_end(array: *const arr.RunEndArray, private_data: *PrivateData) Error!FFI_Array {
    const n_children = 2;
    const n_buffers = 0;

    const allocator = private_data.arena.allocator();

    const children = try allocator.alloc(FFI_Array, 2);
    children[0] = try export_array_impl(array.run_ends, private_data.increment());
    children[0].schema.name = "run_ends";
    children[1] = try export_array_impl(array.values, private_data.increment());
    children[1].schema.name = "values";

    const array_children = try allocator.alloc([*c]abi.ArrowArray, n_children);
    array_children[0] = &children[0].array;
    array_children[1] = &children[1].array;

    const schema_children = try allocator.alloc([*c]abi.ArrowSchema, n_children);
    schema_children[0] = &children[0].schema;
    schema_children[1] = &children[1].schema;

    return .{
        .schema = .{
            .format = "+r",
            .name = null,
            .metadata = null,
            .flags = .{},
            .n_children = n_children,
            .children = schema_children.ptr,
            .dictionary = null,
            .release = release_schema,
            .private_data = private_data.increment(),
        },
        .array = .{
            .length = array.len,
            .null_count = 0,
            .offset = array.offset,
            .n_buffers = n_buffers,
            .n_children = n_children,
            .buffers = null,
            .children = array_children.ptr,
            .dictionary = null,
            .release = release_array,
            .private_data = private_data,
        },
    };
}

fn union_format(base: []const u8, type_id_set: []const i8, allocator: Allocator) Error![:0]const u8 {
    std.debug.assert(base.len == 4);

    // size is calculated as base + 5  * num_type_ids + 1 because type ids are 8 bit integers so they can't
    // occupy more than 3 digits, a minus sign and a comma.
    // and need the last one byte to make it zero terminated.
    const format = try allocator.alloc(u8, 4 + 5 * type_id_set.len + 1);

    @memcpy(format[0..4], base);

    var write_idx: usize = 4;

    if (type_id_set.len > 0) {
        {
            const out = std.fmt.bufPrint(format[write_idx..], "{}", .{type_id_set[0]}) catch unreachable;
            write_idx += out.len;
        }
        for (1..type_id_set.len) |i| {
            const out = std.fmt.bufPrint(format[write_idx..], ",{}", .{type_id_set[i]}) catch unreachable;
            write_idx += out.len;
        }
    }

    format[write_idx] = 0;

    return @ptrCast(format[0..write_idx]);
}

fn export_union(array: *const arr.UnionArr, offsets: ?[]const i32, format_base: []const u8, private_data: *PrivateData) Error!FFI_Array {
    const n_fields = array.children.len;
    const n_children: i64 = @intCast(n_fields);
    const n_buffers: u32 = if (offsets != null) 2 else 1;

    const allocator = private_data.arena.allocator();

    const buffers = try allocator.alloc(?*const anyopaque, n_buffers);
    buffers[0] = array.type_ids.ptr;
    if (offsets) |o| {
        buffers[1] = o.ptr;
    }

    const children = try allocator.alloc(FFI_Array, n_fields);
    for (0..n_fields) |i| {
        children[i] = try export_array_impl(array.children[i], private_data.increment());
    }

    const array_children = try allocator.alloc(?*abi.ArrowArray, n_fields);
    for (0..n_fields) |i| {
        array_children[i] = &children[i].array;
    }

    const schema_children = try allocator.alloc(?*abi.ArrowSchema, n_fields);
    for (0..n_fields) |i| {
        schema_children[i] = &children[i].schema;
    }

    const format = try union_format(format_base, array.type_id_set, allocator);

    return .{
        .schema = .{
            .format = format,
            .name = null,
            .metadata = null,
            .flags = .{},
            .n_children = n_children,
            .children = schema_children.ptr,
            .dictionary = null,
            .release = release_schema,
            .private_data = private_data.increment(),
        },
        .array = .{
            .length = array.len,
            .null_count = 0,
            .offset = array.offset,
            .n_buffers = n_buffers,
            .n_children = n_children,
            .buffers = buffers.ptr,
            .children = array_children.ptr,
            .dictionary = null,
            .release = release_array,
            .private_data = private_data,
        },
    };
}

fn export_map(array: *const arr.MapArray, private_data: *PrivateData) Error!FFI_Array {
    const n_buffers = 2;
    const n_children = 1;

    const allocator = private_data.arena.allocator();

    const buffers = try allocator.alloc(?*const anyopaque, n_buffers);
    buffers[0] = if (array.validity) |v| v.ptr else null;
    buffers[1] = array.offsets.ptr;

    const child = try allocator.create(FFI_Array);
    child.* = try export_array_impl(arr.Array.from_ptr(.struct_, &array.entries), private_data.increment());
    child.schema.name = "entries";

    const array_children = try allocator.alloc([*c]abi.ArrowArray, n_children);
    array_children[0] = &child.array;

    const schema_children = try allocator.alloc([*c]abi.ArrowSchema, n_children);
    schema_children[0] = &child.schema;

    return .{
        .schema = .{
            .format = "+m",
            .name = null,
            .metadata = null,
            .flags = .{ .nullable = array.validity != null, .map_keys_sorted = array.keys_are_sorted },
            .n_children = n_children,
            .children = schema_children.ptr,
            .dictionary = null,
            .release = release_schema,
            .private_data = private_data.increment(),
        },
        .array = .{
            .length = array.len,
            .null_count = array.null_count,
            .offset = array.offset,
            .n_buffers = n_buffers,
            .n_children = n_children,
            .buffers = buffers.ptr,
            .children = array_children.ptr,
            .dictionary = null,
            .release = release_array,
            .private_data = private_data,
        },
    };
}

fn export_struct(array: *const arr.StructArray, private_data: *PrivateData) Error!FFI_Array {
    const n_fields = array.field_values.len;
    const n_buffers = 1;
    const n_children: i64 = @intCast(n_fields);

    const allocator = private_data.arena.allocator();

    const buffers = try allocator.alloc(?*const anyopaque, n_buffers);
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
        .schema = .{
            .format = "+s",
            .name = null,
            .metadata = null,
            .flags = .{ .nullable = array.validity != null },
            .n_children = n_children,
            .children = schema_children.ptr,
            .dictionary = null,
            .release = release_schema,
            .private_data = private_data.increment(),
        },
        .array = .{
            .length = array.len,
            .null_count = array.null_count,
            .offset = array.offset,
            .n_buffers = n_buffers,
            .n_children = n_children,
            .buffers = buffers.ptr,
            .children = array_children.ptr,
            .dictionary = null,
            .release = release_array,
            .private_data = private_data,
        },
    };
}

fn export_fixed_size_list(array: *const arr.FixedSizeListArray, private_data: *PrivateData) Error!FFI_Array {
    const n_buffers = 1;
    const n_children = 1;

    const allocator = private_data.arena.allocator();

    const format = try std.fmt.allocPrintZ(allocator, "+w:{}", .{array.item_width});

    const buffers = try allocator.alloc(?*const anyopaque, n_buffers);
    buffers[0] = if (array.validity) |v| v.ptr else null;

    const child = try allocator.create(FFI_Array);
    child.* = try export_array_impl(array.inner, private_data.increment());

    const array_children = try allocator.alloc([*c]abi.ArrowArray, n_children);
    array_children[0] = &child.array;

    const schema_children = try allocator.alloc([*c]abi.ArrowSchema, n_children);
    schema_children[0] = &child.schema;

    return .{
        .schema = .{
            .format = format,
            .name = null,
            .metadata = null,
            .flags = .{ .nullable = array.validity != null },
            .n_children = n_children,
            .children = schema_children.ptr,
            .dictionary = null,
            .release = release_schema,
            .private_data = private_data.increment(),
        },
        .array = .{
            .length = array.len,
            .null_count = array.null_count,
            .offset = array.offset,
            .n_buffers = n_buffers,
            .n_children = n_children,
            .buffers = buffers.ptr,
            .children = array_children.ptr,
            .dictionary = null,
            .release = release_array,
            .private_data = private_data,
        },
    };
}

fn export_list_view(comptime IndexT: type, array: *const arr.ListViewArr(IndexT), private_data: *PrivateData) Error!FFI_Array {
    const n_buffers = 3;
    const n_children = 1;

    const format = comptime switch (IndexT) {
        i32 => "+vl",
        i64 => "+vL",
        else => @compileError("unsupported index type"),
    };

    const allocator = private_data.arena.allocator();
    const buffers = try allocator.alloc(?*const anyopaque, n_buffers);
    buffers[0] = if (array.validity) |v| v.ptr else null;
    buffers[1] = array.offsets.ptr;
    buffers[2] = array.sizes.ptr;

    const child = try allocator.create(FFI_Array);
    child.* = try export_array_impl(array.inner, private_data.increment());

    const array_children = try allocator.alloc(?*abi.ArrowArray, n_children);
    array_children[0] = &child.array;

    const schema_children = try allocator.alloc(?*abi.ArrowSchema, n_children);
    schema_children[0] = &child.schema;

    return .{
        .schema = .{
            .format = format,
            .name = null,
            .metadata = null,
            .flags = .{ .nullable = array.validity != null },
            .n_children = n_children,
            .children = schema_children.ptr,
            .dictionary = null,
            .release = release_schema,
            .private_data = private_data.increment(),
        },
        .array = .{
            .length = array.len,
            .null_count = array.null_count,
            .offset = array.offset,
            .n_buffers = n_buffers,
            .n_children = n_children,
            .buffers = buffers.ptr,
            .children = array_children.ptr,
            .dictionary = null,
            .release = release_array,
            .private_data = private_data,
        },
    };
}

fn export_list(comptime IndexT: type, array: *const arr.ListArr(IndexT), private_data: *PrivateData) Error!FFI_Array {
    const n_buffers = 2;
    const n_children = 1;

    const format = comptime switch (IndexT) {
        i32 => "+l",
        i64 => "+L",
        else => @compileError("unsupported index type"),
    };

    const allocator = private_data.arena.allocator();
    const buffers = try allocator.alloc(?*const anyopaque, n_buffers);
    buffers[0] = if (array.validity) |v| v.ptr else null;
    buffers[1] = array.offsets.ptr;

    const child = try allocator.create(FFI_Array);
    child.* = try export_array_impl(array.inner, private_data.increment());

    const array_children = try allocator.alloc([*c]abi.ArrowArray, n_children);
    array_children[0] = &child.array;

    const schema_children = try allocator.alloc([*c]abi.ArrowSchema, n_children);
    schema_children[0] = &child.schema;

    return .{
        .schema = .{
            .format = format,
            .name = null,
            .metadata = null,
            .flags = .{ .nullable = array.validity != null },
            .n_children = n_children,
            .children = schema_children.ptr,
            .dictionary = null,
            .release = release_schema,
            .private_data = private_data.increment(),
        },
        .array = .{
            .length = array.len,
            .null_count = array.null_count,
            .offset = array.offset,
            .n_buffers = n_buffers,
            .n_children = n_children,
            .buffers = buffers.ptr,
            .children = array_children.ptr,
            .dictionary = null,
            .release = release_array,
            .private_data = private_data,
        },
    };
}

fn export_timestamp(timestamp_array: *const arr.TimestampArray, private_data: *PrivateData) Error!FFI_Array {
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

    return export_primitive(i64, &timestamp_array.inner, format, private_data);
}

fn export_fixed_size_binary(array: *const arr.FixedSizeBinaryArray, private_data: *PrivateData) Error!FFI_Array {
    const n_buffers = 2;

    const allocator = private_data.arena.allocator();

    const format = try std.fmt.allocPrintZ(allocator, "w:{}", .{array.byte_width});

    const buffers = try allocator.alloc(?*const anyopaque, n_buffers);
    buffers[0] = if (array.validity) |v| v.ptr else null;
    buffers[1] = array.data.ptr;

    return .{
        .schema = .{
            .format = format,
            .name = null,
            .metadata = null,
            .flags = .{ .nullable = array.validity != null },
            .n_children = 0,
            .children = null,
            .dictionary = null,
            .release = release_schema,
            .private_data = private_data.increment(),
        },
        .array = .{
            .length = array.len,
            .null_count = array.null_count,
            .offset = array.offset,
            .n_buffers = n_buffers,
            .n_children = 0,
            .buffers = buffers.ptr,
            .children = null,
            .dictionary = null,
            .release = release_array,
            .private_data = private_data,
        },
    };
}

fn decimal_format(params: arr.DecimalParams, width: []const u8, allocator: Allocator) Error![:0]const u8 {
    return std.fmt.allocPrintZ(allocator, "d:{},{},{s}", .{ params.precision, params.scale, width });
}

fn export_binary_view(array: *const arr.BinaryViewArray, format: [:0]const u8, private_data: *PrivateData) Error!FFI_Array {
    const n_buffers = array.buffers.len + 2;

    const allocator = private_data.arena.allocator();
    const buffers = try allocator.alloc(?*const anyopaque, n_buffers);
    buffers[0] = if (array.validity) |v| v.ptr else null;
    buffers[1] = array.views.ptr;

    for (array.buffers, 0..) |b, i| {
        buffers[i + 2] = b;
    }

    return .{
        .schema = .{
            .format = format,
            .name = null,
            .metadata = null,
            .flags = .{ .nullable = array.validity != null },
            .n_children = 0,
            .children = null,
            .dictionary = null,
            .release = release_schema,
            .private_data = private_data.increment(),
        },
        .array = .{
            .length = array.len,
            .null_count = array.null_count,
            .offset = array.offset,
            .n_buffers = @as(i64, @intCast(n_buffers)),
            .n_children = 0,
            .buffers = buffers.ptr,
            .children = null,
            .dictionary = null,
            .release = release_array,
            .private_data = private_data,
        },
    };
}

fn export_binary(comptime IndexT: type, array: *const arr.BinaryArr(IndexT), format: [:0]const u8, private_data: *PrivateData) Error!FFI_Array {
    const n_buffers = 3;

    const allocator = private_data.arena.allocator();
    const buffers = try allocator.alloc(?*const anyopaque, n_buffers);
    buffers[0] = if (array.validity) |v| v.ptr else null;
    buffers[1] = array.offsets.ptr;
    buffers[2] = array.data.ptr;

    return .{
        .schema = .{
            .format = format,
            .name = null,
            .metadata = null,
            .flags = .{ .nullable = array.validity != null },
            .n_children = 0,
            .children = null,
            .dictionary = null,
            .release = release_schema,
            .private_data = private_data.increment(),
        },
        .array = .{
            .length = array.len,
            .null_count = array.null_count,
            .offset = array.offset,
            .n_buffers = n_buffers,
            .n_children = 0,
            .buffers = buffers.ptr,
            .children = null,
            .dictionary = null,
            .release = release_array,
            .private_data = private_data,
        },
    };
}

fn export_bool(array: *const arr.BoolArray, private_data: *PrivateData) Error!FFI_Array {
    const n_buffers = 2;

    const allocator = private_data.arena.allocator();
    const buffers = try allocator.alloc(?*const anyopaque, n_buffers);
    buffers[0] = if (array.validity) |v| v.ptr else null;
    buffers[1] = array.values.ptr;

    return .{
        .schema = .{
            .format = "b",
            .name = null,
            .metadata = null,
            .flags = .{ .nullable = array.validity != null },
            .n_children = 0,
            .children = null,
            .dictionary = null,
            .release = release_schema,
            .private_data = private_data.increment(),
        },
        .array = .{
            .length = array.len,
            .null_count = array.null_count,
            .offset = array.offset,
            .n_buffers = n_buffers,
            .n_children = 0,
            .buffers = buffers.ptr,
            .children = null,
            .dictionary = null,
            .release = release_array,
            .private_data = private_data,
        },
    };
}

fn export_primitive(comptime T: type, array: *const arr.PrimitiveArr(T), format: [:0]const u8, private_data: *PrivateData) Error!FFI_Array {
    const n_buffers = 2;

    const allocator = private_data.arena.allocator();
    const buffers = try allocator.alloc(?*const anyopaque, n_buffers);
    buffers[0] = if (array.validity) |v| v.ptr else null;
    buffers[1] = array.values.ptr;

    return .{
        .schema = .{
            .format = format,
            .name = null,
            .metadata = null,
            .flags = .{ .nullable = array.validity != null },
            .n_children = 0,
            .children = null,
            .dictionary = null,
            .release = release_schema,
            .private_data = private_data.increment(),
        },
        .array = .{
            .length = array.len,
            .null_count = array.null_count,
            .offset = array.offset,
            .n_buffers = n_buffers,
            .n_children = 0,
            .buffers = buffers.ptr,
            .children = null,
            .dictionary = null,
            .release = release_array,
            .private_data = private_data,
        },
    };
}

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

    const array = arr.Array.from_ptr(.i32, typed);

    // use 'catch unreachable' up to here beacuse we don't want everything to leak
    // and just using defer isn't feasible because all of that ownership is handed to the consumer
    // of FFI_Array
    //
    // would have to handle this in a more complete way in a real application.
    var ffi_array = export_array(.{ .array = array, .arena = export_arena }) catch unreachable;
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
