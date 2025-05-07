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

fn import_primitive(comptime T: type, array: *const FFI_Array) arr.PrimitiveArray(T) {
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

fn import_binary(comptime index_type: arr.IndexType, array: *const FFI_Array) arr.GenericBinaryArray(index_type) {
    const buffers = array.array.buffers orelse unreachable;

    std.debug.assert(array.array.n_buffers == 3);

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = import_validity(array.schema.flags, buffers[0], size);

    const offsets = import_buffer(index_type.to_type(), buffers[1], size + 1);

    return .{
        .data = import_buffer(u8, buffers[2], @intCast(offsets[offsets.len - 1])),
        .offsets = offsets,
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

fn import_decimal(format: []const u8, array: *const FFI_Array) arr.Array {
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
                return .{ .decimal32 = arr.Decimal32Array{ .params = params, .inner = inner } };
            } else if (std.mem.eql(u8, s, "64")) {
                const inner = import_primitive(i64, array);
                return .{ .decimal64 = arr.Decimal64Array{ .params = params, .inner = inner } };
            } else if (std.mem.eql(u8, s, "128")) {
                const inner = import_primitive(i128, array);
                return .{ .decimal128 = arr.Decimal128Array{ .params = params, .inner = inner } };
            } else if (std.mem.eql(u8, s, "256")) {
                const inner = import_primitive(i256, array);
                return .{ .decimal256 = arr.Decimal256Array{ .params = params, .inner = inner } };
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
    return .{ .decimal128 = arr.Decimal128Array{ .params = params, .inner = inner } };
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

fn import_list(comptime index_type: arr.IndexType, array: *const FFI_Array, allocator: Allocator) Error!arr.GenericListArray(index_type) {
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
    const inner = try allocator.create(arr.Array);
    inner.* = try import_array(&child, allocator);

    return .{
        .inner = inner,
        .offsets = import_buffer(index_type.to_type(), buffers[1], size + 1),
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
    };
}

fn import_list_view(comptime index_type: arr.IndexType, array: *const FFI_Array, allocator: Allocator) Error!arr.GenericListViewArray(index_type) {
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
    const inner = try allocator.create(arr.Array);
    inner.* = try import_array(&child, allocator);

    const index_t = index_type.to_type();

    return .{
        .inner = inner,
        .offsets = import_buffer(index_t, buffers[1], size),
        .sizes = import_buffer(index_t, buffers[2], size),
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
    const inner = try allocator.create(arr.Array);
    inner.* = try import_array(&child, allocator);

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
    const entries = try allocator.create(arr.StructArray);
    entries.* = (try import_array(&child, allocator)).struct_;

    return .{
        .entries = entries,
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

    const schema_children = array.schema.children orelse unreachable;
    const field_names = try allocator.alloc([:0]const u8, n_fields);
    for (0..n_fields) |i| {
        const child = schema_children[i] orelse unreachable;
        const name = child.name orelse unreachable;
        field_names[i] = std.mem.span(name);
    }

    const children = try allocator.alloc(arr.Array, n_fields);
    for (0..n_fields) |i| {
        const child = array.get_child(i);
        children[i] = try import_array(&child, allocator);
    }

    const type_ids = import_buffer(i8, buffers[0], size);

    const inner = arr.UnionArray{
        .type_id_set = type_id_set,
        .children = children,
        .type_ids = type_ids,
        .len = len,
        .offset = offset,
        .field_names = field_names,
    };

    switch (format[2]) {
        'd' => {
            std.debug.assert(array.array.n_buffers == 2);

            return .{ .dense_union = arr.DenseUnionArray{
                .inner = inner,
                .offsets = import_buffer(i32, buffers[1], size),
            } };
        },
        's' => {
            std.debug.assert(array.array.n_buffers == 1);

            return .{ .sparse_union = arr.SparseUnionArray{
                .inner = inner,
            } };
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
    const run_ends = try allocator.create(arr.Array);
    run_ends.* = try import_array(&run_ends_ffi, allocator);

    const values_ffi = array.get_child(1);
    const values = try allocator.create(arr.Array);
    values.* = try import_array(&values_ffi, allocator);

    return .{
        .run_ends = run_ends,
        .values = values,
        .len = len,
        .offset = offset,
    };
}

fn import_bool(array: *const FFI_Array) arr.BoolArray {
    const buffers = array.array.buffers orelse unreachable;

    std.debug.assert(array.array.n_buffers == 2);

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
    const keys = try allocator.create(arr.Array);
    keys.* = try import_array(array, allocator);

    const array_dict = array.array.dictionary orelse unreachable;
    const schema_dict = array.schema.dictionary orelse unreachable;
    const dict_ffi = FFI_Array{
        .array = array_dict.*,
        .schema = schema_dict.*,
    };
    const values = try allocator.create(arr.Array);
    values.* = try import_array(&dict_ffi, allocator);

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
        return .{ .dict = try import_dict(array, allocator) };
    }

    const format: []const u8 = std.mem.span(array.schema.format);
    std.debug.assert(format.len > 0);

    switch (format[0]) {
        'n' => {
            return .{ .null = import_null(array) };
        },
        'b' => {
            return .{ .bool = import_bool(array) };
        },
        'c' => {
            return .{ .i8 = import_primitive(i8, array) };
        },
        'C' => {
            return .{ .u8 = import_primitive(u8, array) };
        },
        's' => {
            return .{ .i16 = import_primitive(i16, array) };
        },
        'S' => {
            return .{ .u16 = import_primitive(u16, array) };
        },
        'i' => {
            return .{ .i32 = import_primitive(i32, array) };
        },
        'I' => {
            return .{ .u32 = import_primitive(u32, array) };
        },
        'l' => {
            return .{ .i64 = import_primitive(i64, array) };
        },
        'L' => {
            return .{ .u64 = import_primitive(u64, array) };
        },
        'e' => {
            return .{ .f16 = import_primitive(f16, array) };
        },
        'f' => {
            return .{ .f32 = import_primitive(f32, array) };
        },
        'g' => {
            return .{ .f64 = import_primitive(f64, array) };
        },
        'z' => {
            return .{ .binary = import_binary(.i32, array) };
        },
        'Z' => {
            return .{ .large_binary = import_binary(.i64, array) };
        },
        'u' => {
            return .{ .utf8 = .{ .inner = import_binary(.i32, array) } };
        },
        'U' => {
            return .{ .large_utf8 = .{ .inner = import_binary(.i64, array) } };
        },
        'v' => {
            return switch (format[1]) {
                'z' => .{ .binary_view = import_binary_view(array) },
                'u' => .{ .utf8_view = .{ .inner = import_binary_view(array) } },
                else => unreachable,
            };
        },
        'd' => {
            return import_decimal(format, array);
        },
        'w' => {
            return .{ .fixed_size_binary = import_fixed_size_binary(format, array) };
        },
        't' => {
            switch (format[1]) {
                'd' => return switch (format[2]) {
                    'D' => .{ .date32 = .{ .inner = import_primitive(i32, array) } },
                    'm' => .{ .date64 = .{ .inner = import_primitive(i64, array) } },
                    else => unreachable,
                },
                't' => return switch (format[2]) {
                    's' => .{ .time32 = arr.Time32Array{ .inner = import_primitive(i32, array), .unit = .second } },
                    'm' => .{ .time32 = arr.Time32Array{ .inner = import_primitive(i32, array), .unit = .millisecond } },
                    'u' => .{ .time64 = arr.Time64Array{ .inner = import_primitive(i64, array), .unit = .microsecond } },
                    'n' => .{ .time64 = arr.Time64Array{ .inner = import_primitive(i64, array), .unit = .nanosecond } },
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

                    return .{ .timestamp = import_timestamp(format, unit, array) };
                },
                'D' => {
                    const unit: arr.TimestampUnit = switch (format[2]) {
                        's' => .second,
                        'm' => .millisecond,
                        'u' => .microsecond,
                        'n' => .nanosecond,
                        else => unreachable,
                    };

                    return .{ .duration = arr.DurationArray{
                        .inner = import_primitive(i64, array),
                        .unit = unit,
                    } };
                },
                'i' => return switch (format[2]) {
                    'M' => .{ .interval_year_month = arr.IntervalYearMonthArray{ .inner = import_primitive(i32, array) } },
                    'D' => .{ .interval_day_time = arr.IntervalDayTimeArray{ .inner = import_primitive([2]i32, array) } },
                    'n' => .{ .interval_month_day_nano = arr.IntervalMonthDayNanoArray{ .inner = import_primitive(arr.MonthDayNano, array) } },
                    else => unreachable,
                },
                else => unreachable,
            }
        },
        '+' => {
            return switch (format[1]) {
                'l' => .{ .list = try import_list(.i32, array, allocator) },
                'L' => .{ .large_list = try import_list(.i64, array, allocator) },
                'v' => switch (format[2]) {
                    'l' => .{ .list_view = try import_list_view(.i32, array, allocator) },
                    'L' => .{ .large_list_view = try import_list_view(.i64, array, allocator) },
                    else => unreachable,
                },
                'w' => .{ .fixed_size_list = try import_fixed_size_list(format, array, allocator) },
                's' => .{ .struct_ = try import_struct(array, allocator) },
                'm' => .{ .map = try import_map(array, allocator) },
                'u' => import_union(format, array, allocator),
                'r' => .{ .run_end_encoded = try import_run_end(array, allocator) },
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
pub fn export_array(params: struct { array: *const arr.Array, arena: ArenaAllocator, ffi_arr: ?FFI_Array = null }) Error!FFI_Array {
    const private_data = try PrivateData.init(params.arena, params.ffi_arr);
    errdefer private_data.deinit();

    const out = try export_array_impl(params.array, private_data);

    return out;
}

fn export_array_impl(array: *const arr.Array, private_data: *PrivateData) Error!FFI_Array {
    switch (array.*) {
        .null => |*a| {
            return export_null(a, private_data);
        },
        .i8 => |*a| {
            return export_primitive(i8, a, "c", private_data);
        },
        .i16 => |*a| {
            return export_primitive(i16, a, "s", private_data);
        },
        .i32 => |*a| {
            return export_primitive(i32, a, "i", private_data);
        },
        .i64 => |*a| {
            return export_primitive(i64, a, "l", private_data);
        },
        .u8 => |*a| {
            return export_primitive(u8, a, "C", private_data);
        },
        .u16 => |*a| {
            return export_primitive(u16, a, "S", private_data);
        },
        .u32 => |*a| {
            return export_primitive(u32, a, "I", private_data);
        },
        .u64 => |*a| {
            return export_primitive(u64, a, "L", private_data);
        },
        .f16 => |*a| {
            return export_primitive(f16, a, "e", private_data);
        },
        .f32 => |*a| {
            return export_primitive(f32, a, "f", private_data);
        },
        .f64 => |*a| {
            return export_primitive(f64, a, "g", private_data);
        },
        .binary => |*a| {
            return export_binary(.i32, a, "z", private_data);
        },
        .large_binary => |*a| {
            return export_binary(.i64, a, "Z", private_data);
        },
        .utf8 => |*a| {
            return export_binary(.i32, &a.inner, "u", private_data);
        },
        .large_utf8 => |*a| {
            return export_binary(.i64, &a.inner, "U", private_data);
        },
        .bool => |*a| {
            return export_bool(a, private_data);
        },
        .binary_view => |*a| {
            return export_binary_view(a, "vz", private_data);
        },
        .utf8_view => |*a| {
            return export_binary_view(&a.inner, "vu", private_data);
        },
        .decimal32 => |*d_array| {
            const format = try decimal_format(d_array.params, "32", private_data.arena.allocator());
            return export_primitive(i32, &d_array.inner, format, private_data);
        },
        .decimal64 => |*d_array| {
            const format = try decimal_format(d_array.params, "64", private_data.arena.allocator());
            return export_primitive(i64, &d_array.inner, format, private_data);
        },
        .decimal128 => |*d_array| {
            const format = try decimal_format(d_array.params, "128", private_data.arena.allocator());
            return export_primitive(i128, &d_array.inner, format, private_data);
        },
        .decimal256 => |*d_array| {
            const format = try decimal_format(d_array.params, "256", private_data.arena.allocator());
            return export_primitive(i256, &d_array.inner, format, private_data);
        },
        .fixed_size_binary => |*a| {
            return export_fixed_size_binary(a, private_data);
        },
        .date32 => |*a| {
            return export_primitive(i32, &a.inner, "tdD", private_data);
        },
        .date64 => |*a| {
            return export_primitive(i64, &a.inner, "tdm", private_data);
        },
        .time32 => |*t_array| {
            const format = switch (t_array.unit) {
                .second => "tts",
                .millisecond => "ttm",
            };
            return export_primitive(i32, &t_array.inner, format, private_data);
        },
        .time64 => |*t_array| {
            const format = switch (t_array.unit) {
                .microsecond => "ttu",
                .nanosecond => "ttn",
            };
            return export_primitive(i64, &t_array.inner, format, private_data);
        },
        .timestamp => |*a| {
            return export_timestamp(a, private_data);
        },
        .duration => |*d_array| {
            const format = switch (d_array.unit) {
                .second => "tDs",
                .millisecond => "tDm",
                .microsecond => "tDu",
                .nanosecond => "tDn",
            };
            return export_primitive(i64, &d_array.inner, format, private_data);
        },
        .interval_year_month => |*a| {
            return export_primitive(i32, &a.inner, "tiM", private_data);
        },
        .interval_day_time => |*a| {
            return export_primitive([2]i32, &a.inner, "tiD", private_data);
        },
        .interval_month_day_nano => |*a| {
            return export_primitive(arr.MonthDayNano, &a.inner, "tin", private_data);
        },
        .list => |*a| {
            return export_list(.i32, a, private_data);
        },
        .large_list => |*a| {
            return export_list(.i64, a, private_data);
        },
        .list_view => |*a| {
            return export_list_view(.i32, a, private_data);
        },
        .large_list_view => |*a| {
            return export_list_view(.i64, a, private_data);
        },
        .fixed_size_list => |*a| {
            return export_fixed_size_list(a, private_data);
        },
        .struct_ => |*a| {
            return export_struct(a, private_data);
        },
        .map => |*a| {
            return export_map(a, private_data);
        },
        .dense_union => |*u_array| {
            return export_union(&u_array.inner, u_array.offsets, "+ud:", private_data);
        },
        .sparse_union => |*u_array| {
            return export_union(&u_array.inner, null, "+us:", private_data);
        },
        .run_end_encoded => |*a| {
            return export_run_end(a, private_data);
        },
        .dict => |*a| {
            return export_dict(a, private_data);
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
        var out = try export_array_impl(&array.children[i], private_data.increment());
        out.schema.name = array.field_names[i].ptr;
        children[i] = out;
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

    const entries = try allocator.create(arr.Array);
    entries.* = .{ .struct_ = array.entries.* };

    child.* = try export_array_impl(entries, private_data.increment());
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
        var out = try export_array_impl(&array.field_values[i], private_data.increment());
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

fn export_list_view(comptime index_type: arr.IndexType, array: *const arr.GenericListViewArray(index_type), private_data: *PrivateData) Error!FFI_Array {
    const n_buffers = 3;
    const n_children = 1;

    const format = comptime switch (index_type) {
        .i32 => "+vl",
        .i64 => "+vL",
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

fn export_list(comptime index_type: arr.IndexType, array: *const arr.GenericListArray(index_type), private_data: *PrivateData) Error!FFI_Array {
    const n_buffers = 2;
    const n_children = 1;

    const format = comptime switch (index_type) {
        .i32 => "+l",
        .i64 => "+L",
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

fn export_binary(comptime index_type: arr.IndexType, array: *const arr.GenericBinaryArray(index_type), format: [:0]const u8, private_data: *PrivateData) Error!FFI_Array {
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

fn export_primitive(comptime T: type, array: *const arr.PrimitiveArray(T), format: [:0]const u8, private_data: *PrivateData) Error!FFI_Array {
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

extern fn print_hello_from_rust() void;

test "QWEQWEQWEWQE" {
    print_hello_from_rust();
}
