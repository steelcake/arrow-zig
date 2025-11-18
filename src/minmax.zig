const std = @import("std");
const arr = @import("./array.zig");
const Scalar = @import("./scalar.zig").Scalar;
const bitmap = @import("./bitmap.zig");
const get = @import("./get.zig");
const testing = std.testing;
const ArenaAllocator = std.heap.ArenaAllocator;

pub const Error = error{
    ArrayTypeNotSupported,
};

pub const Op = enum {
    min,
    max,
};

pub fn check_minmax_primitive(comptime op: Op, comptime T: type, minmax_result: ?T, array: *const arr.PrimitiveArray(T)) void {
    const minmax_val = if (minmax_result) |x| x else {
        std.debug.assert(array.null_count == array.len);
        return;
    };

    if (@typeInfo(T) == .float) {
        std.debug.assert(!std.math.isNan(minmax_val));
    }

    var found = false;

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable);

        const Closure = struct {
            mm_val: T,
            found_mm: *bool,
            array: *const arr.PrimitiveArray(T),

            fn process(self: @This(), idx: u32) void {
                const v = self.array.values[idx];

                if (v == self.mm_val) {
                    self.found_mm.* = true;
                }

                switch (op) {
                    .min => std.debug.assert(self.mm_val <= v),
                    .max => std.debug.assert(self.mm_val >= v),
                }
            }
        };

        bitmap.for_each(
            Closure,
            Closure.process,
            Closure{ .mm_val = minmax_val, .found_mm = &found, .array = array },
            validity,
            array.offset,
            array.len,
        );
    } else {
        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            const v = array.values[idx];
            if (v == minmax_val) {
                found = true;
            }

            switch (op) {
                .min => std.debug.assert(minmax_val <= v),
                .max => std.debug.assert(minmax_val >= v),
            }
        }
    }

    std.debug.assert(found);
}

fn binary_minmax_impl(comptime op: Op, left: []const u8, right: []const u8) []const u8 {
    switch (op) {
        .min => return binary_min_impl(left, right),
        .max => return binary_max_impl(left, right),
    }
}

fn binary_max_impl(left: []const u8, right: []const u8) []const u8 {
    if (std.mem.order(u8, left, right) == .gt) {
        return left;
    } else {
        return right;
    }
}

fn binary_min_impl(left: []const u8, right: []const u8) []const u8 {
    if (std.mem.order(u8, left, right) == .gt) {
        return right;
    } else {
        return left;
    }
}

fn check_binary_order(comptime op: Op, minmax_val: []const u8, array_val: []const u8) void {
    const order = std.mem.order(u8, minmax_val, array_val);
    switch (op) {
        .min => std.debug.assert(order == .eq or order == .lt),
        .max => std.debug.assert(order == .eq or order == .gt),
    }
}

pub fn check_minmax_binary(comptime op: Op, comptime index_t: arr.IndexType, minmax_result: ?[]const u8, array: *const arr.GenericBinaryArray(index_t)) void {
    const minmax_val = if (minmax_result) |x| x else {
        std.debug.assert(array.null_count == array.len);
        return;
    };

    var found = false;

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable);

        const Closure = struct {
            mm_val: []const u8,
            found_mm: *bool,
            array: *const arr.GenericBinaryArray(index_t),

            fn process(self: @This(), idx: u32) void {
                const v = get.get_binary(index_t, self.array.data, self.array.offsets, idx);

                if (std.mem.eql(u8, self.mm_val, v)) {
                    self.found_mm.* = true;
                }
                check_binary_order(op, self.mm_val, v);
            }
        };

        bitmap.for_each(
            Closure,
            Closure.process,
            Closure{ .mm_val = minmax_val, .found_mm = &found, .array = array },
            validity,
            array.offset,
            array.len,
        );
    } else {
        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            const v = get.get_binary(index_t, array.data, array.offsets, idx);
            if (std.mem.eql(u8, minmax_val, v)) {
                found = true;
            }
            check_binary_order(op, minmax_val, v);
        }
    }

    std.debug.assert(found);
}

pub fn check_minmax_binary_view(comptime op: Op, minmax_result: ?[]const u8, array: *const arr.BinaryViewArray) void {
    const minmax_val = if (minmax_result) |x| x else {
        std.debug.assert(array.null_count == array.len);
        return;
    };

    var found = false;

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable);

        const Closure = struct {
            mm_val: []const u8,
            found_mm: *bool,
            array: *const arr.BinaryViewArray,

            fn process(self: @This(), idx: u32) void {
                const v = get.get_binary_view(self.array.buffers, self.array.views, idx);
                if (std.mem.eql(u8, self.mm_val, v)) {
                    self.found_mm.* = true;
                }
                check_binary_order(op, self.mm_val, v);
            }
        };

        bitmap.for_each(
            Closure,
            Closure.process,
            Closure{ .mm_val = minmax_val, .found_mm = &found, .array = array },
            validity,
            array.offset,
            array.len,
        );
    } else {
        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            const v = get.get_binary_view(array.buffers, array.views, idx);
            if (std.mem.eql(u8, minmax_val, v)) {
                found = true;
            }
            check_binary_order(op, minmax_val, v);
        }
    }

    std.debug.assert(found);
}

pub fn check_minmax_fixed_size_binary(comptime op: Op, minmax_result: ?[]const u8, array: *const arr.FixedSizeBinaryArray) void {
    const minmax_val = if (minmax_result) |x| x else {
        std.debug.assert(array.null_count == array.len);
        return;
    };

    var found = false;

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable);

        const Closure = struct {
            mm_val: []const u8,
            found_mm: *bool,
            array: *const arr.FixedSizeBinaryArray,

            fn process(self: @This(), idx: u32) void {
                const v = get.get_fixed_size_binary(self.array.data, self.array.byte_width, idx);

                if (std.mem.eql(u8, self.mm_val, v)) {
                    self.found_mm.* = true;
                }
                check_binary_order(op, self.mm_val, v);
            }
        };

        bitmap.for_each(
            Closure,
            Closure.process,
            Closure{ .mm_val = minmax_val, .found_mm = &found, .array = array },
            validity,
            array.offset,
            array.len,
        );
    } else {
        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            const v = get.get_fixed_size_binary(array.data, array.byte_width, idx);
            if (std.mem.eql(u8, minmax_val, v)) {
                found = true;
            }
            check_binary_order(op, minmax_val, v);
        }
    }

    std.debug.assert(found);
}

pub fn minmax_primitive(comptime op: Op, comptime T: type, array: *const arr.PrimitiveArray(T)) ?T {
    if (array.len == 0) {
        return null;
    }

    if (array.null_count > 0) {
        if (array.len == array.null_count) {
            return null;
        }

        const validity = (array.validity orelse unreachable);

        var m: T = switch (@typeInfo(T)) {
            .int => switch (op) {
                .max => std.math.minInt(T),
                .min => std.math.maxInt(T),
            },
            .float => switch (op) {
                .max => std.math.floatTrueMin(T),
                .min => std.math.floatMax(T),
            },
            else => @compileError("unsupported type"),
        };

        const Closure = struct {
            v: *T,
            array: *const arr.PrimitiveArray(T),

            fn process(self: @This(), idx: u32) void {
                self.v.* = switch (op) {
                    .min => @min(self.v.*, self.array.values[idx]),
                    .max => @max(self.v.*, self.array.values[idx]),
                };
            }
        };

        bitmap.for_each(
            Closure,
            Closure.process,
            Closure{ .v = &m, .array = array },
            validity,
            array.offset,
            array.len,
        );

        return m;
    } else {
        var m: T = array.values[array.offset];
        var idx: u32 = array.offset + 1;

        while (idx < array.offset + array.len) : (idx += 1) {
            switch (op) {
                .min => m = @min(m, array.values[idx]),
                .max => m = @max(m, array.values[idx]),
            }
        }

        return m;
    }
}

pub fn minmax_binary(comptime op: Op, comptime index_t: arr.IndexType, array: *const arr.GenericBinaryArray(index_t)) ?[]const u8 {
    if (array.len == 0) {
        return null;
    }

    if (array.null_count > 0) {
        if (array.len == array.null_count) {
            return null;
        }
        const validity = (array.validity orelse unreachable);

        var m: ?[]const u8 = null;

        const Closure = struct {
            v: *?[]const u8,
            array: *const arr.GenericBinaryArray(index_t),

            fn process(self: @This(), idx: u32) void {
                const s = get.get_binary(index_t, self.array.data, self.array.offsets, idx);

                if (self.v.*) |v| {
                    self.v.* = binary_minmax_impl(op, v, s);
                } else {
                    self.v.* = s;
                }
            }
        };

        bitmap.for_each(
            Closure,
            Closure.process,
            Closure{ .v = &m, .array = array },
            validity,
            array.offset,
            array.len,
        );

        return m;
    } else {
        var m: []const u8 = get.get_binary(index_t, array.data, array.offsets, array.offset);
        var idx: u32 = array.offset + 1;

        while (idx < array.offset + array.len) : (idx += 1) {
            const s = get.get_binary(index_t, array.data, array.offsets, idx);
            switch (op) {
                .min => {
                    if (std.mem.order(u8, m, s) == .gt) {
                        m = s;
                    }
                },
                .max => {
                    if (std.mem.order(u8, m, s) == .lt) {
                        m = s;
                    }
                },
            }
        }

        return m;
    }
}

pub fn minmax_binary_view(comptime op: Op, array: *const arr.BinaryViewArray) ?[]const u8 {
    if (array.len == 0) {
        return null;
    }

    if (array.null_count > 0) {
        if (array.len == array.null_count) {
            return null;
        }
        const validity = (array.validity orelse unreachable);

        var m: ?[]const u8 = null;

        const Closure = struct {
            v: *?[]const u8,
            array: *const arr.BinaryViewArray,

            fn process(self: @This(), idx: u32) void {
                const s = get.get_binary_view(self.array.buffers, self.array.views, idx);

                if (self.v.*) |v| {
                    self.v.* = binary_minmax_impl(op, v, s);
                } else {
                    self.v.* = s;
                }
            }
        };

        bitmap.for_each(
            Closure,
            Closure.process,
            Closure{ .v = &m, .array = array },
            validity,
            array.offset,
            array.len,
        );

        return m;
    } else {
        var m: []const u8 = get.get_binary_view(array.buffers, array.views, array.offset);
        var idx: u32 = array.offset + 1;

        while (idx < array.offset + array.len) : (idx += 1) {
            const s = get.get_binary_view(array.buffers, array.views, idx);
            switch (op) {
                .min => {
                    if (std.mem.order(u8, m, s) == .gt) {
                        m = s;
                    }
                },
                .max => {
                    if (std.mem.order(u8, m, s) == .lt) {
                        m = s;
                    }
                },
            }
        }

        return m;
    }
}

pub fn minmax_fixed_size_binary(comptime op: Op, array: *const arr.FixedSizeBinaryArray) ?[]const u8 {
    if (array.len == 0) {
        return null;
    }

    if (array.null_count > 0) {
        if (array.len == array.null_count) {
            return null;
        }
        const validity = (array.validity orelse unreachable);

        var m: ?[]const u8 = null;

        const Closure = struct {
            v: *?[]const u8,
            array: *const arr.FixedSizeBinaryArray,

            fn process(self: @This(), idx: u32) void {
                const s = get.get_fixed_size_binary(self.array.data, self.array.byte_width, idx);

                if (self.v.*) |v| {
                    self.v.* = binary_minmax_impl(op, v, s);
                } else {
                    self.v.* = s;
                }
            }
        };

        bitmap.for_each(
            Closure,
            Closure.process,
            Closure{ .v = &m, .array = array },
            validity,
            array.offset,
            array.len,
        );

        return m;
    } else {
        var m = get.get_fixed_size_binary(array.data, array.byte_width, array.offset);
        var idx: u32 = array.offset + 1;

        while (idx < array.offset + array.len) : (idx += 1) {
            const s = get.get_fixed_size_binary(array.data, array.byte_width, idx);
            switch (op) {
                .min => {
                    if (std.mem.order(u8, m, s) == .gt) {
                        m = s;
                    }
                },
                .max => {
                    if (std.mem.order(u8, m, s) == .lt) {
                        m = s;
                    }
                },
            }
        }

        return m;
    }
}

pub fn minmax(comptime op: Op, array: *const arr.Array) Error!?Scalar {
    switch (array.*) {
        .i8 => |*a| return if (minmax_primitive(op, i8, a)) |m| .{ .i8 = m } else null,
        .i16 => |*a| return if (minmax_primitive(op, i16, a)) |m| .{ .i16 = m } else null,
        .i32 => |*a| return if (minmax_primitive(op, i32, a)) |m| .{ .i32 = m } else null,
        .i64 => |*a| return if (minmax_primitive(op, i64, a)) |m| .{ .i64 = m } else null,
        .u8 => |*a| return if (minmax_primitive(op, u8, a)) |m| .{ .u8 = m } else null,
        .u16 => |*a| return if (minmax_primitive(op, u16, a)) |m| .{ .u16 = m } else null,
        .u32 => |*a| return if (minmax_primitive(op, u32, a)) |m| .{ .u32 = m } else null,
        .u64 => |*a| return if (minmax_primitive(op, u64, a)) |m| .{ .u64 = m } else null,
        .f16 => |*a| return if (minmax_primitive(op, f16, a)) |m| .{ .f16 = m } else null,
        .f32 => |*a| return if (minmax_primitive(op, f32, a)) |m| .{ .f32 = m } else null,
        .f64 => |*a| return if (minmax_primitive(op, f64, a)) |m| .{ .f64 = m } else null,
        .binary => |*a| return if (minmax_binary(op, .i32, a)) |m| .{ .binary = m } else null,
        .utf8 => |*a| return if (minmax_binary(op, .i32, &a.inner)) |m| .{ .binary = m } else null,
        .decimal32 => |*a| return if (minmax_primitive(op, i32, &a.inner)) |m| .{ .i32 = m } else null,
        .decimal64 => |*a| return if (minmax_primitive(op, i64, &a.inner)) |m| .{ .i64 = m } else null,
        .decimal128 => |*a| return if (minmax_primitive(op, i128, &a.inner)) |m| .{ .i128 = m } else null,
        .decimal256 => |*a| return if (minmax_primitive(op, i256, &a.inner)) |m| .{ .i256 = m } else null,
        .large_binary => |*a| return if (minmax_binary(op, .i64, a)) |m| .{ .binary = m } else null,
        .large_utf8 => |*a| return if (minmax_binary(op, .i64, &a.inner)) |m| .{ .binary = m } else null,
        .binary_view => |*a| return if (minmax_binary_view(op, a)) |m| .{ .binary = m } else null,
        .utf8_view => |*a| return if (minmax_binary_view(op, &a.inner)) |m| .{ .binary = m } else null,
        .fixed_size_binary => |*a| return if (minmax_fixed_size_binary(op, a)) |m| .{ .binary = m } else null,
        else => return Error.ArrayTypeNotSupported,
    }
}

fn unwrap_minmax_result(comptime T: type, minmax_result: ?Scalar) ?T {
    const field_name = switch (T) {
        []const u8 => "binary",
        else => @typeName(T),
    };

    if (minmax_result) |r| {
        return @field(r, field_name);
    } else {
        return null;
    }
}

pub fn check_minmax(comptime op: Op, array: *const arr.Array, minmax_result: ?Scalar) void {
    switch (array.*) {
        .i8 => |*a| check_minmax_primitive(op, i8, unwrap_minmax_result(i8, minmax_result), a),
        .i16 => |*a| check_minmax_primitive(op, i16, unwrap_minmax_result(i16, minmax_result), a),
        .i32 => |*a| check_minmax_primitive(op, i32, unwrap_minmax_result(i32, minmax_result), a),
        .i64 => |*a| check_minmax_primitive(op, i64, unwrap_minmax_result(i64, minmax_result), a),
        .u8 => |*a| check_minmax_primitive(op, u8, unwrap_minmax_result(u8, minmax_result), a),
        .u16 => |*a| check_minmax_primitive(op, u16, unwrap_minmax_result(u16, minmax_result), a),
        .u32 => |*a| check_minmax_primitive(op, u32, unwrap_minmax_result(u32, minmax_result), a),
        .u64 => |*a| check_minmax_primitive(op, u64, unwrap_minmax_result(u64, minmax_result), a),
        .f16 => |*a| check_minmax_primitive(op, f16, unwrap_minmax_result(f16, minmax_result), a),
        .f32 => |*a| check_minmax_primitive(op, f32, unwrap_minmax_result(f32, minmax_result), a),
        .f64 => |*a| check_minmax_primitive(op, f64, unwrap_minmax_result(f64, minmax_result), a),
        .binary => |*a| check_minmax_binary(op, .i32, unwrap_minmax_result([]const u8, minmax_result), a),
        .utf8 => |*a| check_minmax_binary(op, .i32, unwrap_minmax_result([]const u8, minmax_result), &a.inner),
        .decimal32 => |*a| check_minmax_primitive(op, i32, unwrap_minmax_result(i32, minmax_result), &a.inner),
        .decimal64 => |*a| check_minmax_primitive(op, i64, unwrap_minmax_result(i64, minmax_result), &a.inner),
        .decimal128 => |*a| check_minmax_primitive(op, i128, unwrap_minmax_result(i128, minmax_result), &a.inner),
        .decimal256 => |*a| check_minmax_primitive(op, i256, unwrap_minmax_result(i256, minmax_result), &a.inner),
        .large_binary => |*a| check_minmax_binary(op, .i64, unwrap_minmax_result([]const u8, minmax_result), a),
        .large_utf8 => |*a| check_minmax_binary(op, .i64, unwrap_minmax_result([]const u8, minmax_result), &a.inner),
        .binary_view => |*a| check_minmax_binary_view(op, unwrap_minmax_result([]const u8, minmax_result), a),
        .utf8_view => |*a| check_minmax_binary_view(op, unwrap_minmax_result([]const u8, minmax_result), &a.inner),
        .fixed_size_binary => |*a| check_minmax_fixed_size_binary(op, unwrap_minmax_result([]const u8, minmax_result), a),
        else => unreachable,
    }
}

pub fn min(array: *const arr.Array) Error!?Scalar {
    return minmax(.min, array);
}

pub fn max(array: *const arr.Array) Error!?Scalar {
    return minmax(.max, array);
}

pub fn check_min(array: *const arr.Array, minmax_result: ?Scalar) void {
    check_minmax(.min, array, minmax_result);
}

pub fn check_max(array: *const arr.Array, minmax_result: ?Scalar) void {
    check_minmax(.max, array, minmax_result);
}
