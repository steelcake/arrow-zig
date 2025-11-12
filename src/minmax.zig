const std = @import("std");
const arr = @import("./array.zig");
const Scalar = @import("./scalar.zig").Scalar;
const bitmap = @import("./bitmap.zig");
const get = @import("./get.zig");
const testing = std.testing;
const ArenaAllocator = std.heap.ArenaAllocator;

const Error = error{
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

    var found = false;

    if (array.null_count > 0) {
        const validity = (array.validity orelse unreachable).ptr;

        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            if (get.get_primitive_opt(T, array.values.ptr, validity, idx)) |v| {
                if (v == minmax_val) {
                    found = true;
                }

                switch (op) {
                    .min => std.debug.assert(minmax_val <= v),
                    .max => std.debug.assert(minmax_val >= v),
                }
            }
        }
    } else {
        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            const v = array.values.ptr[idx];
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
        const validity = (array.validity orelse unreachable).ptr;

        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            if (get.get_binary_opt(index_t, array.data.ptr, array.offsets.ptr, validity, idx)) |v| {
                if (std.mem.eql(u8, minmax_val, v)) {
                    found = true;
                }
                check_binary_order(op, minmax_val, v);
            }
        }
    } else {
        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            const v = get.get_binary(index_t, array.data.ptr, array.offsets.ptr, idx);
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
        const validity = (array.validity orelse unreachable).ptr;

        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            if (get.get_binary_view_opt(array.buffers.ptr, array.views.ptr, validity, idx)) |v| {
                if (std.mem.eql(u8, minmax_val, v)) {
                    found = true;
                }
                check_binary_order(op, minmax_val, v);
            }
        }
    } else {
        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            const v = get.get_binary_view(array.buffers.ptr, array.views.ptr, idx);
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
        const validity = (array.validity orelse unreachable).ptr;

        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            if (get.get_fixed_size_binary_opt(array.data.ptr, array.byte_width, validity, idx)) |v| {
                if (std.mem.eql(u8, minmax_val, v)) {
                    found = true;
                }
                check_binary_order(op, minmax_val, v);
            }
        }
    } else {
        var idx: u32 = array.offset;
        while (idx < array.offset + array.len) : (idx += 1) {
            const v = get.get_fixed_size_binary(array.data.ptr, array.byte_width, idx);
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
        const validity = (array.validity orelse unreachable).ptr;

        var m: T = undefined;
        var idx: u32 = array.offset;

        // find the first value
        while (idx < array.offset + array.len) : (idx += 1) {
            if (bitmap.get(validity, idx)) {
                m = array.values.ptr[idx];
                idx += 1;
                break;
            }
        } else {
            return null;
        }

        while (idx < array.offset + array.len) : (idx += 1) {
            if (bitmap.get(validity, idx)) {
                switch (op) {
                    .min => m = @min(m, array.values.ptr[idx]),
                    .max => m = @max(m, array.values.ptr[idx]),
                }
            }
        }

        return m;
    } else {
        var m: T = array.values.ptr[array.offset];
        var idx: u32 = array.offset + 1;

        while (idx < array.offset + array.len) : (idx += 1) {
            switch (op) {
                .min => m = @min(m, array.values.ptr[idx]),
                .max => m = @max(m, array.values.ptr[idx]),
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
        const validity = (array.validity orelse unreachable).ptr;

        var m: []const u8 = undefined;
        var idx: u32 = array.offset;

        // find the first value
        while (idx < array.offset + array.len) : (idx += 1) {
            if (bitmap.get(validity, idx)) {
                m = get.get_binary(index_t, array.data.ptr, array.offsets.ptr, idx);
                idx += 1;
                break;
            }
        } else {
            return null;
        }

        while (idx < array.offset + array.len) : (idx += 1) {
            if (bitmap.get(validity, idx)) {
                const s = get.get_binary(index_t, array.data.ptr, array.offsets.ptr, idx);
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
        }

        return m;
    } else {
        var m: []const u8 = get.get_binary(index_t, array.data.ptr, array.offsets.ptr, array.offset);
        var idx: u32 = array.offset + 1;

        while (idx < array.offset + array.len) : (idx += 1) {
            const s = get.get_binary(index_t, array.data.ptr, array.offsets.ptr, idx);
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
        const validity = (array.validity orelse unreachable).ptr;

        var m: []const u8 = undefined;
        var idx: u32 = array.offset;

        // find the first value
        while (idx < array.offset + array.len) : (idx += 1) {
            if (bitmap.get(validity, idx)) {
                m = get.get_binary_view(array.buffers.ptr, array.views.ptr, idx);
                idx += 1;
                break;
            }
        } else {
            return null;
        }

        while (idx < array.offset + array.len) : (idx += 1) {
            if (bitmap.get(validity, idx)) {
                const s = get.get_binary_view(array.buffers.ptr, array.views.ptr, idx);
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
        }

        return m;
    } else {
        var m: []const u8 = get.get_binary_view(array.buffers.ptr, array.views.ptr, array.offset);
        var idx: u32 = array.offset + 1;

        while (idx < array.offset + array.len) : (idx += 1) {
            const s = get.get_binary_view(array.buffers.ptr, array.views.ptr, idx);
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
        const validity = (array.validity orelse unreachable).ptr;

        var m: []const u8 = undefined;
        var idx: u32 = array.offset;

        // find the first value
        while (idx < array.offset + array.len) : (idx += 1) {
            if (bitmap.get(validity, idx)) {
                m = get.get_fixed_size_binary(array.data.ptr, array.byte_width, idx);
                idx += 1;
                break;
            }
        } else {
            return null;
        }

        while (idx < array.offset + array.len) : (idx += 1) {
            if (bitmap.get(validity, idx)) {
                const s = get.get_fixed_size_binary(array.data.ptr, array.byte_width, idx);
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
        }

        return m;
    } else {
        var m = get.get_fixed_size_binary(array.data.ptr, array.byte_width, array.offset);
        var idx: u32 = array.offset + 1;

        while (idx < array.offset + array.len) : (idx += 1) {
            const s = get.get_fixed_size_binary(array.data.ptr, array.byte_width, idx);
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
