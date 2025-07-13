const std = @import("std");
const arr = @import("./array.zig");
const Scalar = @import("./scalar.zig").Scalar;
const bitmap = @import("./bitmap.zig");
const get = @import("./get.zig");
const builder = @import("./builder.zig");
const testing = std.testing;
const ArenaAllocator = std.heap.ArenaAllocator;

const Error = error{
    ArrayTypeNotSupported,
};

const Op = enum {
    min,
    max,
};

fn primitive_impl(comptime T: type, comptime op: Op, array: *const arr.PrimitiveArray(T)) ?T {
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

fn binary_impl(comptime index_t: arr.IndexType, comptime op: Op, array: *const arr.GenericBinaryArray(index_t)) ?[]const u8 {
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

fn binary_view_impl(comptime op: Op, array: *const arr.BinaryViewArray) ?[]const u8 {
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

fn fixed_size_binary_impl(comptime op: Op, array: *const arr.FixedSizeBinaryArray) ?[]const u8 {
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

pub fn min_primitive(comptime T: type, array: *const arr.PrimitiveArray(T)) ?T {
    return primitive_impl(T, .min, array);
}

pub fn min_binary(comptime index_t: arr.IndexType, array: *const arr.GenericBinaryArray(index_t)) ?[]const u8 {
    return binary_impl(index_t, .min, array);
}

pub fn min_fixed_size_binary(array: *const arr.FixedSizeBinaryArray) ?[]const u8 {
    return fixed_size_binary_impl(.min, array);
}

pub fn min_binary_view(array: *const arr.BinaryViewArray) ?[]const u8 {
    return binary_view_impl(.min, array);
}

pub fn min(array: *const arr.Array) Error!?Scalar {
    switch (array.*) {
        .i8 => |*a| return if (min_primitive(i8, a)) |m| .{ .i8 = m } else null,
        .i16 => |*a| return if (min_primitive(i16, a)) |m| .{ .i16 = m } else null,
        .i32 => |*a| return if (min_primitive(i32, a)) |m| .{ .i32 = m } else null,
        .i64 => |*a| return if (min_primitive(i64, a)) |m| .{ .i64 = m } else null,
        .u8 => |*a| return if (min_primitive(u8, a)) |m| .{ .u8 = m } else null,
        .u16 => |*a| return if (min_primitive(u16, a)) |m| .{ .u16 = m } else null,
        .u32 => |*a| return if (min_primitive(u32, a)) |m| .{ .u32 = m } else null,
        .u64 => |*a| return if (min_primitive(u64, a)) |m| .{ .u64 = m } else null,
        .f16 => |*a| return if (min_primitive(f16, a)) |m| .{ .f16 = m } else null,
        .f32 => |*a| return if (min_primitive(f32, a)) |m| .{ .f32 = m } else null,
        .f64 => |*a| return if (min_primitive(f64, a)) |m| .{ .f64 = m } else null,
        .binary => |*a| return if (min_binary(.i32, a)) |m| .{ .binary = m } else null,
        .utf8 => |*a| return if (min_binary(.i32, &a.inner)) |m| .{ .binary = m } else null,
        .decimal32 => |*a| return if (min_primitive(i32, &a.inner)) |m| .{ .i32 = m } else null,
        .decimal64 => |*a| return if (min_primitive(i64, &a.inner)) |m| .{ .i64 = m } else null,
        .decimal128 => |*a| return if (min_primitive(i128, &a.inner)) |m| .{ .i128 = m } else null,
        .decimal256 => |*a| return if (min_primitive(i256, &a.inner)) |m| .{ .i256 = m } else null,
        .large_binary => |*a| return if (min_binary(.i64, a)) |m| .{ .binary = m } else null,
        .large_utf8 => |*a| return if (min_binary(.i64, &a.inner)) |m| .{ .binary = m } else null,
        .binary_view => |*a| return if (min_binary_view(a)) |m| .{ .binary = m } else null,
        .utf8_view => |*a| return if (min_binary_view(&a.inner)) |m| .{ .binary = m } else null,
        .fixed_size_binary => |*a| return if (min_fixed_size_binary(a)) |m| .{ .binary = m } else null,
        else => return Error.ArrayTypeNotSupported,
    }
}

pub fn max_primitive(comptime T: type, array: *const arr.PrimitiveArray(T)) ?T {
    return primitive_impl(T, .max, array);
}

pub fn max_binary(comptime index_t: arr.IndexType, array: *const arr.GenericBinaryArray(index_t)) ?[]const u8 {
    return binary_impl(index_t, .max, array);
}

pub fn max_fixed_size_binary(array: *const arr.FixedSizeBinaryArray) ?[]const u8 {
    return fixed_size_binary_impl(.max, array);
}

pub fn max_binary_view(array: *const arr.BinaryViewArray) ?[]const u8 {
    return binary_view_impl(.max, array);
}

pub fn max(array: *const arr.Array) Error!?Scalar {
    switch (array.*) {
        .i8 => |*a| return if (max_primitive(i8, a)) |m| .{ .i8 = m } else null,
        .i16 => |*a| return if (max_primitive(i16, a)) |m| .{ .i16 = m } else null,
        .i32 => |*a| return if (max_primitive(i32, a)) |m| .{ .i32 = m } else null,
        .i64 => |*a| return if (max_primitive(i64, a)) |m| .{ .i64 = m } else null,
        .u8 => |*a| return if (max_primitive(u8, a)) |m| .{ .u8 = m } else null,
        .u16 => |*a| return if (max_primitive(u16, a)) |m| .{ .u16 = m } else null,
        .u32 => |*a| return if (max_primitive(u32, a)) |m| .{ .u32 = m } else null,
        .u64 => |*a| return if (max_primitive(u64, a)) |m| .{ .u64 = m } else null,
        .f16 => |*a| return if (max_primitive(f16, a)) |m| .{ .f16 = m } else null,
        .f32 => |*a| return if (max_primitive(f32, a)) |m| .{ .f32 = m } else null,
        .f64 => |*a| return if (max_primitive(f64, a)) |m| .{ .f64 = m } else null,
        .binary => |*a| return if (max_binary(.i32, a)) |m| .{ .binary = m } else null,
        .utf8 => |*a| return if (max_binary(.i32, &a.inner)) |m| .{ .binary = m } else null,
        .decimal32 => |*a| return if (max_primitive(i32, &a.inner)) |m| .{ .i32 = m } else null,
        .decimal64 => |*a| return if (max_primitive(i64, &a.inner)) |m| .{ .i64 = m } else null,
        .decimal128 => |*a| return if (max_primitive(i128, &a.inner)) |m| .{ .i128 = m } else null,
        .decimal256 => |*a| return if (max_primitive(i256, &a.inner)) |m| .{ .i256 = m } else null,
        .large_binary => |*a| return if (max_binary(.i64, a)) |m| .{ .binary = m } else null,
        .large_utf8 => |*a| return if (max_binary(.i64, &a.inner)) |m| .{ .binary = m } else null,
        .binary_view => |*a| return if (max_binary_view(a)) |m| .{ .binary = m } else null,
        .utf8_view => |*a| return if (max_binary_view(&a.inner)) |m| .{ .binary = m } else null,
        .fixed_size_binary => |*a| return if (max_fixed_size_binary(a)) |m| .{ .binary = m } else null,
        else => return Error.ArrayTypeNotSupported,
    }
}

test "min i16" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.Int16Builder.from_slice_opt(&.{ 1, -1, 69, null, -69, null }, allocator);

    const expected: ?Scalar = .{ .i16 = -69 };
    const result: ?Scalar = try min(&.{ .i16 = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "min binary" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.BinaryBuilder.from_slice_opt(&.{ "hello", "world", null }, allocator);

    const expected: ?Scalar = .{ .binary = "hello" };
    const result: ?Scalar = try min(&.{ .binary = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "min decimal256" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.Decimal256Builder.from_slice_opt(.{ .precision = 31, .scale = -31 }, &.{ null, null, null, -69, 69 }, allocator);

    const expected: ?Scalar = .{ .i256 = -69 };
    const result: ?Scalar = try min(&.{ .decimal256 = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "min fixed-size binary" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.FixedSizeBinaryBuilder.from_slice_opt(4, &.{ "anan", "zaaa", null, "xddd" }, allocator);

    const expected: ?Scalar = .{ .binary = "anan" };
    const result: ?Scalar = try min(&.{ .fixed_size_binary = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "min binary-view" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.BinaryViewBuilder.from_slice_opt(&.{ "hello", "world", null }, allocator);

    const expected: ?Scalar = .{ .binary = "hello" };
    const result: ?Scalar = try min(&.{ .binary_view = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "min i16 empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.Int16Builder.from_slice_opt(&.{}, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try min(&.{ .i16 = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "min binary empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.BinaryBuilder.from_slice_opt(&.{}, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try min(&.{ .binary = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "min decimal256 empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.Decimal256Builder.from_slice(.{ .precision = 31, .scale = -31 }, &.{}, false, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try min(&.{ .decimal256 = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "min fixed-size-binary empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.FixedSizeBinaryBuilder.from_slice_opt(4, &.{}, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try min(&.{ .fixed_size_binary = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "min binary-view empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.BinaryViewBuilder.from_slice_opt(&.{}, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try min(&.{ .binary_view = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "min i16 null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.Int16Builder.from_slice_opt(&.{ null, null }, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try min(&.{ .i16 = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "min binary null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.BinaryBuilder.from_slice_opt(&.{null}, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try min(&.{ .binary = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "min decimal256 null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.Decimal256Builder.from_slice_opt(.{ .precision = 31, .scale = -31 }, &.{null}, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try min(&.{ .decimal256 = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "min fixed-size-binary null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.FixedSizeBinaryBuilder.from_slice_opt(4, &.{null}, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try min(&.{ .fixed_size_binary = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "min binary-view null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.BinaryViewBuilder.from_slice_opt(&.{ null, null, null }, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try min(&.{ .binary_view = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "min i16 non-null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.Int16Builder.from_slice(&.{ 1, 2, 69, -11, 0 }, false, allocator);

    const expected: ?Scalar = .{ .i16 = -11 };
    const result: ?Scalar = try min(&.{ .i16 = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "min binary non-null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.BinaryBuilder.from_slice(&.{ "asd", "qwe" }, false, allocator);

    const expected: ?Scalar = .{ .binary = "asd" };
    const result: ?Scalar = try min(&.{ .binary = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "min decimal256 non-null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.Decimal256Builder.from_slice(.{ .precision = 31, .scale = -31 }, &.{ 1131, -1131, 69, -69, -2000, -1999 }, false, allocator);

    const expected: ?Scalar = .{ .i256 = -2000 };
    const result: ?Scalar = try min(&.{ .decimal256 = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "min fixed-size-binary non-null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.FixedSizeBinaryBuilder.from_slice(4, &.{ "asdf", "qwee" }, false, allocator);

    const expected: ?Scalar = .{ .binary = "asdf" };
    const result: ?Scalar = try min(&.{ .fixed_size_binary = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "min binary-view non-null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.BinaryViewBuilder.from_slice(&.{ "ASDSADSA", "qweqwe", "xzczxcxz" }, false, allocator);

    const expected: ?Scalar = .{ .binary = "ASDSADSA" };
    const result: ?Scalar = try min(&.{ .binary_view = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max i16" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.Int16Builder.from_slice_opt(&.{ 1, -1, 69, null, -69, null }, allocator);

    const expected: ?Scalar = .{ .i16 = 69 };
    const result: ?Scalar = try max(&.{ .i16 = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max binary" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.BinaryBuilder.from_slice_opt(&.{ "hello", "world", null }, allocator);

    const expected: ?Scalar = .{ .binary = "world" };
    const result: ?Scalar = try max(&.{ .binary = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max decimal256" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.Decimal256Builder.from_slice_opt(.{ .precision = 31, .scale = -31 }, &.{ null, null, null, -69, 69 }, allocator);

    const expected: ?Scalar = .{ .i256 = 69 };
    const result: ?Scalar = try max(&.{ .decimal256 = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max fixed-size binary" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.FixedSizeBinaryBuilder.from_slice_opt(4, &.{ "anan", "zaaa", null, "xddd" }, allocator);

    const expected: ?Scalar = .{ .binary = "zaaa" };
    const result: ?Scalar = try max(&.{ .fixed_size_binary = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max binary-view" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.BinaryViewBuilder.from_slice_opt(&.{ "hello", "world", null }, allocator);

    const expected: ?Scalar = .{ .binary = "world" };
    const result: ?Scalar = try max(&.{ .binary_view = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max i16 empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.Int16Builder.from_slice_opt(&.{}, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try max(&.{ .i16 = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max binary empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.BinaryBuilder.from_slice_opt(&.{}, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try max(&.{ .binary = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max decimal256 empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.Decimal256Builder.from_slice(.{ .precision = 31, .scale = -31 }, &.{}, false, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try max(&.{ .decimal256 = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max fixed-size-binary empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.FixedSizeBinaryBuilder.from_slice_opt(4, &.{}, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try max(&.{ .fixed_size_binary = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max binary-view empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.BinaryViewBuilder.from_slice_opt(&.{}, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try max(&.{ .binary_view = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max i16 null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.Int16Builder.from_slice_opt(&.{ null, null }, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try max(&.{ .i16 = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max binary null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.BinaryBuilder.from_slice_opt(&.{null}, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try max(&.{ .binary = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max decimal256 null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.Decimal256Builder.from_slice_opt(.{ .precision = 31, .scale = -31 }, &.{null}, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try max(&.{ .decimal256 = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max fixed-size-binary null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.FixedSizeBinaryBuilder.from_slice_opt(4, &.{null}, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try max(&.{ .fixed_size_binary = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max binary-view null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.BinaryViewBuilder.from_slice_opt(&.{ null, null, null }, allocator);

    const expected: ?Scalar = null;
    const result: ?Scalar = try max(&.{ .binary_view = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max i16 non-null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.Int16Builder.from_slice(&.{ 1, 2, 69, -11, 0 }, false, allocator);

    const expected: ?Scalar = .{ .i16 = 69 };
    const result: ?Scalar = try max(&.{ .i16 = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max binary non-null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.BinaryBuilder.from_slice(&.{ "asd", "qwe" }, false, allocator);

    const expected: ?Scalar = .{ .binary = "qwe" };
    const result: ?Scalar = try max(&.{ .binary = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max decimal256 non-null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.Decimal256Builder.from_slice(.{ .precision = 31, .scale = -31 }, &.{ 1131, -1131, 69, -69, -2000, -1999 }, false, allocator);

    const expected: ?Scalar = .{ .i256 = 1131 };
    const result: ?Scalar = try max(&.{ .decimal256 = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max fixed-size-binary non-null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.FixedSizeBinaryBuilder.from_slice(4, &.{ "asdf", "qwee" }, false, allocator);

    const expected: ?Scalar = .{ .binary = "qwee" };
    const result: ?Scalar = try max(&.{ .fixed_size_binary = array });

    try std.testing.expectEqualDeep(expected, result);
}

test "max binary-view non-null" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const array = try builder.BinaryViewBuilder.from_slice(&.{ "ASDSADSA", "qweqwe", "xzczxcxz" }, false, allocator);

    const expected: ?Scalar = .{ .binary = "xzczxcxz" };
    const result: ?Scalar = try max(&.{ .binary_view = array });

    try std.testing.expectEqualDeep(expected, result);
}
