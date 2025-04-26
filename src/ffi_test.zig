const std = @import("std");
const ArenaAllocator = std.heap.ArenaAllocator;
const Allocator = std.mem.Allocator;
const testing = std.testing;

const ffi = @import("./ffi.zig");
const arr = @import("./array.zig");
const bitmap = @import("./bitmap.zig");
const expect_equal = @import("./expect_equal.zig").expect_equal;

extern fn test_helper_roundtrip_array(input_array: *const anyopaque, input_schema: *const anyopaque, output_array: *anyopaque, output_schema: *anyopaque) i32;

fn run_test(array: *const arr.Array, arena: ArenaAllocator) !void {
    var input = ffi_export: {
        const x = try ffi.export_array(.{
            .array = array,
            .arena = arena,
        });
        errdefer arena.deinit();

        break :ffi_export x;
    };

    var output: ffi.FFI_Array = undefined;

    errdefer input.release();

    try testing.expectEqual(0, test_helper_roundtrip_array(&input.array, &input.schema, &output.array, &output.schema));

    defer output.release();
    var import_arena = ArenaAllocator.init(testing.allocator);
    defer import_arena.deinit();
    const import_alloc = import_arena.allocator();
    const imported = try ffi.import_array(&output, import_alloc);
    try expect_equal(array, &imported);
}

fn validity_len(len: u32) u32 {
    return (len + 7) / 8;
}

fn make_primitive(comptime T: type, vals: []const T, allocator: Allocator) !arr.PrimitiveArr(T) {
    const values = try allocator.alloc(T, vals.len);
    @memcpy(values, vals);

    const offset: u32 = 2;
    const len = @as(u32, @intCast(vals.len)) - offset;

    const bitmap_len = validity_len(@intCast(vals.len));

    const validity = try allocator.alloc(u8, bitmap_len);
    for (0..bitmap_len) |i| {
        validity[i] = @intCast(i % 256);
    }

    var null_count: u32 = 0;
    for (offset..offset + len) |i| {
        null_count += @intFromBool(bitmap.get(validity, @intCast(i)));
    }

    return .{
        .len = len,
        .offset = offset,
        .validity = validity,
        .null_count = null_count,
        .values = values,
    };
}

fn test_primitive(comptime array_type: arr.ArrayType, comptime T: type, vals: []const T) !void {
    var arena = ArenaAllocator.init(testing.allocator);

    const array = init: {
        const allocator = arena.allocator();
        errdefer arena.deinit();
        break :init try make_primitive(T, vals, allocator);
    };

    try run_test(&@unionInit(arr.Array, @tagName(array_type), array), arena);
}

test "primitive roundtrip" {
    try test_primitive(.i8, i8, &[_]i8{ -5, -69, -12, 3, 2, 3, 1, 2, 122 });
    try test_primitive(.u8, u8, &[_]u8{ 3, 2, 3, 1, 2, 132 });
    try test_primitive(.i16, i16, &[_]i16{ -5, -69, 3, 2, 3, 1, 2, 132, 321, 324 });
    try test_primitive(.u16, u16, &[_]u16{ 3, 2, 3, 1, 2, 132, 321, 324 });
    try test_primitive(.i32, i32, &[_]i32{ -5, -69, -123123, 3, 2, 3, 1, 2, 132, 321, 324, 56456 });
    try test_primitive(.u32, u32, &[_]u32{ 3, 2, 3, 1, 2, 132, 321, 324, 56456 });
    try test_primitive(.i64, i64, &[_]i64{ -5, -69, -123123, 3, 2, 3, 1, 2, 132, 321, 324, 56456 });
    try test_primitive(.u64, u64, &[_]u64{ 3, 2, 3, 1, 2, 132, 321, 324, 56456 });
}
