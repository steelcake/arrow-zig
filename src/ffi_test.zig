const std = @import("std");
const testing = std.testing;
const ArenaAllocator = std.heap.ArenaAllocator;
const Allocator = std.mem.Allocator;

const arr = @import("./array.zig");
const builder = @import("./builder.zig");
const ffi = @import("./ffi.zig");
const equals = @import("./equals.zig").equals;
const slice = @import("./slice.zig").slice;

// Each `id` corresponds to a specific arrow array, this function is supposed to import the given array, create a new array based on the `id` it receives, assert these two arrays are equal,
// and export the array it created back to the caller.
extern fn arrow_ffi_test_case(id: u8, array: ffi.abi.ArrowArray, schema: ffi.abi.ArrowSchema, out_array: *ffi.abi.ArrowArray, out_schema: *ffi.abi.ArrowSchema) void;

fn make_array(id: u8, allocator: Allocator) !arr.Array {
    return switch (id) {
        0 => slice(&.{ .i8 = try builder.Int8Builder.from_slice_opt(&.{ 111, 1, -1, 69, null, -69, null }, allocator) }, 1, 6),
        1 => .{ .i16 = try builder.Int16Builder.from_slice_opt(&.{ 1, -1, 69, null, -69, null }, allocator) },
        2 => .{ .i32 = try builder.Int32Builder.from_slice_opt(&.{ 1, -1, 69, null, -69, null }, allocator) },
        3 => .{ .i64 = try builder.Int64Builder.from_slice_opt(&.{ 1, -1, 69, null, -69, null }, allocator) },
        4 => .{ .u8 = try builder.UInt8Builder.from_slice(&.{ 1, 1, 69, 69 }, false, allocator) },
        5 => .{ .u16 = try builder.UInt16Builder.from_slice_opt(&.{ 1, 1, null, null, 69, 69 }, allocator) },
        6 => .{ .u32 = try builder.UInt32Builder.from_slice_opt(&.{ 1, 1, null, null, 69, 69 }, allocator) },
        7 => .{ .u64 = try builder.UInt64Builder.from_slice_opt(&.{ 1, 1, null, null, 69, 69 }, allocator) },
        8 => .{ .null = .{ .len = 69 } },
        9 => .{ .bool = try builder.BoolBuilder.from_slice_opt(&.{ true, false, null }, allocator) },
        10 => .{ .f32 = try builder.Float32Builder.from_slice_opt(&.{ 1, -1, 69, null, -69, null }, allocator) },
        11 => .{ .f64 = try builder.Float64Builder.from_slice_opt(&.{ 1, -1, 69, null, -69, null }, allocator) },
        12 => .{ .binary = try builder.BinaryBuilder.from_slice_opt(&.{ "hello", "world", null }, allocator) },
        13 => .{ .large_binary = try builder.LargeBinaryBuilder.from_slice_opt(&.{ "hello", "world", null }, allocator) },
        14 => .{ .utf8 = try builder.Utf8Builder.from_slice_opt(&.{ "hello", "world", null }, allocator) },
        15 => .{ .large_utf8 = try builder.LargeUtf8Builder.from_slice_opt(&.{ "hello", "world", null }, allocator) },
        16 => .{ .decimal128 = try builder.Decimal128Builder.from_slice(.{ .precision = 31, .scale = -31 }, &.{ 1, 2, 3, 4, 69 }, true, allocator) },
        17 => .{ .decimal256 = try builder.Decimal256Builder.from_slice(.{ .precision = 31, .scale = -31 }, &.{ 69, -69 }, false, allocator) },
        18 => .{ .date32 = try builder.Date32Builder.from_slice(&.{ 69, 69, 11, 15 }, false, allocator) },
        19 => .{ .date64 = try builder.Date64Builder.from_slice(&.{ 69, 69, 11, 15 }, false, allocator) },
        else => unreachable,
    };
}

fn run_test(id: u8) !void {
    var input_ffi_array = ffi.FFI_Array{ .array = undefined, .schema = undefined };

    {
        var arena = ArenaAllocator.init(testing.allocator);
        const allocator = arena.allocator();
        const array = try make_array(id, allocator);
        const ffi_array = try ffi.export_array(.{ .array = &array, .arena = arena });
        arrow_ffi_test_case(id, ffi_array.array, ffi_array.schema, &input_ffi_array.array, &input_ffi_array.schema);
    }

    defer input_ffi_array.release();

    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const output_array = try make_array(id, allocator);
    const input_array = try ffi.import_array(&input_ffi_array, allocator);
    try equals(&output_array, &input_array);
}

// fn run_test(id: u8) !void {
//     return run_test_impl(id) catch |e| err: {
//         std.log.err("failed test id: {}", .{id});
//         break :err e;
//     };
// }

test "ffi basic" {
    try run_test(0);
    try run_test(1);
    try run_test(2);
    try run_test(3);
    try run_test(4);
    try run_test(5);
    try run_test(6);
    try run_test(7);
    try run_test(8);
    try run_test(9);
    try run_test(10);
    try run_test(11);
    try run_test(12);
    try run_test(13);
    try run_test(14);
    try run_test(15);
    try run_test(16);
    try run_test(17);
    try run_test(18);
    try run_test(19);
    try run_test(20);
    try run_test(21);
    try run_test(22);
    try run_test(23);
    try run_test(24);
    try run_test(25);
    try run_test(26);
    try run_test(27);
    try run_test(28);
    try run_test(29);
    try run_test(30);
    try run_test(31);
    try run_test(32);
    try run_test(33);
    try run_test(34);
    try run_test(35);
    try run_test(36);
    try run_test(37);
    try run_test(38);
    try run_test(39);
    try run_test(40);
}
