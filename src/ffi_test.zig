const std = @import("std");
const testing = std.testing;
const ArenaAllocator = std.heap.ArenaAllocator;
const Allocator = std.mem.Allocator;

const arr = @import("./array.zig");
const ffi = @import("./ffi.zig");
const equals = @import("./equals.zig").equals;
const test_array = @import("./test_array.zig");
const make_array = test_array.make_array;
const NUM_TESTS = test_array.NUM_ARRAYS;

// Each `id` corresponds to a specific arrow array, this function is supposed to import the given array, create a new array based on the `id` it receives, assert these two arrays are equal,
// and export the array it created back to the caller.
extern fn arrow_ffi_test_case(id: u8, array: ffi.abi.ArrowArray, schema: ffi.abi.ArrowSchema, out_array: *ffi.abi.ArrowArray, out_schema: *ffi.abi.ArrowSchema) callconv(.C) void;

fn ffi_test_case(id: u8, array: ffi.abi.ArrowArray, schema: ffi.abi.ArrowSchema, out_array: *ffi.abi.ArrowArray, out_schema: *ffi.abi.ArrowSchema) void {
    var import_arena = ArenaAllocator.init(testing.allocator);
    defer import_arena.deinit();

    var ffi_arr = ffi.FFI_Array{ .array = array, .schema = schema };
    defer ffi_arr.release();

    const imported_arr = ffi.import_array(&ffi_arr, import_arena.allocator()) catch unreachable;

    var make_arena = ArenaAllocator.init(testing.allocator);
    const made_arr = test_array.make_array(id, make_arena.allocator()) catch unreachable;

    equals(&imported_arr, &made_arr);

    const export_ffi_arr = ffi.export_array(.{ .array = &made_arr, .arena = make_arena }) catch unreachable;

    out_array.* = export_ffi_arr.array;
    out_schema.* = export_ffi_arr.schema;
}

fn run_test_impl(id: u8) !void {
    var input_ffi_array = ffi.FFI_Array{ .array = undefined, .schema = undefined };

    {
        var arena = ArenaAllocator.init(testing.allocator);
        const allocator = arena.allocator();
        const array = try make_array(id, allocator);
        const ffi_array = try ffi.export_array(.{ .array = &array, .arena = arena });
        ffi_test_case(id, ffi_array.array, ffi_array.schema, &input_ffi_array.array, &input_ffi_array.schema);
    }

    defer input_ffi_array.release();

    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const output_array = try make_array(id, allocator);
    const input_array = try ffi.import_array(&input_ffi_array, allocator);
    equals(&output_array, &input_array);
}

fn run_test(id: u8) !void {
    return run_test_impl(id) catch |e| err: {
        std.log.err("failed test id: {}", .{id});
        break :err e;
    };
}

test "ffi basic" {
    for (0..NUM_TESTS) |i| {
        try run_test(@intCast(i));
    }
}
