const std = @import("std");
const ArenaAllocator = std.heap.ArenaAllocator;
const testing = std.testing;

const ffi = @import("./ffi.zig");
const arr = @import("./array.zig");

extern fn test_helper_roundtrip_array(input_array: *const anyopaque, input_schema: *const anyopaque, output_array: *anyopaque, output_schema: *anyopaque) i32;

test "wqe" {
    const allocator = testing.allocator;

    var arena = ArenaAllocator.init(allocator);
    const arena_alloc = arena.allocator();
    const values = try arena_alloc.alloc(i32, 4);
    @memcpy(values, &[_]i32{ 1, 2, 3, 4 });
    const array = arr.Int32Array{
        .len = 3,
        .offset = 1,
        .validity = null,
        .null_count = 0,
        .values = values,
    };

    const input = try ffi.export_array(.{
        .array = arr.Array.from_ptr(.i32, &array),
        .arena = arena,
    });

    var output: ffi.FFI_Array = undefined;

    try testing.expectEqual(0, test_helper_roundtrip_array(&input.array, &input.schema, &output.array, &output.schema));

    output.release();
}
