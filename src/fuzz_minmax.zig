const std = @import("std");
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const arr = @import("./array.zig");
const minmax = @import("./minmax.zig");
const validate = @import("./validate.zig");

const FuzzInput = @import("./fuzz_input.zig").FuzzInput;

fn minmax_test(input: *FuzzInput, alloc: Allocator) !void {
    var arena = ArenaAllocator.init(alloc);
    defer arena.deinit();
    const arena_alloc = arena.allocator();

    const array_len = try input.int(u8);

    const array: arr.Array = switch ((try input.int(u8)) % 15) {
        0 => .{ .i8 = try input.primitive_array(i8, array_len, arena_alloc) },
        1 => .{ .i16 = try input.primitive_array(i16, array_len, arena_alloc) },
        2 => .{ .i32 = try input.primitive_array(i32, array_len, arena_alloc) },
        3 => .{ .i64 = try input.primitive_array(i64, array_len, arena_alloc) },
        4 => .{ .u8 = try input.primitive_array(u8, array_len, arena_alloc) },
        5 => .{ .u16 = try input.primitive_array(u16, array_len, arena_alloc) },
        6 => .{ .u32 = try input.primitive_array(u32, array_len, arena_alloc) },
        7 => .{ .u64 = try input.primitive_array(u64, array_len, arena_alloc) },
        8 => .{ .decimal32 = try input.decimal_array(.i32, array_len, arena_alloc) },
        9 => .{ .decimal64 = try input.decimal_array(.i64, array_len, arena_alloc) },
        10 => .{ .decimal128 = try input.decimal_array(.i128, array_len, arena_alloc) },
        11 => .{ .decimal256 = try input.decimal_array(.i256, array_len, arena_alloc) },
        12 => .{ .binary = try input.binary_array(.i32, array_len, arena_alloc) },
        13 => .{ .binary_view = try input.binary_view_array(array_len, arena_alloc) },
        14 => .{ .fixed_size_binary = try input.fixed_size_binary_array(array_len, arena_alloc) },
        else => unreachable,
    };

    try validate.validate(&array);

    const min_result = try minmax.min(&array);
    minmax.check_min(&array, min_result);
    const max_result = try minmax.max(&array);
    minmax.check_max(&array, max_result);
}

fn to_fuzz(data: []const u8) !void {
    var general_purpose_allocator: std.heap.GeneralPurposeAllocator(.{}) = .init;
    const gpa = general_purpose_allocator.allocator();
    defer {
        switch (general_purpose_allocator.deinit()) {
            .ok => {},
            .leak => |l| {
                std.debug.panic("LEAK: {any}", .{l});
            },
        }
    }

    var input = FuzzInput{ .data = data };
    try minmax_test(&input, gpa);
}

fn to_fuzz_wrap(_: void, data: []const u8) anyerror!void {
    return to_fuzz(data) catch |e| {
        if (e == error.ShortInput) return {} else return e;
    };
}

test "fuzz" {
    try std.testing.fuzz({}, to_fuzz_wrap, .{});
}
