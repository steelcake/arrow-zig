const std = @import("std");
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const arr = @import("./array.zig");
const equals = @import("./equals.zig");
const ffi = @import("./ffi.zig");
const validate = @import("./validate.zig");

const FuzzInput = @import("./fuzz_input.zig").FuzzInput;

fn ffi_test(array: *const arr.Array, export_arena: ArenaAllocator, alloc: Allocator) !void {
    var ffi_array = try ffi.export_array(.{ .array = array, .arena = export_arena });
    defer ffi_array.release();

    var import_arena = ArenaAllocator.init(alloc);
    const import_alloc = import_arena.allocator();
    defer import_arena.deinit();
    const imported = try ffi.import_array(&ffi_array, import_alloc);
    try validate.validate(&imported);

    equals.equals(&imported, array);
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

    var arena = ArenaAllocator.init(gpa);
    // Don't free the arena on success since ffi_test will consume it
    errdefer arena.deinit();
    const alloc = arena.allocator();

    var input = FuzzInput{ .data = data };
    const array_len = try input.int(u8);
    const array = try input.make_array(array_len, alloc);
    try validate.validate(&array);

    try ffi_test(&array, arena, gpa);
}

fn to_fuzz_wrap(_: void, data: []const u8) anyerror!void {
    return to_fuzz(data) catch |e| {
        if (e == error.ShortInput) return {} else return e;
    };
}

test "fuzz" {
    try std.testing.fuzz({}, to_fuzz_wrap, .{});
}
