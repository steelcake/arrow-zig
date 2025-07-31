const std = @import("std");
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const arr = @import("./array.zig");
const length = @import("./length.zig");
const slice_array_impl = @import("./slice.zig").slice;
const data_type = @import("./data_type.zig");
const concat = @import("./concat.zig").concat;
const equals = @import("./equals.zig");
const validate = @import("./validate.zig");

const FuzzInput = @import("./fuzz_input.zig").FuzzInput;

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

    var arena = ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();
    const array_len = try input.int(u8);
    const array = try input.make_array(array_len, alloc);

    const dt = try input.make_data_type(alloc);

    try validate.validate(&array);

    // ignore errors, only check for crash
    _ = data_type.check_data_type(&array, &dt);
}

fn to_fuzz_wrap(_: void, data: []const u8) anyerror!void {
    return to_fuzz(data) catch |e| {
        if (e == error.ShortInput) return {} else return e;
    };
}

test "fuzz" {
    try std.testing.fuzz({}, to_fuzz_wrap, .{});
}
