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

fn concat_test(array: *const arr.Array, input: *FuzzInput, alloc: Allocator) !void {
    var concat_arena = ArenaAllocator.init(alloc);
    defer concat_arena.deinit();
    const concat_alloc = concat_arena.allocator();

    const slice0 = try input.slice_array(array);
    try validate.validate(&slice0);
    const slice1 = try input.slice_array(array);
    try validate.validate(&slice1);
    const slice2 = try input.slice_array(array);
    try validate.validate(&slice2);

    const dt = try data_type.get_data_type(array, concat_alloc);

    const concated = conc: {
        var scratch_arena = ArenaAllocator.init(alloc);
        defer scratch_arena.deinit();
        const scratch_alloc = scratch_arena.allocator();
        break :conc try concat(dt, &.{ slice0, slice1, slice2 }, concat_alloc, scratch_alloc);
    };
    try validate.validate(&concated);

    const slice0_len = length.length(&slice0);
    const slice1_len = length.length(&slice1);
    const slice2_len = length.length(&slice2);
    const slice0_out = slice_array_impl(&concated, 0, slice0_len);
    const slice1_out = slice_array_impl(&concated, slice0_len, slice1_len);
    const slice2_out = slice_array_impl(&concated, slice0_len + slice1_len, slice2_len);

    equals.equals(&slice0_out, &slice0);
    equals.equals(&slice1_out, &slice1);
    equals.equals(&slice2_out, &slice2);
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

    var arena = ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();
    const array_len = try input.int(u8);
    const array = try input.make_array(array_len, alloc);

    try validate.validate(&array);
    try concat_test(&array, &input, gpa);
}

fn to_fuzz_wrap(_: void, data: []const u8) anyerror!void {
    return to_fuzz(data) catch |e| {
        if (e == error.ShortInput) return {} else return e;
    };
}

test "fuzz" {
    try std.testing.fuzz({}, to_fuzz_wrap, .{});
}
