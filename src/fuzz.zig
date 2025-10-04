const std = @import("std");
const FixedBufferAllocator = std.heap.FixedBufferAllocator;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const arr = @import("./array.zig");
const length = @import("./length.zig");
const slice_array_impl = @import("./slice.zig").slice;
const data_type = @import("./data_type.zig");
const concat = @import("./concat.zig").concat;
const equals = @import("./equals.zig");
const validate = @import("./validate.zig");
const ffi = @import("./ffi.zig");
const minmax = @import("./minmax.zig");

const FuzzInput = @import("./fuzz_input.zig").FuzzInput;

fn fuzz_minmax(data: []const u8, gpa: Allocator) !void {
    var input = FuzzInput{ .data = data };

    var arena = ArenaAllocator.init(gpa);
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

test "fuzz minmax" {
    try FuzzWrap(fuzz_minmax, 1 << 30).run();
}

fn fuzz_ffi(data: []const u8, gpa: Allocator) !void {
    var arena = ArenaAllocator.init(gpa);
    const alloc = arena.allocator();

    var input = FuzzInput{ .data = data };
    const array_len = input.int(u8) catch |e| {
        arena.deinit();
        return e;
    };

    const array = input.make_array(array_len, alloc) catch |e| {
        arena.deinit();
        return e;
    };

    validate.validate(&array) catch |e| {
        arena.deinit();
        return e;
    };

    // don't free the arena if we reach this point because ffi.export_array takes ownership of it
    var ffi_array = try ffi.export_array(.{ .array = &array, .arena = arena });
    defer ffi_array.release();

    var import_arena = ArenaAllocator.init(gpa);
    const import_alloc = import_arena.allocator();
    defer import_arena.deinit();
    const imported = try ffi.import_array(&ffi_array, import_alloc);
    try validate.validate(&imported);

    equals.equals(&imported, &array);
}

test "fuzz ffi" {
    try FuzzWrap(fuzz_ffi, 1 << 30).run();
}

fn fuzz_concat(data: []const u8, gpa: Allocator) !void {
    var input = FuzzInput{ .data = data };

    var arena = ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();
    const array_len = try input.int(u8);
    const array = try input.make_array(array_len, alloc);

    try validate.validate(&array);

    var concat_arena = ArenaAllocator.init(alloc);
    defer concat_arena.deinit();
    const concat_alloc = concat_arena.allocator();

    const slice0 = try input.slice_array(&array);
    try validate.validate(&slice0);
    const slice1 = try input.slice_array(&array);
    try validate.validate(&slice1);
    const slice2 = try input.slice_array(&array);
    try validate.validate(&slice2);

    const dt = try data_type.get_data_type(&array, concat_alloc);

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

test "fuzz concat" {
    try FuzzWrap(fuzz_concat, 1 << 30).run();
}

fn fuzz_check_dt(data: []const u8, gpa: Allocator) !void {
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

test "fuzz check_dt" {
    try FuzzWrap(fuzz_check_dt, 1 << 30).run();
}

fn FuzzWrap(comptime fuzz_one: fn (data: []const u8, gpa: Allocator) anyerror!void, comptime alloc_size: comptime_int) type {
    const FuzzContext = struct {
        fb_alloc: *FixedBufferAllocator,
    };

    return struct {
        fn run_one(ctx: FuzzContext, data: []const u8) anyerror!void {
            ctx.fb_alloc.reset();

            var general_purpose_allocator = std.heap.GeneralPurposeAllocator(.{
                .backing_allocator_zeroes = false,
            }){
                .backing_allocator = ctx.fb_alloc.allocator(),
            };
            const gpa = general_purpose_allocator.allocator();
            defer {
                switch (general_purpose_allocator.deinit()) {
                    .ok => {},
                    .leak => |l| {
                        std.debug.panic("LEAK: {any}", .{l});
                    },
                }
            }

            fuzz_one(data, gpa) catch |e| {
                if (e == error.ShortInput) return {} else return e;
            };
        }

        fn run() !void {
            var fb_alloc = FixedBufferAllocator.init(std.heap.page_allocator.alloc(u8, alloc_size) catch unreachable);
            try std.testing.fuzz(FuzzContext{
                .fb_alloc = &fb_alloc,
            }, run_one, .{});
        }
    };
}
