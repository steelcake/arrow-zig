const std = @import("std");
const FixedBufferAllocator = std.heap.FixedBufferAllocator;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const fuzzin = @import("fuzzin");
const FuzzInput = fuzzin.FuzzInput;
const LimitedAllocator = fuzzin.LimitedAllocator;

const arr = @import("./array.zig");
const length = @import("./length.zig");
const slice_array_impl = @import("./slice.zig").slice;
const data_type = @import("./data_type.zig");
const concat = @import("./concat.zig").concat;
const equals = @import("./equals.zig");
const validate = @import("./validate.zig");
const ffi = @import("./ffi.zig");
const minmax = @import("./minmax.zig");

const fuzz_input = @import("./fuzz_input.zig");

fn fuzz_minmax(arr_buf: []u8, input: *FuzzInput, dbg_alloc: Allocator) !void {
    _ = dbg_alloc;

    var fb_alloc = FixedBufferAllocator.init(arr_buf);
    const alloc = fb_alloc.allocator();

    const array_len = try input.int(u8);

    const dt = try fuzz_input.data_type_flat(input);
    const array = try fuzz_input.array(input, &dt, array_len, alloc);

    const min_result = minmax.min(&array) catch |e| {
        switch (e) {
            // keep cycling if the array can't be minmaxed
            minmax.Error.ArrayTypeNotSupported => return,
        }
    };

    minmax.check_min(&array, min_result);
    const max_result = minmax.max(&array) catch unreachable;
    minmax.check_max(&array, max_result);
}

test fuzz_minmax {
    const arr_buf = try std.heap.page_allocator.alloc(u8, 1 << 12);
    fuzzin.fuzz_test(
        []u8,
        arr_buf,
        fuzz_minmax,
        0,
    );
}

fn fuzz_ffi(ctx: void, input: *FuzzInput, dbg_alloc: Allocator) !void {
    _ = ctx;

    var arena = ArenaAllocator.init(dbg_alloc);
    var limited_alloc = LimitedAllocator.init(arena.allocator(), 1 << 19);
    const alloc = limited_alloc.allocator();

    const array_len = input.int(u8) catch |e| {
        arena.deinit();
        return e;
    };
    const dt = fuzz_input.data_type(input, alloc, 16) catch |e| {
        arena.deinit();
        return e;
    };
    const array = fuzz_input.array(input, &dt, array_len, alloc) catch |e| {
        arena.deinit();
        return e;
    };

    // don't free the arena after we reach this point
    //  because ffi.export_array takes ownership of it
    //  even if there is an error
    var ffi_array = ffi.export_array(.{ .array = &array, .arena = arena }) catch unreachable;
    defer ffi_array.release();

    var import_arena = ArenaAllocator.init(dbg_alloc);
    const import_alloc = import_arena.allocator();

    const imported = ffi.import_array(&ffi_array, import_alloc) catch unreachable;
    validate.validate_array(&imported) catch unreachable;

    equals.equals(&imported, &array);

    var ffi_array2 = ffi.export_array(.{
        .array = &imported,
        .arena = import_arena,
        .ffi_arr = ffi_array,
    }) catch unreachable;
    defer ffi_array2.release();

    var import_arena2 = ArenaAllocator.init(dbg_alloc);
    const import_alloc2 = import_arena2.allocator();

    const imported2 = ffi.import_array(&ffi_array2, import_alloc2) catch unreachable;
    validate.validate_array(&imported2) catch unreachable;

    equals.equals(&imported2, &array);
    equals.equals(&imported2, &imported);
}

test fuzz_ffi {
    fuzzin.fuzz_test(
        void,
        {},
        fuzz_minmax,
        0,
    );
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
