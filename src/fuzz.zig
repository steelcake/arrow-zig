const std = @import("std");
const FixedBufferAllocator = std.heap.FixedBufferAllocator;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const fuzzin = @import("fuzzin");
const FuzzInput = fuzzin.FuzzInput;
const LimitedAllocator = fuzzin.LimitedAllocator;

const nanoarrow_validate = @import("nanoarrow_validate").validate;

const arr = @import("./array.zig");
const length = @import("./length.zig");
const slice_array_impl = @import("./slice.zig").slice;
const data_type = @import("./data_type.zig");
const concat = @import("./concat.zig").concat;
const equals = @import("./equals.zig");
const validate = @import("./validate.zig");
const ffi = @import("./ffi.zig");
const minmax = @import("./minmax.zig");
const bitmap = @import("./bitmap.zig");
const null_count = @import("./null_count.zig");

const fuzz_input = @import("./fuzz_input.zig");

fn fuzz_null_count(arr_buf: []u8, input: *FuzzInput, dbg_alloc: Allocator) fuzzin.Error!void {
    _ = dbg_alloc;

    var fb_alloc = FixedBufferAllocator.init(arr_buf);
    var arena = ArenaAllocator.init(fb_alloc.allocator());
    const alloc = arena.allocator();

    const dt = try fuzz_input.data_type(input, alloc, 8);
    const len = try input.int(u8);

    const array = data_type.all_null_array(&dt, len, alloc) catch {
        // just go to the next cycle if we run out of memory
        return;
    };

    _ = null_count.null_count(&array);
}

test fuzz_null_count {
    const arr_buf = try std.heap.page_allocator.alloc(u8, 1 << 20);
    fuzzin.fuzz_test(
        []u8,
        arr_buf,
        fuzz_null_count,
        0,
    );
}

fn fuzz_all_null_array(ctx: void, input: *FuzzInput, dbg_alloc: Allocator) fuzzin.Error!void {
    _ = ctx;

    var arena = ArenaAllocator.init(dbg_alloc);
    defer arena.deinit();
    var limited_alloc = LimitedAllocator.init(arena.allocator(), 1 << 12);

    const dt = try fuzz_input.data_type(input, limited_alloc.allocator(), 8);
    const len = try input.int(u8);

    const array = data_type.all_null_array(&dt, len, arena.allocator()) catch {
        // just go to the next cycle if we run out of memory
        return;
    };

    validate.validate_array(&array) catch unreachable;
    data_type.check_data_type(&array, &dt) catch unreachable;

    std.debug.assert(length.length(&array) == len);
    const nc = null_count.null_count(&array);

    switch (dt) {
        .dict, .dense_union, .sparse_union, .run_end_encoded => {},
        else => {
            std.debug.assert(nc == len);
        },
    }
}

test fuzz_all_null_array {
    fuzzin.fuzz_test(
        void,
        {},
        fuzz_all_null_array,
        1 << 24,
    );
}

fn fuzz_empty_array(ctx: void, input: *FuzzInput, dbg_alloc: Allocator) fuzzin.Error!void {
    _ = ctx;

    var arena = ArenaAllocator.init(dbg_alloc);
    defer arena.deinit();
    var limited_alloc = LimitedAllocator.init(arena.allocator(), 1 << 15);

    const dt = try fuzz_input.data_type(input, limited_alloc.allocator(), 8);

    const array = data_type.empty_array(&dt, arena.allocator()) catch unreachable;
    validate.validate_array(&array) catch unreachable;
    data_type.check_data_type(&array, &dt) catch unreachable;

    std.debug.assert(length.length(&array) == 0);
}

test fuzz_empty_array {
    fuzzin.fuzz_test(
        void,
        {},
        fuzz_empty_array,
        1 << 20,
    );
}

fn fuzz_bitmap_copy(out: []u8, input: []const u8) anyerror!void {
    if (input.len < 3) {
        return;
    }

    const len: u32 = input[0];
    const input_offset: u32 = input[1];
    const output_offset: u32 = input[2];

    const input_num_bytes: u32 = (len + input_offset + 7) / 8;
    if (input.len < 3 + input_num_bytes) return;

    const output_num_bytes: u32 = (len + output_offset + 7) / 8;
    std.debug.assert(out.len >= output_num_bytes);

    const o = out[0..output_num_bytes];
    const i = input[3 .. 3 + input_num_bytes];

    bitmap.copy(len, o, output_offset, i, input_offset);

    var idx: u32 = 0;
    while (idx < len) : (idx += 1) {
        if (bitmap.get(i, input_offset + idx) != bitmap.get(o, output_offset + idx)) {
            std.debug.panic("{} {} {} {} {any} {any}", .{ idx, input_offset, output_offset, len, i, o });
        }
    }
}

test fuzz_bitmap_copy {
    const out = try std.heap.page_allocator.alloc(u8, 1 << 10);
    try std.testing.fuzz(out, fuzz_bitmap_copy, .{});
}

fn fuzz_bitmap_count_unset_bits(ctx: void, input: []const u8) anyerror!void {
    _ = ctx;

    if (input.len < 2) {
        return;
    }

    const len: u32 = input[0];
    const offset: u32 = input[1];

    const num_bytes: u32 = (len + offset + 7) / 8;

    if (input.len < 2 + num_bytes) return;

    const i = input[2 .. 2 + num_bytes];

    const n_nulls = bitmap.count_unset_bits(i, offset, len);

    var check: u32 = 0;
    var idx: u32 = offset;
    while (idx < offset + len) : (idx += 1) {
        if (!bitmap.get(i, idx)) {
            check += 1;
        }
    }

    std.debug.assert(check == n_nulls);
}

test fuzz_bitmap_count_unset_bits {
    try std.testing.fuzz({}, fuzz_bitmap_count_unset_bits, .{});
}

fn fuzz_bitmap_for_each(out: []u8, input: []const u8) anyerror!void {
    if (input.len < 2) {
        return;
    }

    const len: u32 = input[0];
    const offset: u32 = input[1];

    const num_bytes: u32 = (len + offset + 7) / 8;

    if (input.len < 2 + num_bytes) return;

    const o = out[0..num_bytes];
    const i = input[2 .. 2 + num_bytes];

    // o_O
    @memset(o, 0);

    const Closure = struct {
        fn process(outs: []u8, idx: u32) void {
            std.debug.assert(!bitmap.get(outs, idx));
            bitmap.set(outs, idx);
        }
    };

    bitmap.for_each([]u8, Closure.process, o, i, offset, len);

    var idx: u32 = offset;
    while (idx < offset + len) : (idx += 1) {
        if (bitmap.get(i, idx) != bitmap.get(o, idx)) {
            std.debug.panic("{} {} {} {any} {any}", .{ idx, offset, len, i, o });
        }
    }
}

test fuzz_bitmap_for_each {
    const out = try std.heap.page_allocator.alloc(u8, 1 << 10);
    try std.testing.fuzz(out, fuzz_bitmap_for_each, .{});
}

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
    var limited_alloc = LimitedAllocator.init(arena.allocator(), 1 << 12);
    const alloc = limited_alloc.allocator();

    const array_len = input.int(u8) catch |e| {
        arena.deinit();
        return e;
    };
    const dt = fuzz_input.data_type(input, alloc, 8) catch |e| {
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

    nanoarrow_validate(&ffi_array.array, &ffi_array.schema);

    var import_arena = ArenaAllocator.init(dbg_alloc);
    // don't free like this since we will ffi a second time
    // defer import_arena.deinit();
    const import_alloc = import_arena.allocator();

    const imported = ffi.import_array(&ffi_array, import_alloc) catch {
        std.debug.panic("failed to import array. ffi_array = {any}", .{ffi_array});
    };
    validate.validate_array(&imported) catch unreachable;

    equals.equals(&imported, &array);

    var ffi_array2 = ffi.export_array(.{
        .array = &imported,
        .arena = import_arena,
        .ffi_arr = ffi_array,
    }) catch unreachable;
    defer ffi_array2.release();

    nanoarrow_validate(&ffi_array2.array, &ffi_array2.schema);

    var import_arena2 = ArenaAllocator.init(dbg_alloc);
    defer import_arena2.deinit();
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
        fuzz_ffi,
        1 << 20,
    );
}

fn fuzz_concat(ctx: void, input: *FuzzInput, dbg_alloc: Allocator) fuzzin.Error!void {
    _ = ctx;

    var arena = ArenaAllocator.init(dbg_alloc);
    defer arena.deinit();
    var limited_alloc = LimitedAllocator.init(arena.allocator(), 1 << 12);
    const alloc = limited_alloc.allocator();

    const array_len = try input.int(u8);
    const dt = try fuzz_input.data_type(input, alloc, 8);
    const array = try fuzz_input.array(input, &dt, array_len, alloc);

    validate.validate_array(&array) catch unreachable;

    var concat_arena = ArenaAllocator.init(dbg_alloc);
    defer concat_arena.deinit();
    var concat_limited_alloc = LimitedAllocator.init(concat_arena.allocator(), 1 << 16);
    const concat_alloc = concat_limited_alloc.allocator();

    const slice0 = try fuzz_input.slice(input, &array);
    validate.validate_array(&slice0) catch unreachable;
    const slice1 = try fuzz_input.slice(input, &array);
    validate.validate_array(&slice1) catch unreachable;
    const slice2 = try fuzz_input.slice(input, &array);
    validate.validate_array(&slice2) catch unreachable;

    const concated = conc: {
        var scratch_arena = ArenaAllocator.init(dbg_alloc);
        defer scratch_arena.deinit();
        const scratch_alloc = scratch_arena.allocator();
        break :conc concat(dt, &.{ slice0, slice1, slice2 }, concat_alloc, scratch_alloc) catch unreachable;
    };
    validate.validate_array(&concated) catch unreachable;

    const slice0_len = length.length(&slice0);
    const slice1_len = length.length(&slice1);
    const slice2_len = length.length(&slice2);
    const slice0_out = slice_array_impl(&concated, 0, slice0_len);
    const slice1_out = slice_array_impl(&concated, slice0_len, slice1_len);
    const slice2_out = slice_array_impl(&concated, slice0_len + slice1_len, slice2_len);
    validate.validate_array(&slice0_out) catch unreachable;
    validate.validate_array(&slice1_out) catch unreachable;
    validate.validate_array(&slice2_out) catch unreachable;

    equals.equals(&slice0_out, &slice0);
    equals.equals(&slice1_out, &slice1);
    equals.equals(&slice2_out, &slice2);
}

test fuzz_concat {
    fuzzin.fuzz_test(
        void,
        {},
        fuzz_concat,
        1 << 22,
    );
}

fn fuzz_check_dt(arr_buf: []u8, input: *FuzzInput, dbg_alloc: Allocator) fuzzin.Error!void {
    _ = dbg_alloc;

    var fb_alloc = FixedBufferAllocator.init(arr_buf);
    var arena = ArenaAllocator.init(fb_alloc.allocator());
    const alloc = arena.allocator();

    const array_len = try input.int(u8);
    const dt = try fuzz_input.data_type(input, alloc, 8);
    const array = try fuzz_input.array(input, &dt, array_len, alloc);

    data_type.check_data_type(&array, &dt) catch unreachable;

    const other_dt = try fuzz_input.data_type(input, alloc, 8);

    data_type.check_data_type(&array, &other_dt) catch return;
    dt.eql(&other_dt) catch unreachable;
}

test fuzz_check_dt {
    const arr_buf = try std.heap.page_allocator.alloc(u8, 1 << 12);
    fuzzin.fuzz_test(
        []u8,
        arr_buf,
        fuzz_check_dt,
        0,
    );
}

fn fuzz_validate_array_auto(arr_buf: []u8, input: *FuzzInput, dbg_alloc: Allocator) fuzzin.Error!void {
    _ = dbg_alloc;

    var fb_alloc = FixedBufferAllocator.init(arr_buf);
    var arena = ArenaAllocator.init(fb_alloc.allocator());
    const alloc = arena.allocator();

    const array = try input.auto(arr.Array, alloc, 20);

    validate.validate_array(&array) catch return;

    var ffi_array = ffi.export_array(.{ .array = &array, .arena = arena }) catch return;
    defer ffi_array.release();

    nanoarrow_validate(&ffi_array.array, &ffi_array.schema);
}

test fuzz_validate_array_auto {
    const arr_buf = try std.heap.page_allocator.alloc(u8, 1 << 20);
    fuzzin.fuzz_test(
        []u8,
        arr_buf,
        fuzz_validate_array_auto,
        0,
    );
}

fn fuzz_validate_data_type_auto(dt_buf: []u8, input: *FuzzInput, dbg_alloc: Allocator) fuzzin.Error!void {
    _ = dbg_alloc;

    var fb_alloc = FixedBufferAllocator.init(dt_buf);
    var arena = ArenaAllocator.init(fb_alloc.allocator());
    const alloc = arena.allocator();

    const dt = try input.auto(data_type.DataType, alloc, 20);

    validate.validate_data_type(&dt) catch {};
}

test fuzz_validate_data_type_auto {
    const arr_buf = try std.heap.page_allocator.alloc(u8, 1 << 12);
    fuzzin.fuzz_test(
        []u8,
        arr_buf,
        fuzz_validate_data_type_auto,
        0,
    );
}
