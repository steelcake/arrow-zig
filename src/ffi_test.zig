const std = @import("std");
const ArenaAllocator = std.heap.ArenaAllocator;
const Allocator = std.mem.Allocator;
const testing = std.testing;
const Random = std.Random.DefaultPrng;

const ffi = @import("./ffi.zig");
const arr = @import("./array.zig");
const bitmap = @import("./bitmap.zig");

extern fn test_helper_roundtrip_array(input_array: *const anyopaque, input_schema: *const anyopaque, output_array: *anyopaque, output_schema: *anyopaque) i32;

fn run_test(array: *const arr.Array, arena: ArenaAllocator) !void {
    var input = ffi_export: {
        const x = try ffi.export_array(.{
            .array = array,
            .arena = arena,
        });
        errdefer arena.deinit();

        break :ffi_export x;
    };

    var output: ffi.FFI_Array = undefined;

    errdefer input.release();

    try testing.expectEqual(0, test_helper_roundtrip_array(&input.array, &input.schema, &output.array, &output.schema));

    defer output.release();
    var import_arena = ArenaAllocator.init(testing.allocator);
    defer import_arena.deinit();
    const import_alloc = import_arena.allocator();
    const imported = try ffi.import_array(&output, import_alloc);

    try testing.expectEqualDeep(array, &imported);
}

const offset = 3;
const len = 113;

fn make_validity(rand: *Random, allocator: Allocator) !struct { validity: []const u8, null_count: u32 } {
    const num_bytes = (len + 7) / 8;

    const validity = try allocator.alloc(u8, num_bytes);
    for (0..num_bytes) |i| {
        validity[i] = rand.random().int(u8);
    }

    var null_count: u32 = 0;
    for (offset..len) |i| {
        null_count += @intFromBool(bitmap.get(validity, @intCast(i)));
    }

    return .{ .null_count = null_count, .validity = validity };
}

fn make_bool(rand: *Random, allocator: Allocator) !arr.BoolArray {
    const num_bytes = (len + 7) / 8;

    const values = try allocator.alloc(u8, num_bytes);
    for (0..num_bytes) |i| {
        values[i] = rand.random().int(u8);
    }

    return .{ .len = len - offset, .offset = offset, .validity = null, .null_count = 0, .values = values };
}

fn make_primitive(comptime T: type, rand: *Random, allocator: Allocator) !arr.PrimitiveArr(T) {
    const values = try allocator.alloc(T, len);

    for (0..len) |i| {
        if (T == f16) {
            values[i] = @floatCast(rand.random().float(f32));
        } else {
            switch (@typeInfo(T)) {
                .int => values[i] = rand.random().int(T),
                .float => values[i] = rand.random().float(T),
                else => unreachable,
            }
        }
    }

    return .{
        .len = len - offset,
        .offset = offset,
        .validity = null,
        .null_count = 0,
        .values = values,
    };
}

fn make_binary(comptime index_type: arr.IndexType, rand: *Random, allocator: Allocator) !arr.BinaryArr(index_type) {
    const IndexT = index_type.to_type();

    const max_item_len = 7;

    const offsets = try allocator.alloc(IndexT, len + 1);
    offsets[0] = 0;
    const data = try allocator.alloc(u8, len * max_item_len);

    for (0..len) |i| {
        const item_len = rand.random().intRangeAtMost(IndexT, 0, max_item_len);
        offsets[i + 1] = offsets[i] + item_len;
        rand.random().bytes(data[@intCast(offsets[i])..@intCast(offsets[i + 1])]);
    }

    return arr.BinaryArr(index_type){
        .len = len - offset,
        .offset = offset,
        .validity = null,
        .null_count = 0,
        .data = data[@intCast(offsets[0])..@intCast(offsets[offsets.len - 1])],
        .offsets = offsets,
    };
}

fn make_primitive_with_validity(comptime T: type, rand: *Random, allocator: Allocator) !arr.PrimitiveArr(T) {
    var a = try make_primitive(T, rand, allocator);
    const v = try make_validity(rand, allocator);

    a.validity = v.validity;
    a.null_count = v.null_count;

    return a;
}

fn make_bool_with_validity(rand: *Random, allocator: Allocator) !arr.BoolArray {
    var a = try make_bool(rand, allocator);
    const v = try make_validity(rand, allocator);

    a.validity = v.validity;
    a.null_count = v.null_count;

    return a;
}

fn make_binary_with_validity(comptime index_type: arr.IndexType, rand: *Random, allocator: Allocator) !arr.BinaryArr(index_type) {
    var a = try make_binary(index_type, rand, allocator);
    const v = try make_validity(rand, allocator);

    a.validity = v.validity;
    a.null_count = v.null_count;

    return a;
}

fn test_primitive(comptime T: type, random: *Random) !void {
    var arena = ArenaAllocator.init(testing.allocator);

    const array = init: {
        const allocator = arena.allocator();
        errdefer arena.deinit();
        break :init try make_primitive_with_validity(T, random, allocator);
    };

    try run_test(&@unionInit(arr.Array, @typeName(T), array), arena);
}

fn test_decimal(comptime int: arr.DecimalInt, random: *Random) !void {
    var arena = ArenaAllocator.init(testing.allocator);

    const array = init: {
        const allocator = arena.allocator();
        errdefer arena.deinit();
        const inner = try make_primitive_with_validity(int.to_type(), random, allocator);

        break :init arr.DecimalArr(int){
            .inner = inner,
            .params = .{
                .precision = 6,
                .scale = -3,
            },
        };
    };

    try run_test(&@unionInit(arr.Array, "decimal" ++ @tagName(int)[1..], array), arena);
}

fn test_date(comptime backing_t: arr.IndexType, random: *Random) !void {
    var arena = ArenaAllocator.init(testing.allocator);

    const array = init: {
        const allocator = arena.allocator();
        errdefer arena.deinit();
        const inner = try make_primitive_with_validity(backing_t.to_type(), random, allocator);

        break :init arr.DateArr(backing_t){ .inner = inner };
    };

    try run_test(&@unionInit(arr.Array, "date" ++ @tagName(backing_t)[1..], array), arena);
}

fn test_time(comptime backing_t: arr.IndexType, random: *Random) !void {
    const units = comptime if (backing_t == .i32)
        [_]arr.Time32Unit{ .second, .millisecond }
    else
        [_]arr.Time64Unit{ .nanosecond, .microsecond };

    inline for (units) |unit| {
        var arena = ArenaAllocator.init(testing.allocator);

        const array = init: {
            const allocator = arena.allocator();
            errdefer arena.deinit();
            const inner = try make_primitive_with_validity(backing_t.to_type(), random, allocator);

            break :init arr.TimeArr(backing_t){ .inner = inner, .unit = unit };
        };

        try run_test(&@unionInit(arr.Array, "time" ++ @tagName(backing_t)[1..], array), arena);
    }
}

fn test_binary(comptime index_type: arr.IndexType, random: *Random) !void {
    var arena = ArenaAllocator.init(testing.allocator);

    const array = init: {
        const allocator = arena.allocator();
        errdefer arena.deinit();
        break :init try make_binary_with_validity(index_type, random, allocator);
    };

    const tagName = switch (index_type) {
        .i32 => "binary",
        .i64 => "large_binary",
    };

    try run_test(&@unionInit(arr.Array, tagName, array), arena);
}

fn test_utf8(comptime index_type: arr.IndexType, random: *Random) !void {
    var arena = ArenaAllocator.init(testing.allocator);

    const array = init: {
        const allocator = arena.allocator();
        errdefer arena.deinit();
        const inner = try make_binary_with_validity(index_type, random, allocator);
        break :init arr.Utf8Arr(index_type){ .inner = inner };
    };

    const tagName = switch (index_type) {
        .i32 => "utf8",
        .i64 => "large_utf8",
    };

    try run_test(&@unionInit(arr.Array, tagName, array), arena);
}

fn test_bool(random: *Random) !void {
    var arena = ArenaAllocator.init(testing.allocator);

    const array = init: {
        const allocator = arena.allocator();
        errdefer arena.deinit();
        break :init try make_bool_with_validity(random, allocator);
    };

    try run_test(&arr.Array{ .bool = array }, arena);
}

test "primitive roundtrip" {
    var rand = Random.init(69);

    inline for (&[_]type{ i8, i16, i32, i64, u8, u16, u32, u64, f16, f32, f64 }) |t| {
        try test_primitive(t, &rand);
    }
}

test "decimal roundtrip" {
    var rand = Random.init(69);

    inline for (&[_]arr.DecimalInt{
        // disable these because nanoarrow doesn't support them so test fails
        // .i32,
        // .i64,
        .i128,
        .i256,
    }) |int| {
        try test_decimal(int, &rand);
    }
}

test "date roundtrip" {
    var rand = Random.init(60);

    inline for (&[_]arr.IndexType{ .i32, .i64 }) |backing_t| {
        try test_date(backing_t, &rand);
    }
}

test "time roundtrip" {
    var rand = Random.init(69);

    inline for (&[_]arr.IndexType{ .i32, .i64 }) |backing_t| {
        try test_time(backing_t, &rand);
    }
}

test "binary roundtrip" {
    var rand = Random.init(1131);

    inline for (&[_]arr.IndexType{ .i32, .i64 }) |index_type| {
        try test_binary(index_type, &rand);
    }
}

test "utf8 roundtrip" {
    var rand = Random.init(1131);

    inline for (&[_]arr.IndexType{ .i32, .i64 }) |index_type| {
        try test_utf8(index_type, &rand);
    }
}

test "bool roundtrip" {
    var rand = Random.init(69);
    try test_bool(&rand);
}

test "null roundtrip" {
    const arena = ArenaAllocator.init(testing.allocator);
    try run_test(&arr.Array{ .null = arr.NullArray{ .len = 69 } }, arena);
}
