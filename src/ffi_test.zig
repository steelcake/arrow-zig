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
const len = 1024;

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

fn make_primitive(comptime T: type, rand: *Random, allocator: Allocator) !arr.PrimitiveArr(T) {
    const values = try allocator.alloc(T, len);

    for (0..len) |i| {
        values[i] = rand.random().int(T);
    }

    return .{
        .len = len - offset,
        .offset = offset,
        .validity = null,
        .null_count = 0,
        .values = values,
    };
}

fn make_primitive_with_validity(comptime T: type, rand: *Random, allocator: Allocator) !arr.PrimitiveArr(T) {
    var a = try make_primitive(T, rand, allocator);
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

test "primitive roundtrip" {
    var rand = Random.init(69);

    inline for (&[_]type{ i8, i16, i32, i64, u8, u16, u32, u64 }) |t| {
        try test_primitive(t, &rand);
    }

    inline for (&[_]arr.DecimalInt{
        // .i32,
        // .i64,
        .i128,
        .i256,
    }) |int| {
        try test_decimal(int, &rand);
    }
}
