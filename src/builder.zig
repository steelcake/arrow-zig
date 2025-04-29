const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const ArenaAllocator = std.heap.ArenaAllocator;

const arr = @import("./array.zig");
const bitmap = @import("./bitmap.zig");

const Error = error{
    OutOfMemory,
    OutOfCapacity,
    NonNullable,
    LenCapacityMismatch,
};

pub const BoolBuilder = struct {
    values: []u8,
    validity: ?[]u8,
    null_count: u32,
    len: u32,
    capacity: u32,

    pub fn with_capacity(capacity: u32, nullable: bool, allocator: Allocator) Error!BoolBuilder {
        const num_bytes = (capacity + 7) / 8;

        const values = try allocator.alloc(u8, num_bytes);
        @memset(values, 0);

        var validity: ?[]u8 = null;
        if (nullable) {
            const v = try allocator.alloc(u8, num_bytes);
            @memset(v, 0);
            validity = v;
        }

        return BoolBuilder{
            .values = values,
            .validity = validity,
            .null_count = 0,
            .len = 0,
            .capacity = capacity,
        };
    }

    pub fn finish(self: BoolBuilder) Error!arr.BoolArray {
        std.debug.assert(self.validity != null or self.null_count == 0);

        if (self.capacity != self.len) {
            return Error.LenCapacityMismatch;
        }

        return arr.BoolArray{
            .len = self.len,
            .offset = 0,
            .validity = self.validity,
            .values = self.values,
            .null_count = self.null_count,
        };
    }

    pub fn append_option(self: *BoolBuilder, val: ?bool) Error!void {
        if (self.capacity == self.len) {
            return Error.OutOfCapacity;
        }

        const validity = self.validity orelse return Error.NonNullable;

        if (val) |b| {
            bitmap.set(validity.ptr, self.len);
            if (b) {
                bitmap.set(self.values.ptr, self.len);
            }
        } else {
            self.null_count += 1;
        }
        self.len += 1;
    }

    pub fn append_value(self: *BoolBuilder, val: bool) Error!void {
        if (self.capacity == self.len) {
            return Error.OutOfCapacity;
        }

        if (self.validity) |v| {
            bitmap.set(v.ptr, self.len);
        }

        if (val) {
            bitmap.set(self.values.ptr, self.len);
        }
        self.len += 1;
    }

    pub fn append_null(self: *BoolBuilder) Error!void {
        if (self.capacity == self.len) {
            return Error.OutOfCapacity;
        }
        if (self.validity == null) {
            return Error.NonNullable;
        }

        self.null_count += 1;
        self.len += 1;
    }
};

pub fn PrimitiveBuilder(comptime T: type) type {
    return struct {
        const Self = @This();

        values: []T,
        validity: ?[]u8,
        null_count: u32,
        len: u32,
        capacity: u32,

        pub fn with_capacity(capacity: u32, nullable: bool, allocator: Allocator) Error!Self {
            const values = try allocator.alloc(T, capacity);
            @memset(values, 0);

            const num_bytes = (capacity + 7) / 8;
            var validity: ?[]u8 = null;
            if (nullable) {
                const v = try allocator.alloc(u8, num_bytes);
                @memset(v, 0);
                validity = v;
            }

            return Self{
                .values = values,
                .validity = validity,
                .null_count = 0,
                .len = 0,
                .capacity = capacity,
            };
        }

        pub fn finish(self: Self) Error!arr.PrimitiveArr(T) {
            std.debug.assert(self.validity != null or self.null_count == 0);

            if (self.capacity != self.len) {
                return Error.LenCapacityMismatch;
            }

            return arr.PrimitiveArr(T){
                .len = self.len,
                .offset = 0,
                .validity = self.validity,
                .values = self.values,
                .null_count = self.null_count,
            };
        }

        pub fn append_option(self: *Self, val: ?T) Error!void {
            if (self.capacity == self.len) {
                return Error.OutOfCapacity;
            }

            const validity = self.validity orelse return Error.NonNullable;

            if (val) |v| {
                bitmap.set(validity.ptr, self.len);
                self.values.ptr[self.len] = v;
            } else {
                self.null_count += 1;
            }
            self.len += 1;
        }

        pub fn append_value(self: *Self, val: T) Error!void {
            if (self.capacity == self.len) {
                return Error.OutOfCapacity;
            }

            if (self.validity) |v| {
                bitmap.set(v.ptr, self.len);
            }

            self.values.ptr[self.len] = val;
            self.len += 1;
        }

        pub fn append_null(self: *Self) Error!void {
            if (self.capacity == self.len) {
                return Error.OutOfCapacity;
            }
            if (self.validity == null) {
                return Error.NonNullable;
            }

            self.null_count += 1;
            self.len += 1;
        }
    };
}

pub const UInt8Builder = PrimitiveBuilder(u8);
pub const UInt16Builder = PrimitiveBuilder(u16);
pub const UInt32Builder = PrimitiveBuilder(u32);
pub const UInt64Builder = PrimitiveBuilder(u64);
pub const Int8Builder = PrimitiveBuilder(i8);
pub const Int16Builder = PrimitiveBuilder(i16);
pub const Int32Builder = PrimitiveBuilder(i32);
pub const Int64Builder = PrimitiveBuilder(i64);
pub const Float16Builder = PrimitiveBuilder(f16);
pub const Float32Builder = PrimitiveBuilder(f32);
pub const Float64Builder = PrimitiveBuilder(f64);

test "bool nullable " {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const len = 10;

    var builder = try BoolBuilder.with_capacity(len, true, allocator);

    try builder.append_null();
    try builder.append_value(false);
    try builder.append_value(true);
    try builder.append_option(null);
    try builder.append_option(true);
    try builder.append_option(false);

    try testing.expectEqual(Error.LenCapacityMismatch, builder.finish());

    for (0..4) |_| {
        try builder.append_null();
    }

    try testing.expectEqual(Error.OutOfCapacity, builder.append_null());
    try testing.expectEqual(Error.OutOfCapacity, builder.append_value(false));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_option(null));

    const array = try builder.finish();

    try testing.expectEqual(len, array.len);
    try testing.expectEqual(6, array.null_count);
    try testing.expectEqual(0, array.offset);
    try testing.expectEqualDeep(&[_]u8{ 0b00010100, 0 }, array.values);
    try testing.expectEqualDeep(&[_]u8{ 0b00110110, 0 }, array.validity.?);
}

test "bool non-nullable" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const len = 10;

    var builder = try BoolBuilder.with_capacity(len, false, allocator);

    try testing.expectEqual(Error.NonNullable, builder.append_null());
    try testing.expectEqual(Error.NonNullable, builder.append_option(null));

    try builder.append_value(false);
    try builder.append_value(true);

    try testing.expectEqual(Error.LenCapacityMismatch, builder.finish());

    for (0..8) |_| {
        try builder.append_value(false);
    }

    try testing.expectEqual(Error.OutOfCapacity, builder.append_value(false));

    const array = try builder.finish();

    try testing.expectEqual(len, array.len);
    try testing.expectEqual(0, array.null_count);
    try testing.expectEqual(0, array.offset);
    try testing.expectEqualDeep(&[_]u8{ 0b00000010, 0 }, array.values);
    try testing.expectEqual(null, array.validity);
}

test "primitive nullable" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const len = 10;

    var builder = try PrimitiveBuilder(i64).with_capacity(len, true, allocator);

    try builder.append_null();
    try builder.append_value(69);
    try builder.append_value(31);
    try builder.append_option(null);
    try builder.append_option(1131);
    try builder.append_option(11);

    try testing.expectEqual(Error.LenCapacityMismatch, builder.finish());

    for (0..4) |_| {
        try builder.append_null();
    }

    try testing.expectEqual(Error.OutOfCapacity, builder.append_null());
    try testing.expectEqual(Error.OutOfCapacity, builder.append_value(69));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_option(null));

    const array = try builder.finish();

    try testing.expectEqual(len, array.len);
    try testing.expectEqual(6, array.null_count);
    try testing.expectEqual(0, array.offset);
    try testing.expectEqualDeep(&[_]i64{ 0, 69, 31, 0, 1131, 11, 0, 0, 0, 0 }, array.values);
    try testing.expectEqualDeep(&[_]u8{ 0b00110110, 0 }, array.validity.?);
}

test "primitive non-nullable" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const len = 10;

    var builder = try PrimitiveBuilder(u32).with_capacity(len, false, allocator);

    try testing.expectEqual(Error.NonNullable, builder.append_null());
    try testing.expectEqual(Error.NonNullable, builder.append_option(null));

    try builder.append_value(31);
    try builder.append_value(69);

    try testing.expectEqual(Error.LenCapacityMismatch, builder.finish());

    for (0..8) |_| {
        try builder.append_value(12);
    }

    try testing.expectEqual(Error.OutOfCapacity, builder.append_value(1131));

    const array = try builder.finish();

    try testing.expectEqual(len, array.len);
    try testing.expectEqual(0, array.null_count);
    try testing.expectEqual(0, array.offset);
    try testing.expectEqualDeep(&[_]u32{ 31, 69, 12, 12, 12, 12, 12, 12, 12, 12 }, array.values);
    try testing.expectEqual(null, array.validity);
}
