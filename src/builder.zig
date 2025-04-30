const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const ArenaAllocator = std.heap.ArenaAllocator;

const arr = @import("./array.zig");
const bitmap = @import("./bitmap.zig");
const length = @import("./length.zig").length;

const Error = error{
    OutOfMemory,
    OutOfCapacity,
    NonNullable,
    LenCapacityMismatch,
    InvalidSliceLength,
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
        if (val) |v| {
            try self.append_value(v);
        } else {
            try self.append_null();
        }
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

fn PrimitiveBuilder(comptime T: type) type {
    return struct {
        const Self = @This();

        values: []T,
        validity: ?[]u8,
        null_count: u32,
        len: u32,

        pub fn with_capacity(capacity: u32, nullable: bool, allocator: Allocator) Error!Self {
            const values = try allocator.alloc(T, capacity);
            @memset(@as([]u8, @ptrCast(values)), 0);

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
            };
        }

        pub fn finish(self: Self) Error!arr.PrimitiveArray(T) {
            std.debug.assert(self.validity != null or self.null_count == 0);

            if (self.values.len != self.len) {
                return Error.LenCapacityMismatch;
            }

            return arr.PrimitiveArray(T){
                .len = self.len,
                .offset = 0,
                .validity = self.validity,
                .values = self.values,
                .null_count = self.null_count,
            };
        }

        pub fn append_option(self: *Self, val: ?T) Error!void {
            if (val) |v| {
                try self.append_value(v);
            } else {
                try self.append_null();
            }
        }

        pub fn append_value(self: *Self, val: T) Error!void {
            if (self.values.len == self.len) {
                return Error.OutOfCapacity;
            }

            if (self.validity) |v| {
                bitmap.set(v.ptr, self.len);
            }

            self.values.ptr[self.len] = val;
            self.len += 1;
        }

        pub fn append_null(self: *Self) Error!void {
            if (self.values.len == self.len) {
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

pub const FixedSizeBinaryBuilder = struct {
    const Self = FixedSizeBinaryBuilder;

    data: []u8,
    validity: ?[]u8,
    null_count: u32,
    len: u32,
    capacity: u32,
    byte_width: u32,

    pub fn with_capacity(byte_width: u32, capacity: u32, nullable: bool, allocator: Allocator) Error!Self {
        const data = try allocator.alloc(u8, byte_width * capacity);
        @memset(data, 0);

        const num_bytes = (capacity + 7) / 8;
        var validity: ?[]u8 = null;
        if (nullable) {
            const v = try allocator.alloc(u8, num_bytes);
            @memset(v, 0);
            validity = v;
        }

        return Self{
            .data = data,
            .validity = validity,
            .null_count = 0,
            .len = 0,
            .capacity = capacity,
            .byte_width = byte_width,
        };
    }

    pub fn finish(self: Self) Error!arr.FixedSizeBinaryArray {
        std.debug.assert(self.validity != null or self.null_count == 0);

        if (self.capacity != self.len) {
            return Error.LenCapacityMismatch;
        }

        return arr.FixedSizeBinaryArray{
            .len = self.len,
            .offset = 0,
            .validity = self.validity,
            .data = self.data,
            .null_count = self.null_count,
            .byte_width = self.byte_width,
        };
    }

    pub fn append_option(self: *Self, val: ?[]const u8) Error!void {
        if (val) |v| {
            try self.append_value(v);
        } else {
            try self.append_null();
        }
    }

    pub fn append_value(self: *Self, val: []const u8) Error!void {
        if (self.capacity == self.len) {
            return Error.OutOfCapacity;
        }
        if (val.len != self.byte_width) {
            return Error.InvalidSliceLength;
        }

        if (self.validity) |v| {
            bitmap.set(v.ptr, self.len);
        }

        @memcpy(self.data[self.byte_width * self.len ..].ptr, val);
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

pub fn DecimalBuilder(comptime int: arr.DecimalInt) type {
    return struct {
        const Self = @This();
        const T = int.to_type();

        inner: PrimitiveBuilder(T),
        params: arr.DecimalParams,

        pub fn with_capacity(params: arr.DecimalParams, capacity: u32, nullable: bool, allocator: Allocator) Error!Self {
            return Self{
                .inner = try PrimitiveBuilder(T).with_capacity(capacity, nullable, allocator),
                .params = params,
            };
        }

        pub fn finish(self: Self) Error!arr.DecimalArray(int) {
            return arr.DecimalArray(int){
                .inner = try self.inner.finish(),
                .params = self.params,
            };
        }

        pub fn append_option(self: *Self, val: ?T) Error!void {
            try self.inner.append_option(val);
        }

        pub fn append_value(self: *Self, val: T) Error!void {
            try self.inner.append_value(val);
        }

        pub fn append_null(self: *Self) Error!void {
            try self.inner.append_null();
        }
    };
}

pub const Decimal32Builder = DecimalBuilder(.i32);
pub const Decimal64Builder = DecimalBuilder(.i64);
pub const Decimal128Builder = DecimalBuilder(.i128);
pub const Decimal256Builder = DecimalBuilder(.i256);

pub fn GenericBinaryBuilder(comptime index_type: arr.IndexType) type {
    return struct {
        const Self = @This();
        const I = index_type.to_type();

        data: []u8,
        offsets: []I,
        validity: ?[]u8,
        null_count: u32,
        len: u32,
        data_len: u32,
        capacity: u32,

        pub fn with_capacity(data_capacity: u32, capacity: u32, nullable: bool, allocator: Allocator) Error!Self {
            const data = try allocator.alloc(u8, data_capacity);
            @memset(data, 0);

            const num_bytes = (capacity + 7) / 8;
            var validity: ?[]u8 = null;
            if (nullable) {
                const v = try allocator.alloc(u8, num_bytes);
                @memset(v, 0);
                validity = v;
            }

            const offsets = try allocator.alloc(I, capacity + 1);
            @memset(offsets, 0);

            return Self{
                .data = data,
                .validity = validity,
                .null_count = 0,
                .len = 0,
                .data_len = 0,
                .capacity = capacity,
                .offsets = offsets,
            };
        }

        pub fn finish(self: Self) Error!arr.GenericBinaryArray(index_type) {
            std.debug.assert(self.validity != null or self.null_count == 0);

            if (self.capacity != self.len) {
                return Error.LenCapacityMismatch;
            }
            if (self.data.len != self.data_len) {
                return Error.LenCapacityMismatch;
            }

            return arr.GenericBinaryArray(index_type){
                .len = self.len,
                .offset = 0,
                .validity = self.validity,
                .data = self.data,
                .null_count = self.null_count,
                .offsets = self.offsets,
            };
        }

        pub fn append_option(self: *Self, val: ?[]const u8) Error!void {
            if (val) |v| {
                try self.append_value(v);
            } else {
                try self.append_null();
            }
        }

        pub fn append_value(self: *Self, val: []const u8) Error!void {
            if (self.capacity == self.len) {
                return Error.OutOfCapacity;
            }
            if (self.data.len < self.data_len + val.len) {
                return Error.OutOfCapacity;
            }
            if (self.validity) |v| {
                bitmap.set(v.ptr, self.len);
            }

            @memcpy(self.data[self.data_len..].ptr, val);
            self.data_len += @intCast(val.len);
            self.len += 1;
            self.offsets[self.len] = @intCast(self.data_len);
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
            self.offsets[self.len] = @intCast(self.data_len);
        }
    };
}

pub const BinaryBuilder = GenericBinaryBuilder(.i32);
pub const LargeBinaryBuilder = GenericBinaryBuilder(.i64);

pub fn GenericUtf8Builder(comptime index_type: arr.IndexType) type {
    return struct {
        const Self = @This();

        inner: GenericBinaryBuilder(index_type),

        pub fn with_capacity(data_capacity: u32, capacity: u32, nullable: bool, allocator: Allocator) Error!Self {
            return Self{
                .inner = try GenericBinaryBuilder(index_type).with_capacity(data_capacity, capacity, nullable, allocator),
            };
        }

        pub fn finish(self: Self) Error!arr.GenericUtf8Array(index_type) {
            return arr.GenericUtf8Array(index_type){
                .inner = try self.inner.finish(),
            };
        }

        pub fn append_option(self: *Self, val: ?[]const u8) Error!void {
            try self.inner.append_option(val);
        }

        pub fn append_value(self: *Self, val: []const u8) Error!void {
            try self.inner.append_value(val);
        }

        pub fn append_null(self: *Self) Error!void {
            try self.inner.append_null();
        }
    };
}

pub const Utf8Builder = GenericUtf8Builder(.i32);
pub const LargeUtf8Builder = GenericUtf8Builder(.i64);

pub fn DateBuilder(comptime backing_t: arr.IndexType) type {
    return struct {
        const Self = @This();
        const T = backing_t.to_type();

        inner: PrimitiveBuilder(T),

        pub fn with_capacity(capacity: u32, nullable: bool, allocator: Allocator) Error!Self {
            return Self{
                .inner = try PrimitiveBuilder(T).with_capacity(capacity, nullable, allocator),
            };
        }

        pub fn finish(self: Self) Error!arr.DateArray(backing_t) {
            return arr.DateArray(backing_t){
                .inner = try self.inner.finish(),
            };
        }

        pub fn append_option(self: *Self, val: ?T) Error!void {
            try self.inner.append_option(val);
        }

        pub fn append_value(self: *Self, val: T) Error!void {
            try self.inner.append_value(val);
        }

        pub fn append_null(self: *Self) Error!void {
            try self.inner.append_null();
        }
    };
}

pub const Date32Builder = DateBuilder(.i32);
pub const Date64Builder = DateBuilder(.i64);

pub fn TimeBuilder(comptime backing_t: arr.IndexType) type {
    return struct {
        const Self = @This();
        const Inner = arr.TimeArray(backing_t);
        const T = backing_t.to_type();

        inner: PrimitiveBuilder(T),
        unit: Inner.Unit,

        pub fn with_capacity(unit: Inner.Unit, capacity: u32, nullable: bool, allocator: Allocator) Error!Self {
            return Self{
                .inner = try PrimitiveBuilder(T).with_capacity(capacity, nullable, allocator),
                .unit = unit,
            };
        }

        pub fn finish(self: Self) Error!Inner {
            return Inner{
                .inner = try self.inner.finish(),
                .unit = self.unit,
            };
        }

        pub fn append_option(self: *Self, val: ?T) Error!void {
            try self.inner.append_option(val);
        }

        pub fn append_value(self: *Self, val: T) Error!void {
            try self.inner.append_value(val);
        }

        pub fn append_null(self: *Self) Error!void {
            try self.inner.append_null();
        }
    };
}

pub const Time32Builder = TimeBuilder(.i32);
pub const Time64Builder = TimeBuilder(.i64);

pub const TimestampBuilder = struct {
    const Self = @This();

    inner: Int64Builder,
    ts: arr.Timestamp,

    pub fn with_capacity(ts: arr.Timestamp, capacity: u32, nullable: bool, allocator: Allocator) Error!Self {
        return Self{
            .inner = try Int64Builder.with_capacity(capacity, nullable, allocator),
            .ts = ts,
        };
    }

    pub fn finish(self: Self) Error!arr.TimestampArray {
        return arr.TimestampArray{
            .inner = try self.inner.finish(),
            .ts = self.ts,
        };
    }

    pub fn append_option(self: *Self, val: ?i64) Error!void {
        try self.inner.append_option(val);
    }

    pub fn append_value(self: *Self, val: i64) Error!void {
        try self.inner.append_value(val);
    }

    pub fn append_null(self: *Self) Error!void {
        try self.inner.append_null();
    }
};

pub fn IntervalBuilder(comptime interval_type: arr.IntervalType) type {
    return struct {
        const Self = @This();

        const T = interval_type.to_type();

        inner: PrimitiveBuilder(T),

        pub fn with_capacity(capacity: u32, nullable: bool, allocator: Allocator) Error!Self {
            return Self{
                .inner = try PrimitiveBuilder(T).with_capacity(capacity, nullable, allocator),
            };
        }

        pub fn finish(self: Self) Error!arr.IntervalArray(interval_type) {
            return arr.IntervalArray(interval_type){
                .inner = try self.inner.finish(),
            };
        }

        pub fn append_option(self: *Self, val: ?T) Error!void {
            try self.inner.append_option(val);
        }

        pub fn append_value(self: *Self, val: T) Error!void {
            try self.inner.append_value(val);
        }

        pub fn append_null(self: *Self) Error!void {
            try self.inner.append_null();
        }
    };
}

pub const IntervalDayTimeBuilder = IntervalBuilder(.day_time);
pub const IntervalMonthDayNanoBuilder = IntervalBuilder(.month_day_nano);
pub const IntervalYearMonthBuilder = IntervalBuilder(.year_month);

pub const DurationBuilder = struct {
    const Self = @This();

    inner: Int64Builder,
    unit: arr.TimestampUnit,

    pub fn with_capacity(unit: arr.TimestampUnit, capacity: u32, nullable: bool, allocator: Allocator) Error!Self {
        return Self{
            .inner = try Int64Builder.with_capacity(capacity, nullable, allocator),
            .unit = unit,
        };
    }

    pub fn finish(self: Self) Error!arr.DurationArray {
        return arr.DurationArray{
            .inner = try self.inner.finish(),
            .unit = self.unit,
        };
    }

    pub fn append_option(self: *Self, val: ?i64) Error!void {
        try self.inner.append_option(val);
    }

    pub fn append_value(self: *Self, val: i64) Error!void {
        try self.inner.append_value(val);
    }

    pub fn append_null(self: *Self) Error!void {
        try self.inner.append_null();
    }
};

pub const BinaryViewBuilder = struct {
    const Self = @This();

    buffers: [][*]const u8,
    buffer: []u8,
    buffer_len: u32,
    validity: ?[]u8,
    null_count: u32,
    views: []arr.BinaryView,
    len: u32,

    pub fn with_capacity(buffer_capacity: u32, capacity: u32, nullable: bool, allocator: Allocator) Error!Self {
        const buffer = try allocator.alloc(u8, buffer_capacity);
        @memset(buffer, 0);

        const buffers = try allocator.alloc([*]const u8, 1);
        buffers[0] = &.{};

        const num_bytes = (capacity + 7) / 8;
        var validity: ?[]u8 = null;
        if (nullable) {
            const v = try allocator.alloc(u8, num_bytes);
            @memset(v, 0);
            validity = v;
        }

        const views = try allocator.alloc(arr.BinaryView, capacity);
        @memset(@as([]u8, @ptrCast(views)), 0);

        return Self{
            .buffers = buffers,
            .buffer = buffer,
            .buffer_len = 0,
            .validity = validity,
            .null_count = 0,
            .views = views,
            .len = 0,
        };
    }

    pub fn finish(self: Self) Error!arr.BinaryViewArray {
        std.debug.assert(self.validity != null or self.null_count == 0);

        if (self.views.len != self.len) {
            return Error.LenCapacityMismatch;
        }
        if (self.buffer.len != self.buffer_len) {
            return Error.LenCapacityMismatch;
        }

        self.buffers[0] = self.buffer.ptr;

        return arr.BinaryViewArray{
            .views = self.views,
            .buffers = self.buffers,
            .len = self.len,
            .offset = 0,
            .validity = self.validity,
            .null_count = self.null_count,
        };
    }

    pub fn append_option(self: *Self, val: ?[]const u8) Error!void {
        if (val) |v| {
            try self.append_value(v);
        } else {
            try self.append_null();
        }
    }

    pub fn append_value(self: *Self, val: []const u8) Error!void {
        if (self.views.len == self.len) {
            return Error.OutOfCapacity;
        }
        if (self.validity) |v| {
            bitmap.set(v.ptr, self.len);
        }

        const len: u32 = @intCast(val.len);

        if (val.len <= 12) {
            var data: [12]u8 = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
            for (0..val.len) |i| {
                data[i] = val[i];
            }
            const view = arr.BinaryViewShort{
                .length = len,
                .data = data,
            };
            self.views[self.len] = @bitCast(view);
        } else {
            if (len + self.buffer_len > self.buffer.len) {
                return Error.OutOfCapacity;
            }

            const view = arr.BinaryView{
                .length = len,
                .prefix = .{ val.ptr[0], val.ptr[1], val.ptr[2], val.ptr[3] },
                .buffer_idx = 0,
                .offset = self.buffer_len,
            };

            @memcpy(self.buffer[self.buffer_len..].ptr, val);

            self.buffer_len += len;
            self.views[self.len] = view;
        }

        self.len += 1;
    }

    pub fn append_null(self: *Self) Error!void {
        if (self.views.len == self.len) {
            return Error.OutOfCapacity;
        }
        if (self.validity == null) {
            return Error.NonNullable;
        }

        self.null_count += 1;

        self.views[self.len] = arr.BinaryView{
            .length = 0,
            .prefix = .{ 0, 0, 0, 0 },
            .buffer_idx = 0,
            .offset = 0,
        };

        self.len += 1;
    }
};

test "bool empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var builder = try BoolBuilder.with_capacity(0, true, allocator);

    try testing.expectEqual(Error.OutOfCapacity, builder.append_null());
    try testing.expectEqual(Error.OutOfCapacity, builder.append_value(false));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_option(null));

    const array = try builder.finish();
    try testing.expectEqual(0, array.len);
    try testing.expectEqual(0, array.null_count);
    try testing.expectEqual(0, array.offset);
    try testing.expectEqualDeep(&[_]u8{}, array.values);
    try testing.expectEqualDeep(&[_]u8{}, array.validity.?);
}

test "bool nullable" {
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
    try builder.append_option(true);

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

test "primitive empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var builder = try PrimitiveBuilder(i16).with_capacity(0, true, allocator);

    try testing.expectEqual(Error.OutOfCapacity, builder.append_null());
    try testing.expectEqual(Error.OutOfCapacity, builder.append_value(-69));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_option(null));

    const array = try builder.finish();
    try testing.expectEqual(0, array.len);
    try testing.expectEqual(0, array.null_count);
    try testing.expectEqual(0, array.offset);
    try testing.expectEqualDeep(&[_]i16{}, array.values);
    try testing.expectEqualDeep(&[_]u8{}, array.validity.?);
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
    try builder.append_option(69);

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

test "fixed-size-binary empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var builder = try FixedSizeBinaryBuilder.with_capacity(11, 0, true, allocator);

    try testing.expectEqual(Error.OutOfCapacity, builder.append_null());
    try testing.expectEqual(Error.OutOfCapacity, builder.append_value("12312312312"));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_option(null));

    const array = try builder.finish();
    try testing.expectEqual(0, array.len);
    try testing.expectEqual(0, array.null_count);
    try testing.expectEqual(0, array.offset);
    try testing.expectEqualDeep(&[_]u8{}, array.data);
    try testing.expectEqualDeep(&[_]u8{}, array.validity.?);
    try testing.expectEqual(11, array.byte_width);
}

test "fixed-size-binary nullable" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const len = 10;
    const byte_width = 3;

    var builder = try FixedSizeBinaryBuilder.with_capacity(byte_width, len, true, allocator);

    try testing.expectEqual(Error.InvalidSliceLength, builder.append_value("1131"));
    try testing.expectEqual(Error.InvalidSliceLength, builder.append_option("1131"));

    try builder.append_null();
    try builder.append_value("asd");
    try builder.append_value("qwe");
    try builder.append_option(null);
    try builder.append_option("ppp");
    try builder.append_option("xyz");

    try testing.expectEqual(Error.LenCapacityMismatch, builder.finish());

    for (0..4) |_| {
        try builder.append_null();
    }

    try testing.expectEqual(Error.OutOfCapacity, builder.append_null());
    try testing.expectEqual(Error.OutOfCapacity, builder.append_value("asd"));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_option(null));

    const array = try builder.finish();

    try testing.expectEqual(len, array.len);
    try testing.expectEqual(6, array.null_count);
    try testing.expectEqual(0, array.offset);
    try testing.expectEqualDeep(&[_]u8{
        0,   0,   0,
        'a', 's', 'd',
        'q', 'w', 'e',
        0,   0,   0,
        'p', 'p', 'p',
        'x', 'y', 'z',
        0,   0,   0,
        0,   0,   0,
        0,   0,   0,
        0,   0,   0,
    }, array.data);
    try testing.expectEqualDeep(&[_]u8{ 0b00110110, 0 }, array.validity.?);
    try testing.expectEqual(byte_width, array.byte_width);
}

test "fixed-size-binary non-nullable" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const len = 10;
    const byte_width = 3;

    var builder = try FixedSizeBinaryBuilder.with_capacity(byte_width, len, false, allocator);

    try testing.expectEqual(Error.InvalidSliceLength, builder.append_value("1131"));

    try testing.expectEqual(Error.NonNullable, builder.append_null());
    try testing.expectEqual(Error.NonNullable, builder.append_option(null));

    try builder.append_value("asd");
    try builder.append_option("qwe");

    try testing.expectEqual(Error.LenCapacityMismatch, builder.finish());

    for (0..8) |_| {
        try builder.append_value("xyz");
    }

    try testing.expectEqual(Error.OutOfCapacity, builder.append_value("691"));

    const array = try builder.finish();

    try testing.expectEqual(len, array.len);
    try testing.expectEqual(0, array.null_count);
    try testing.expectEqual(0, array.offset);
    try testing.expectEqualDeep(&[_]u8{
        'a', 's', 'd',
        'q', 'w', 'e',
        'x', 'y', 'z',
        'x', 'y', 'z',
        'x', 'y', 'z',
        'x', 'y', 'z',
        'x', 'y', 'z',
        'x', 'y', 'z',
        'x', 'y', 'z',
        'x', 'y', 'z',
    }, array.data);
    try testing.expectEqual(null, array.validity);
    try testing.expectEqual(byte_width, array.byte_width);
}

test "decimal smoke" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var builder = try DecimalBuilder(.i128).with_capacity(.{ .precision = 69, .scale = 31 }, 0, true, allocator);

    try testing.expectEqual(Error.OutOfCapacity, builder.append_null());
    try testing.expectEqual(Error.OutOfCapacity, builder.append_value(12312312312));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_option(null));

    const array = try builder.finish();
    _ = array;
}

fn test_binary_empty(comptime index_type: arr.IndexType) !void {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var builder = try GenericBinaryBuilder(index_type).with_capacity(0, 0, true, allocator);

    try testing.expectEqual(Error.OutOfCapacity, builder.append_null());
    try testing.expectEqual(Error.OutOfCapacity, builder.append_value("12312312312"));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_option(null));

    const array = try builder.finish();
    try testing.expectEqual(0, array.len);
    try testing.expectEqual(0, array.null_count);
    try testing.expectEqual(0, array.offset);
    try testing.expectEqualDeep(&[_]u8{}, array.data);
    try testing.expectEqualDeep(&[_]u8{}, array.validity.?);
    try testing.expectEqualDeep(&[_]index_type.to_type(){0}, array.offsets);
}

test "binary empty" {
    try test_binary_empty(.i32);
    try test_binary_empty(.i64);
}

fn test_binary_nullable(comptime index_type: arr.IndexType) !void {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const len = 10;
    const data_capacity = 30;

    var builder = try GenericBinaryBuilder(index_type).with_capacity(data_capacity, len, true, allocator);

    try builder.append_null();
    try builder.append_value("asd");
    try builder.append_value("");
    try builder.append_option(null);
    try builder.append_option("pppqwe");
    try builder.append_option("xyz");

    try testing.expectEqual(Error.LenCapacityMismatch, builder.finish());

    try testing.expectEqual(Error.OutOfCapacity, builder.append_value("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));

    try builder.append_option("");
    try builder.append_value("qweqweqweqweqweqwe");
    try builder.append_option("");
    try builder.append_null();

    try testing.expectEqual(Error.OutOfCapacity, builder.append_null());
    try testing.expectEqual(Error.OutOfCapacity, builder.append_value("asd"));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_option(null));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_option(""));

    const array = try builder.finish();

    try testing.expectEqual(len, array.len);
    try testing.expectEqual(3, array.null_count);
    try testing.expectEqual(0, array.offset);
    try testing.expectEqualDeep(
        "asdpppqwexyzqweqweqweqweqweqwe",
        array.data,
    );
    try testing.expectEqualDeep(&[_]u8{ 0b11110110, 0b00000001 }, array.validity.?);
    try testing.expectEqualDeep(&[_]index_type.to_type(){ 0, 0, 3, 3, 3, 9, 12, 12, 30, 30, 30 }, array.offsets);
}

test "binary nullable" {
    try test_binary_nullable(.i32);
    try test_binary_nullable(.i64);
}

fn test_binary_non_nullable(comptime index_type: arr.IndexType) !void {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const len = 10;
    const data_capacity = 30;

    var builder = try GenericBinaryBuilder(index_type).with_capacity(data_capacity, len, false, allocator);

    try testing.expectEqual(Error.NonNullable, builder.append_null());
    try testing.expectEqual(Error.NonNullable, builder.append_option(null));

    try builder.append_value("");
    try builder.append_value("asd");
    try builder.append_value("");
    try builder.append_value("");
    try builder.append_value("pppqwe");
    try builder.append_option("xyz");

    try testing.expectEqual(Error.LenCapacityMismatch, builder.finish());

    try testing.expectEqual(Error.OutOfCapacity, builder.append_value("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));

    try builder.append_value("");
    try builder.append_value("qweqweqweqweqweqwe");
    try builder.append_value("");
    try builder.append_value("");

    try testing.expectEqual(Error.OutOfCapacity, builder.append_value("asd"));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_value(""));

    const array = try builder.finish();

    try testing.expectEqual(len, array.len);
    try testing.expectEqual(0, array.null_count);
    try testing.expectEqual(0, array.offset);
    try testing.expectEqualDeep(
        "asdpppqwexyzqweqweqweqweqweqwe",
        array.data,
    );
    try testing.expectEqual(null, array.validity);
    try testing.expectEqualDeep(&[_]index_type.to_type(){ 0, 0, 3, 3, 3, 9, 12, 12, 30, 30, 30 }, array.offsets);
}

test "binary non-nullable" {
    try test_binary_non_nullable(.i32);
    try test_binary_non_nullable(.i64);
}

test "utf8 smoke" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var builder = try Utf8Builder.with_capacity(0, 0, true, allocator);

    try testing.expectEqual(Error.OutOfCapacity, builder.append_null());
    try testing.expectEqual(Error.OutOfCapacity, builder.append_value("12312312312"));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_option(null));

    const array = try builder.finish();
    _ = array;
}

test "date smoke" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var builder = try DateBuilder(.i32).with_capacity(0, true, allocator);

    try testing.expectEqual(Error.OutOfCapacity, builder.append_null());
    try testing.expectEqual(Error.OutOfCapacity, builder.append_value(21312));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_option(null));

    const array = try builder.finish();
    _ = array;
}

test "time smoke" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var builder = try TimeBuilder(.i32).with_capacity(.second, 0, true, allocator);

    try testing.expectEqual(Error.OutOfCapacity, builder.append_null());
    try testing.expectEqual(Error.OutOfCapacity, builder.append_value(21312));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_option(null));

    const array = try builder.finish();
    _ = array;
}

test "timestamp smoke" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var builder = try TimestampBuilder.with_capacity(arr.Timestamp{ .unit = .second, .timezone = "Europe/Paris" }, 0, true, allocator);

    try testing.expectEqual(Error.OutOfCapacity, builder.append_null());
    try testing.expectEqual(Error.OutOfCapacity, builder.append_value(21312));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_option(null));

    const array = try builder.finish();
    _ = array;
}

test "interval smoke" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var builder = try IntervalMonthDayNanoBuilder.with_capacity(0, true, allocator);

    try testing.expectEqual(Error.OutOfCapacity, builder.append_null());
    try testing.expectEqual(Error.OutOfCapacity, builder.append_value(arr.MonthDayNano{
        .days = 69,
        .months = 31,
        .nanoseconds = 1131,
    }));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_option(null));

    const array = try builder.finish();
    _ = array;
}

test "duration smoke" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var builder = try DurationBuilder.with_capacity(.second, 0, true, allocator);

    try testing.expectEqual(Error.OutOfCapacity, builder.append_null());
    try testing.expectEqual(Error.OutOfCapacity, builder.append_value(21312));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_option(null));

    const array = try builder.finish();
    _ = array;
}

test "binary-view empty" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var builder = try BinaryViewBuilder.with_capacity(0, 0, true, allocator);

    try testing.expectEqual(Error.OutOfCapacity, builder.append_null());
    try testing.expectEqual(Error.OutOfCapacity, builder.append_value("12312312312"));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_option(null));

    const array = try builder.finish();
    try testing.expectEqual(0, array.len);
    try testing.expectEqual(0, array.null_count);
    try testing.expectEqual(0, array.offset);
    try testing.expectEqual(1, array.buffers.len);
    try testing.expectEqualDeep(&[_]u8{}, array.validity.?);
    try testing.expectEqualDeep(&[_]arr.BinaryView{}, array.views);
}

test "binary-view nullable" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const len = 10;
    const buffer_capacity = 30;

    var builder = try BinaryViewBuilder.with_capacity(buffer_capacity, len, true, allocator);

    try builder.append_null();
    try builder.append_value("asd");
    try builder.append_value("");
    try builder.append_option(null);
    try builder.append_option("pppqwe");
    try builder.append_option("xyz");

    try testing.expectEqual(Error.LenCapacityMismatch, builder.finish());

    try testing.expectEqual(Error.OutOfCapacity, builder.append_value("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));

    try builder.append_option("1234561234567");
    try builder.append_value("qweqweqweqweqweqw");
    try builder.append_option("");
    try builder.append_null();

    try testing.expectEqual(Error.OutOfCapacity, builder.append_null());
    try testing.expectEqual(Error.OutOfCapacity, builder.append_value("asd"));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_option(null));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_option(""));

    const array = try builder.finish();

    try testing.expectEqual(len, array.len);
    try testing.expectEqual(3, array.null_count);
    try testing.expectEqual(0, array.offset);
    try testing.expectEqual(1, array.buffers.len);
    try testing.expectEqualDeep(
        "1234561234567qweqweqweqweqweqw",
        array.buffers[0][0..30],
    );
    try testing.expectEqualDeep(&[_]u8{ 0b11110110, 0b00000001 }, array.validity.?);

    const zero_view = arr.BinaryView{
        .length = 0,
        .prefix = .{ 0, 0, 0, 0 },
        .buffer_idx = 0,
        .offset = 0,
    };

    try testing.expectEqualDeep(&[_]arr.BinaryView{
        zero_view,
        @bitCast(arr.BinaryViewShort{
            .length = 3,
            .data = .{ 'a', 's', 'd', 0, 0, 0, 0, 0, 0, 0, 0, 0 },
        }),
        zero_view,
        zero_view,
        @bitCast(arr.BinaryViewShort{
            .length = 6,
            .data = .{ 'p', 'p', 'p', 'q', 'w', 'e', 0, 0, 0, 0, 0, 0 },
        }),
        @bitCast(arr.BinaryViewShort{
            .length = 3,
            .data = .{ 'x', 'y', 'z', 0, 0, 0, 0, 0, 0, 0, 0, 0 },
        }),
        arr.BinaryView{
            .length = 13,
            .prefix = .{ '1', '2', '3', '4' },
            .buffer_idx = 0,
            .offset = 0,
        },
        arr.BinaryView{
            .length = 17,
            .prefix = .{ 'q', 'w', 'e', 'q' },
            .buffer_idx = 0,
            .offset = 13,
        },
        zero_view,
        zero_view,
    }, array.views);
}

test "binary-view non-nullable" {
    var arena = ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const len = 10;
    const buffer_capacity = 30;

    var builder = try BinaryViewBuilder.with_capacity(buffer_capacity, len, false, allocator);

    try testing.expectEqual(Error.NonNullable, builder.append_null());
    try testing.expectEqual(Error.NonNullable, builder.append_option(null));

    try builder.append_value("");
    try builder.append_value("asd");
    try builder.append_value("");
    try builder.append_value("");
    try builder.append_option("pppqwe");
    try builder.append_option("xyz");

    try testing.expectEqual(Error.LenCapacityMismatch, builder.finish());

    try testing.expectEqual(Error.OutOfCapacity, builder.append_value("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));

    try builder.append_value("1234561234567");
    try builder.append_option("qweqweqweqweqweqw");
    try builder.append_value("");
    try builder.append_value("");

    try testing.expectEqual(Error.OutOfCapacity, builder.append_value("asd"));
    try testing.expectEqual(Error.OutOfCapacity, builder.append_value(""));

    const array = try builder.finish();

    try testing.expectEqual(len, array.len);
    try testing.expectEqual(0, array.null_count);
    try testing.expectEqual(0, array.offset);
    try testing.expectEqual(1, array.buffers.len);
    try testing.expectEqualDeep(
        "1234561234567qweqweqweqweqweqw",
        array.buffers[0][0..30],
    );
    try testing.expectEqual(null, array.validity);

    const zero_view = arr.BinaryView{
        .length = 0,
        .prefix = .{ 0, 0, 0, 0 },
        .buffer_idx = 0,
        .offset = 0,
    };

    try testing.expectEqualDeep(&[_]arr.BinaryView{
        zero_view,
        @bitCast(arr.BinaryViewShort{
            .length = 3,
            .data = .{ 'a', 's', 'd', 0, 0, 0, 0, 0, 0, 0, 0, 0 },
        }),
        zero_view,
        zero_view,
        @bitCast(arr.BinaryViewShort{
            .length = 6,
            .data = .{ 'p', 'p', 'p', 'q', 'w', 'e', 0, 0, 0, 0, 0, 0 },
        }),
        @bitCast(arr.BinaryViewShort{
            .length = 3,
            .data = .{ 'x', 'y', 'z', 0, 0, 0, 0, 0, 0, 0, 0, 0 },
        }),
        arr.BinaryView{
            .length = 13,
            .prefix = .{ '1', '2', '3', '4' },
            .buffer_idx = 0,
            .offset = 0,
        },
        arr.BinaryView{
            .length = 17,
            .prefix = .{ 'q', 'w', 'e', 'q' },
            .buffer_idx = 0,
            .offset = 13,
        },
        zero_view,
        zero_view,
    }, array.views);
}
