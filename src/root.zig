const std = @import("std");
const testing = std.testing;

pub const DecimalParams = struct {
    precision: u8,
    scale: i8,
};

pub const TimeUnit = enum {
    second,
    millisecond,
    microsecond,
    nanosecond,
};

pub const DateUnit = enum {
    day,
    millisecond,
};

pub const Timestamp = struct {
    unit: TimeUnit,
    timezone: ?[]const u8,
};

pub const ArrayType = enum {
    null,
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
    f16,
    f32,
    f64,
    binary,
    utf8,
    bool,
    decimal128,
    decimal256,
    date32,
    date64,
    time32,
    time64,
    timestamp,
    interval_year_month,
    interval_day_time,
    interval_month_day_nano,
    list,
    struct_,
    dense_union,
    sparse_union,
    fixed_size_binary,
    fixed_size_list,
    map,
    duration,
    large_binary,
    large_utf8,
    large_list,
    run_end_encoded,
    binary_view,
    utf8_view,
    list_view,
    large_list_view,
    dict,
};

fn array_type(comptime ArrayT: ArrayType) type {
    return switch (ArrayT) {
        .null => NullArray,
        .i8 => Int8Array,
        .i16 => Int16Array,
        .i32 => Int32Array,
        .i64 => Int64Array,
        .u8 => UInt8Array,
        .u16 => UInt16Array,
        .u32 => UInt32Array,
        .u64 => UInt64Array,
        .f16 => Float16Array,
        .f32 => Float32Array,
        .f64 => Float64Array,
        .binary => BinaryArray,
        .utf8 => Utf8Array,
        .bool => BoolArray,
        .decimal128 => Decimal128Array,
        .decimal256 => Decimal256Array,
        .date32 => Date32Array,
        .date64 => Date64Array,
        .time32 => Time32Array,
        .time64 => Time64Array,
        .timestamp => TimestampArray,
        .interval_year_month => IntervalYearMonthArray,
        .interval_day_time => IntervalDayTimeArray,
        .interval_month_day_nano => IntervalMonthDayNanoArray,
        .list => ListArray,
        .struct_ => StructArray,
        .dense_union => DenseUnionArray,
        .sparse_union => SparseUnionArray,
        .fixed_size_binary => FixedSizeBinaryArray,
        .fixed_size_list => FixedSizeListArray,
        .map => MapArray,
        .duration => DurationArray,
        .large_binary => LargeBinaryArray,
        .large_utf8 => LargeUtf8Array,
        .large_list => LargeListArray,
        .run_end_encoded => RunEndArray,
        .binary_view => BinaryViewArray,
        .utf8_view => Utf8ViewArray,
        .list_view => ListViewArray,
        .large_list_view => LargeListViewArray,
        .dict => DictArray,
    };
}

pub const Array = struct {
    arr: *const anyopaque,
    type_: ArrayType,

    fn as(self: Array, comptime ArrayT: ArrayType) *const array_type(ArrayT) {
        if (ArrayT != self.type_) {
            unreachable;
        }

        return @ptrCast(@alignCast(self.arr));
    }
};

const ALIGNMENT = 64;

pub const BoolArray = struct {
    values: []align(ALIGNMENT) const u8,
    validity: ?[]align(ALIGNMENT) const u8,
    len: i64,
    offset: i64,

    pub fn as_array(self: *const BoolArray) Array {
        return .{
            .type_ = .bool,
            .arr = self,
        };
    }
};

fn PrimitiveArray(comptime T: type) type {
    return struct {
        const Self = @This();

        values: []align(ALIGNMENT) const T,
        validity: ?[]align(ALIGNMENT) const u8,
        len: i64,
        offset: i64,

        pub fn as_array(self: *const Self) Array {
            const type_: ArrayType = comptime switch (T) {
                u8 => .u8,
                u16 => .u16,
                u32 => .u32,
                u64 => .u64,
                i8 => .i8,
                i16 => .i16,
                i32 => .i32,
                i64 => .i64,
                f16 => .f16,
                f32 => .f32,
                f64 => .f64,
                else => @compileError("unsupported primitive type"),
            };

            return .{
                .arr = self,
                .type_ = type_,
            };
        }
    };
}

pub const FixedSizeBinaryArray = struct {
    data: []align(ALIGNMENT) const u8,
    byte_width: i32,
    len: i64,
    offset: i64,

    pub fn as_array(self: *const FixedSizeBinaryArray) Array {
        return .{
            .arr = self,
            .type_ = .fixed_size_binary,
        };
    }
};

pub const UInt8Array = PrimitiveArray(u8);
pub const UInt16Array = PrimitiveArray(u16);
pub const UInt32Array = PrimitiveArray(u32);
pub const UInt64Array = PrimitiveArray(u64);
pub const Int8Array = PrimitiveArray(i8);
pub const Int16Array = PrimitiveArray(i16);
pub const Int32Array = PrimitiveArray(i32);
pub const Int64Array = PrimitiveArray(i64);
pub const Float16Array = PrimitiveArray(f16);
pub const Float32Array = PrimitiveArray(f32);
pub const Float64Array = PrimitiveArray(f64);

fn DecimalArr(comptime T: type) type {
    comptime switch (T) {
        i128 or i256 => {},
        else => @compileError("unsupported decimal impl type"),
    };

    return struct {
        const Self = @This();

        inner: PrimitiveArray(T),
        params: DecimalParams,

        pub fn as_array(self: *const Self) Array {
            const type_: ArrayType = switch (T) {
                i128 => .{ .decimal128 = self.params },
                i256 => .{ .decimal256 = self.params },
                else => @compileError("unsupported index type"),
            };

            return .{
                .type_ = type_,
                .arr = self,
            };
        }
    };
}

pub const Decimal128Array = DecimalArr(i128);
pub const Decimal256Array = DecimalArr(i256);

pub const DictArray = struct {
    keys: Array,
    values: Array,
    is_ordered: bool,
    len: i64,
    offset: i64,

    pub fn as_array(self: *const DictArray) Array {
        return .{
            .arr = self,
            .type_ = .dict,
        };
    }
};

pub const RunEndArray = struct {
    run_ends: Array,
    values: Array,
    len: i64,
    offset: i64,

    pub fn as_array(self: *const RunEndArray) Array {
        return .{
            .arr = self,
            .type_ = .run_end_encoded,
        };
    }
};

fn BinaryArr(comptime IndexT: type) type {
    comptime switch (IndexT) {
        i32 or i64 => {},
        else => @compileError("unsupported index type"),
    };

    return struct {
        const Self = @This();

        data: []align(ALIGNMENT) const u8,
        offsets: []align(ALIGNMENT) const IndexT,
        validity: ?[]align(ALIGNMENT) const u8,
        len: i64,
        offset: i64,

        pub fn as_array(self: *const Self) Array {
            const type_: ArrayType = comptime switch (IndexT) {
                i32 => .binary,
                i64 => .large_binary,
                else => @compileError("unsupported index type"),
            };

            return .{
                .type_ = type_,
                .arr = self,
            };
        }
    };
}

pub const BinaryArray = BinaryArr(i32);
pub const LargeBinaryArray = BinaryArr(i64);

fn Utf8Arr(comptime IndexT: type) type {
    comptime switch (IndexT) {
        i32 or i64 => {},
        else => @compileError("unsupported index type"),
    };

    return struct {
        const Self = @This();

        inner: BinaryArr(IndexT),

        pub fn as_array(self: *const Self) Array {
            const type_: ArrayType = comptime switch (IndexT) {
                i32 => .utf8,
                i64 => .large_utf8,
                else => @compileError("unsupported index type"),
            };

            return .{
                .data_type = type_,
                .arr = self,
            };
        }
    };
}

pub const Utf8Array = Utf8Arr(i32);
pub const LargeUtf8Array = Utf8Arr(i64);

pub const StructArray = struct {
    field_names: BinaryArray,
    field_values: []const Array,
    validity: ?[]align(ALIGNMENT) const u8,
    len: i64,
    offset: i64,

    pub fn as_array(self: *const StructArray) Array {
        return .{
            .type_ = .struct_,
            .arr = self,
        };
    }
};

pub const FixedSizeListArray = struct {
    inner: Array,
    validity: ?[]align(ALIGNMENT) const u8,
    len: i64,
    offset: i64,
    item_width: i32,

    pub fn as_array(self: *const FixedSizeListArray) Array {
        return .{
            .type_ = .fixed_size_list,
            .arr = self,
        };
    }
};

fn ListArr(comptime IndexT: type) type {
    comptime switch (IndexT) {
        i32 or i64 => {},
        else => @compileError("unsupported index type"),
    };

    return struct {
        const Self = @This();

        inner: Array,
        offsets: []align(ALIGNMENT) const IndexT,
        validity: ?[]align(ALIGNMENT) const u8,
        len: i64,
        offset: i64,

        pub fn as_array(self: *const Self) Array {
            const type_: ArrayType = comptime switch (IndexT) {
                i32 => .list,
                i64 => .large_list,
                else => @compileError("unsupported index type"),
            };

            return .{
                .type_ = type_,
                .arr = self,
            };
        }
    };
}

pub const ListArray = ListArr(i32);
pub const LargeListArray = ListArr(i64);

pub const DenseUnionArray = struct {
    type_set: []align(ALIGNMENT) const i8,
    types: []align(ALIGNMENT) const i8,
    offsets: []align(ALIGNMENT) const i32,
    children: []const Array,
    len: i64,
    offset: i64,

    pub fn as_array(self: *const DenseUnionArray) Array {
        return .{
            .type_ = .dense_union,
            .arr = self,
        };
    }
};

pub const SparseUnionArray = struct {
    type_set: []align(ALIGNMENT) const i8,
    types: []align(ALIGNMENT) const i8,
    children: []const Array,
    len: i64,
    offset: i64,

    pub fn as_array(self: *const SparseUnionArray) Array {
        return .{
            .type_ = .sparse_union,
            .arr = self,
        };
    }
};

fn DateArr(comptime T: type) type {
    comptime switch (T) {
        i32 or i64 => {},
        else => @compileError("unsupported index type"),
    };

    return struct {
        const Self = @This();

        inner: PrimitiveArray(T),
        unit: DateUnit,

        pub fn as_array(self: *const Self) Array {
            const type_: ArrayType = comptime switch (T) {
                i32 => .date32,
                i64 => .date64,
            };

            return .{
                .type_ = type_,
                .arr = self,
            };
        }
    };
}

pub const Date32Array = DateArr(i32);
pub const Date64Array = DateArr(i64);

fn TimeArr(comptime T: type) type {
    comptime switch (T) {
        i32 or i64 => {},
        else => @compileError("unsupported index type"),
    };

    return struct {
        const Self = @This();

        inner: PrimitiveArray(T),
        unit: TimeUnit,

        pub fn as_array(self: *const Self) Array {
            const type_: ArrayType = comptime switch (T) {
                i32 => .time32,
                i64 => .time64,
            };

            return .{
                .type_ = type_,
                .arr = self,
            };
        }
    };
}

pub const Time32Array = TimeArr(i32);
pub const Time64Array = TimeArr(i64);

pub const TimestampArray = struct {
    inner: Int64Array,
    ts: Timestamp,

    pub fn as_array(self: *const TimestampArray) Array {
        return .{
            .type_ = .timestamp,
            .arr = self,
        };
    }
};

pub const IntervalDayTimeArray = struct {
    inner: PrimitiveArray([2]i32),

    pub fn as_array(self: *const IntervalDayTimeArray) Array {
        return .{
            .type_ = .interval_day_time,
            .arr = self,
        };
    }
};

pub const MonthDayNano = extern struct {
    months: i32,
    days: i32,
    nanoseconds: i64,
};

pub const IntervalMonthDayNanoArray = struct {
    inner: PrimitiveArray(MonthDayNano),

    pub fn as_array(self: *const IntervalDayTimeArray) Array {
        return .{
            .type_ = .interval_month_day_nano,
            .arr = self,
        };
    }
};

pub const IntervalYearMonthArray = struct {
    inner: Int32Array,

    pub fn as_array(self: *const IntervalDayTimeArray) Array {
        return .{
            .type_ = .interval_year_month,
            .arr = self,
        };
    }
};

pub const DurationArray = struct {
    inner: Int64Array,
    unit: TimeUnit,

    pub fn as_array(self: *const DurationArray) Array {
        return .{
            .type_ = .{ .duration = self.unit },
            .arr = self,
        };
    }
};

pub const NullArray = struct {
    len: i64,
    offset: i64,

    pub fn as_array(self: *const NullArray) Array {
        return .{
            .type_ = .null,
            .arr = self,
        };
    }
};

pub const BinaryView = extern struct {
    length: u32,
    prefix: u32,
    buffer_idx: u32,
    offset: u32,
};

pub const BinaryViewArray = struct {
    views: []align(ALIGNMENT) const BinaryView,
    buffers: []const []align(ALIGNMENT) const u8,
    validity: ?[]align(ALIGNMENT) const u8,
    len: i64,
    offset: i64,

    pub fn as_array(self: *const BinaryViewArray) Array {
        return .{
            .type_ = .binary_view,
            .arr = self,
        };
    }
};

pub const Utf8ViewArray = struct {
    inner: BinaryViewArray,

    pub fn as_array(self: *const Utf8ViewArray) Array {
        return .{
            .type_ = .utf8_view,
            .arr = self,
        };
    }
};

fn ListViewArr(comptime IndexT: type) type {
    comptime switch (IndexT) {
        i32 or i64 => {},
        else => @compileError("unsupported index type"),
    };

    return struct {
        const Self = @This();

        inner: Array,
        offsets: []align(ALIGNMENT) const IndexT,
        sizes: []align(ALIGNMENT) const IndexT,
        validity: ?[]align(ALIGNMENT) const u8,
        len: i64,
        offset: i64,

        pub fn as_array(self: *const Self) Array {
            const type_: ArrayType = comptime switch (IndexT) {
                i32 => .list_view,
                i64 => .large_list_view,
                else => @compileError("unsupported index type"),
            };

            return .{
                .type_ = type_,
                .arr = self,
            };
        }
    };
}

pub const ListViewArray = ListViewArr(i32);
pub const LargeListViewArray = ListViewArr(i64);

pub const MapArray = struct {
    entries: StructArray,
    offsets: []align(ALIGNMENT) const i32,
    len: i64,
    offset: i64,
    keys_are_sorted: bool,

    pub fn as_array(self: *const MapArray) Array {
        return .{
            .type_ = .map,
            .arr = self,
        };
    }
};

test "array casting" {
    const typed_arr: Int32Array = .{
        .validity = null,
        .len = 0,
        .offset = 0,
        .values = &.{},
    };

    const arr = typed_arr.as_array();

    const re_typed_arr = arr.as(.i32);

    try testing.expectEqual(&typed_arr, re_typed_arr);
}
