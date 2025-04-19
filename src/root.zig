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

    fn from(arr: anytype) Array {
        const arr_t = @typeInfo(@TypeOf(arr)).pointer.child;

        inline for (@typeInfo(ArrayType).@"enum".fields) |field| {
            const type_: ArrayType = @enumFromInt(field.value);
            if (array_type(type_) == arr_t) {
                return .{
                    .type_ = type_,
                    .arr = arr,
                };
            }
        }

        @compileError("unrecognized concrete array type");
    }

    fn to(self: Array, comptime ArrayT: ArrayType) *const array_type(ArrayT) {
        std.debug.assert(ArrayT == self.type_);

        return @ptrCast(@alignCast(self.arr));
    }
};

const ALIGNMENT = 64;

pub const BoolArray = struct {
    values: []align(ALIGNMENT) const u8,
    validity: ?[]align(ALIGNMENT) const u8,
    len: i64,
    offset: i64,
};

fn PrimitiveArray(comptime T: type) type {
    return struct {
        const Self = @This();

        values: []align(ALIGNMENT) const T,
        validity: ?[]align(ALIGNMENT) const u8,
        len: i64,
        offset: i64,
    };
}

pub const FixedSizeBinaryArray = struct {
    data: []align(ALIGNMENT) const u8,
    byte_width: i32,
    len: i64,
    offset: i64,
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
};

pub const RunEndArray = struct {
    run_ends: Array,
    values: Array,
    len: i64,
    offset: i64,
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
};

pub const FixedSizeListArray = struct {
    inner: Array,
    validity: ?[]align(ALIGNMENT) const u8,
    len: i64,
    offset: i64,
    item_width: i32,
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
};

pub const SparseUnionArray = struct {
    type_set: []align(ALIGNMENT) const i8,
    types: []align(ALIGNMENT) const i8,
    children: []const Array,
    len: i64,
    offset: i64,
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
    };
}

pub const Time32Array = TimeArr(i32);
pub const Time64Array = TimeArr(i64);

pub const TimestampArray = struct {
    inner: Int64Array,
    ts: Timestamp,
};

pub const IntervalDayTimeArray = struct {
    inner: PrimitiveArray([2]i32),
};

pub const MonthDayNano = extern struct {
    months: i32,
    days: i32,
    nanoseconds: i64,
};

pub const IntervalMonthDayNanoArray = struct {
    inner: PrimitiveArray(MonthDayNano),
};

pub const IntervalYearMonthArray = struct {
    inner: Int32Array,
};

pub const DurationArray = struct {
    inner: Int64Array,
    unit: TimeUnit,
};

pub const NullArray = struct {
    len: i64,
    offset: i64,
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
};

pub const Utf8ViewArray = struct {
    inner: BinaryViewArray,
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
};

test "array casting" {
    const typed_arr: Int32Array = .{
        .validity = null,
        .len = 0,
        .offset = 0,
        .values = &.{},
    };

    const arr = Array.from(&typed_arr);

    const re_typed_arr = arr.to(.i32);

    try testing.expectEqual(&typed_arr, re_typed_arr);
}
