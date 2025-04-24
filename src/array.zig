const std = @import("std");
const Allocator = std.mem.Allocator;

pub const DecimalParams = struct {
    precision: u8,
    scale: i8,
};

pub const TimestampUnit = enum {
    second,
    millisecond,
    microsecond,
    nanosecond,
};

pub const Time32Unit = enum {
    second,
    millisecond,
};

pub const Time64Unit = enum {
    microsecond,
    nanosecond,
};

pub const Timestamp = struct {
    unit: TimestampUnit,
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
    decimal32,
    decimal64,
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

fn type_to_array_type(comptime T: type) ArrayType {
    return comptime switch (T) {
        NullArray => .null,
        Int8Array => .i8,
        Int16Array => .i16,
        Int32Array => .i32,
        Int64Array => .i64,
        UInt8Array => .u8,
        UInt16Array => .u16,
        UInt32Array => .u32,
        UInt64Array => .u64,
        Float16Array => .f16,
        Float32Array => .f32,
        Float64Array => .f64,
        BinaryArray => .binary,
        Utf8Array => .utf8,
        BoolArray => .bool,
        Decimal32Array => .decimal32,
        Decimal64Array => .decimal64,
        Decimal128Array => .decimal128,
        Decimal256Array => .decimal256,
        Date32Array => .date32,
        Date64Array => .date64,
        Time32Array => .time32,
        Time64Array => .time64,
        TimestampArray => .timestamp,
        IntervalYearMonthArray => .interval_year_month,
        IntervalDayTimeArray => .interval_day_time,
        IntervalMonthDayNanoArray => .interval_month_day_nano,
        ListArray => .list,
        StructArray => .struct_,
        DenseUnionArray => .dense_union,
        SparseUnionArray => .sparse_union,
        FixedSizeBinaryArray => .fixed_size_binary,
        FixedSizeListArray => .fixed_size_list,
        MapArray => .map,
        DurationArray => .duration,
        LargeBinaryArray => .large_binary,
        LargeUtf8Array => .large_utf8,
        LargeListArray => .large_list,
        RunEndArray => .run_end_encoded,
        BinaryViewArray => .binary_view,
        Utf8ViewArray => .utf8_view,
        ListViewArray => .list_view,
        LargeListViewArray => .large_list_view,
        DictArray => .dict,
        else => @compileError("unexpected array type"),
    };
}

fn array_type_to_type(comptime ArrayT: ArrayType) type {
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
        .decimal32 => Decimal32Array,
        .decimal64 => Decimal64Array,
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

pub const Error = error{OutOfMemory};

pub const Array = struct {
    arr: *const anyopaque,
    type_: ArrayType,

    pub fn from(arr: anytype, allocator: Allocator) Error!Array {
        const ptr = try allocator.create(@TypeOf(arr));
        ptr.* = arr;
        return Array.from_ptr(ptr);
    }

    pub fn from_ptr(arr: anytype) Array {
        const arr_t = comptime switch (@typeInfo(@TypeOf(arr))) {
            .pointer => |ptr_info| ptr_info.child,
            else => @compileError("expected input to be a pointer"),
        };

        return .{
            .type_ = type_to_array_type(arr_t),
            .arr = arr,
        };
    }

    pub fn to(self: Array, comptime ArrayT: ArrayType) *const array_type_to_type(ArrayT) {
        std.debug.assert(ArrayT == self.type_);
        return @ptrCast(@alignCast(self.arr));
    }
};

pub const BoolArray = struct {
    values: []const u8,
    validity: ?[]const u8,
    len: u32,
    offset: u32,
    null_count: u32,
};

pub fn PrimitiveArr(comptime T: type) type {
    return struct {
        values: []const T,
        validity: ?[]const u8,
        len: u32,
        offset: u32,
        null_count: u32,
    };
}

pub const FixedSizeBinaryArray = struct {
    data: []const u8,
    validity: ?[]const u8,
    byte_width: u32,
    len: u32,
    offset: u32,
    null_count: u32,
};

pub const UInt8Array = PrimitiveArr(u8);
pub const UInt16Array = PrimitiveArr(u16);
pub const UInt32Array = PrimitiveArr(u32);
pub const UInt64Array = PrimitiveArr(u64);
pub const Int8Array = PrimitiveArr(i8);
pub const Int16Array = PrimitiveArr(i16);
pub const Int32Array = PrimitiveArr(i32);
pub const Int64Array = PrimitiveArr(i64);
pub const Float16Array = PrimitiveArr(f16);
pub const Float32Array = PrimitiveArr(f32);
pub const Float64Array = PrimitiveArr(f64);

pub fn DecimalArr(comptime T: type) type {
    comptime switch (T) {
        i32, i64, i128, i256 => {},
        else => @compileError("unsupported decimal impl type"),
    };

    return struct {
        inner: PrimitiveArr(T),
        params: DecimalParams,
    };
}

pub const Decimal32Array = DecimalArr(i32);
pub const Decimal64Array = DecimalArr(i64);
pub const Decimal128Array = DecimalArr(i128);
pub const Decimal256Array = DecimalArr(i256);

pub const DictArray = struct {
    keys: Array,
    values: Array,
    is_ordered: bool,
};

pub const RunEndArray = struct {
    run_ends: Array,
    values: Array,
    len: u32,
    offset: u32,
};

pub fn BinaryArr(comptime IndexT: type) type {
    comptime switch (IndexT) {
        i32, i64 => {},
        else => @compileError("unsupported index type"),
    };

    return struct {
        data: []const u8,
        offsets: []const IndexT,
        validity: ?[]const u8,
        len: u32,
        offset: u32,
        null_count: u32,
    };
}

pub const BinaryArray = BinaryArr(i32);
pub const LargeBinaryArray = BinaryArr(i64);

pub const Utf8Array = struct {
    inner: BinaryArray,
};

pub const LargeUtf8Array = struct {
    inner: LargeBinaryArray,
};

pub const StructArray = struct {
    field_names: []const [:0]const u8,
    field_values: []const Array,
    validity: ?[]const u8,
    len: u32,
    offset: u32,
    null_count: u32,
};

pub const FixedSizeListArray = struct {
    inner: Array,
    validity: ?[]const u8,
    len: u32,
    offset: u32,
    null_count: u32,
    item_width: i32,
};

pub fn ListArr(comptime IndexT: type) type {
    comptime switch (IndexT) {
        i32, i64 => {},
        else => @compileError("unsupported index type"),
    };

    return struct {
        inner: Array,
        offsets: []const IndexT,
        validity: ?[]const u8,
        len: u32,
        offset: u32,
        null_count: u32,
    };
}

pub const ListArray = ListArr(i32);
pub const LargeListArray = ListArr(i64);

pub const DenseUnionArray = struct {
    type_id_set: []const i8,
    type_ids: []const i8,
    offsets: []const i32,
    children: []const Array,
    len: u32,
    offset: u32,
};

pub const SparseUnionArray = struct {
    type_id_set: []const i8,
    type_ids: []const i8,
    children: []const Array,
    len: u32,
    offset: u32,
};

pub fn DateArr(comptime T: type) type {
    comptime switch (T) {
        i32, i64 => {},
        else => @compileError("unsupported index type"),
    };

    return struct {
        inner: PrimitiveArr(T),
    };
}

pub const Date32Array = DateArr(i32);
pub const Date64Array = DateArr(i64);

pub fn TimeArr(comptime T: type) type {
    const UnitT = comptime switch (T) {
        i32 => Time32Unit,
        i64 => Time64Unit,
        else => @compileError("unsupported index type"),
    };

    return struct {
        inner: PrimitiveArr(T),
        unit: UnitT,
    };
}

pub const Time32Array = TimeArr(i32);
pub const Time64Array = TimeArr(i64);

pub const TimestampArray = struct {
    inner: Int64Array,
    ts: Timestamp,
};

pub const IntervalDayTimeArray = struct {
    inner: PrimitiveArr([2]i32),
};

pub const MonthDayNano = extern struct {
    months: i32,
    days: i32,
    nanoseconds: i64,
};

pub const IntervalMonthDayNanoArray = struct {
    inner: PrimitiveArr(MonthDayNano),
};

pub const IntervalYearMonthArray = struct {
    inner: Int32Array,
};

pub const DurationArray = struct {
    inner: Int64Array,
    unit: TimestampUnit,
};

pub const NullArray = struct {
    len: u32,
};

pub const BinaryView = extern struct {
    length: u32,
    prefix: u32,
    buffer_idx: u32,
    offset: u32,
};

pub const BinaryViewArray = struct {
    views: []const BinaryView,
    buffers: []const [*]const u8,
    validity: ?[]const u8,
    len: u32,
    offset: u32,
    null_count: u32,
};

pub const Utf8ViewArray = struct {
    inner: BinaryViewArray,
};

pub fn ListViewArr(comptime IndexT: type) type {
    comptime switch (IndexT) {
        i32, i64 => {},
        else => @compileError("unsupported index type"),
    };

    return struct {
        inner: Array,
        offsets: []const IndexT,
        sizes: []const IndexT,
        validity: ?[]const u8,
        len: u32,
        offset: u32,
        null_count: u32,
    };
}

pub const ListViewArray = ListViewArr(i32);
pub const LargeListViewArray = ListViewArr(i64);

pub const MapArray = struct {
    entries: StructArray,
    offsets: []const i32,
    validity: ?[]const u8,
    len: u32,
    offset: u32,
    null_count: u32,
    keys_are_sorted: bool,
};

test "array casting" {
    const typed_arr: Int32Array = .{
        .validity = null,
        .len = 0,
        .offset = 0,
        .values = &.{},
        .null_count = 0,
    };

    const arr = Array.from_ptr(&typed_arr);

    const re_typed_arr = arr.to(.i32);

    try std.testing.expectEqual(&typed_arr, re_typed_arr);
}
