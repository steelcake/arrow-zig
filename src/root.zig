const std = @import("std");

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

pub const DataType = union(enum) {
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
    decimal128: DecimalParams,
    decimal256: DecimalParams,
    date32: DateUnit,
    date64: DateUnit,
    time32: TimeUnit,
    time64: TimeUnit,
    timestamp: Timestamp,
    interval_year_month,
    interval_day_time,
    interval_month_day_nano,
    list,
    struct_,
    dense_union,
    sparse_union,
    fixed_size_binary: i32,
    fixed_size_list: i32,
    map,
    duration: TimeUnit,
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

pub const Array = struct {
    data_type: DataType,
    arr: *const anyopaque,
};

const ALIGNMENT = 64;

pub const BoolArray = struct {
    values: []align(ALIGNMENT) const u8,
    validity: ?[]align(ALIGNMENT) const u8,
    len: i64,
    offset: i64,

    pub fn as_array(self: *const BoolArray) Array {
        return .{
            .data_type = .bool,
            .arr = self,
        };
    }
};

pub fn FixedSizeBinaryArray(comptime ByteWidth: comptime_int) type {
    return struct {
        const Self = @This();

        data: []align(ALIGNMENT) const [ByteWidth]u8,
        validity: ?[]align(ALIGNMENT) const u8,
        len: i64,
        offset: i64,

        pub fn as_array(self: *const Self) Array {
            const data_type = comptime .{
                .fixed_size_binary = ByteWidth,
            };

            return .{
                .arr = self,
                .data_type = data_type,
            };
        }
    };
}

pub const FixedSizeListArray = struct {
    inner: Array,
    validity: ?[]align(ALIGNMENT) const u8,
    len: i64,
    offset: i64,
    item_width: i32,

    pub fn as_array(self: *const FixedSizeListArray) Array {
        return .{
            .data_type = .{ .fixed_size_list = self.item_width },
            .arr = self,
        };
    }
};

fn PrimitiveArray(comptime T: type) type {
    comptime switch (T) {
        u8 or u16 or u32 or u64 or i8 or i16 or i32 or i64 or i128 or i256 or f16 or f32 or f64 => {},
        else => @compileError("unsupported primitive type"),
    };

    return struct {
        const Self = @This();

        values: []align(ALIGNMENT) const T,
        validity: ?[]align(ALIGNMENT) const u8,
        len: i64,
        offset: i64,

        pub fn as_array(self: *const Self) Array {
            const data_type: DataType = comptime switch (T) {
                u8 => .u8,
                u16 => .u16,
                u32 => .u32,
                u64 => .u64,
                i8 => .i8,
                i16 => .i16,
                i32 => .i32,
                i64 => .i64,
                i128 or i256 => @compileError("can't call to_array on inner primitive array of decimal"),
                else => @compileError("unsupported primitive type"),
            };

            return .{
                .arr = self,
                .data_type = data_type,
            };
        }
    };
}

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
            const data_type: DataType = switch (T) {
                i128 => .{ .decimal128 = self.params },
                i256 => .{ .decimal256 = self.params },
                else => @compileError("unsupported index type"),
            };

            return .{
                .data_type = data_type,
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
    ordered: bool,

    pub fn as_array(self: *const DictArray) Array {
        return .{
            .arr = self,
            .data_type = .dict,
        };
    }
};

pub const RunEndArray = struct {
    run_ends: Array,
    values: Array,

    pub fn as_array(self: *const RunEndArray) Array {
        return .{
            .arr = self,
            .data_type = .run_end_encoded,
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
            const data_type: DataType = comptime switch (IndexT) {
                i32 => .binary,
                i64 => .large_binary,
                else => @compileError("unsupported index type"),
            };

            return .{
                .data_type = data_type,
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
            const data_type: DataType = comptime switch (IndexT) {
                i32 => .utf8,
                i64 => .large_utf8,
                else => @compileError("unsupported index type"),
            };

            return .{
                .data_type = data_type,
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
            .data_type = .struct_,
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
            const data_type: DataType = comptime switch (IndexT) {
                i32 => .list,
                i64 => .large_list,
                else => @compileError("unsupported index type"),
            };

            return .{
                .data_type = data_type,
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

    pub fn as_array(self: *const DenseUnionArray) Array {
        return .{
            .data_type = .dense_union,
            .arr = self,
        };
    }
};

pub const SparseUnionArray = struct {
    type_set: []align(ALIGNMENT) const i8,
    types: []align(ALIGNMENT) const i8,
    children: []const Array,

    pub fn as_array(self: *const SparseUnionArray) Array {
        return .{
            .data_type = .sparse_union,
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
            const data_type: DataType = comptime switch (T) {
                i32 => .{ .date32 = self.unit },
                i64 => .{ .date64 = self.unit },
            };

            return .{
                .data_type = data_type,
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
            const data_type: DataType = comptime switch (T) {
                i32 => .{ .time32 = self.unit },
                i64 => .{ .time64 = self.unit },
            };

            return .{
                .data_type = data_type,
                .arr = self,
            };
        }
    };
}

pub const Time32Array = TimeArr(i32);
pub const Time64Array = TimeArr(i64);

pub const TimestampArray = struct {
    inner: Int64Array,
    dt: Timestamp,

    pub fn as_array(self: *const TimestampArray) Array {
        return .{
            .data_type = .{ .timestamp = self.dt },
            .arr = self,
        };
    }
};

pub const IntervalDayTimeArray = struct {
    values: []align(ALIGNMENT) const [2]i32,
    validity: ?[]align(ALIGNMENT) const u8,
    len: i64,
    offset: i64,

    pub fn as_array(self: *const IntervalDayTimeArray) Array {
        return .{
            .data_type = .interval_day_time,
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
    values: []align(ALIGNMENT) const MonthDayNano,
    validity: ?[]align(ALIGNMENT) const u8,
    len: i64,
    offset: i64,

    pub fn as_array(self: *const IntervalDayTimeArray) Array {
        return .{
            .data_type = .interval_month_day_nano,
            .arr = self,
        };
    }
};

pub const IntervalYearMonthArray = struct {
    inner: Int32Array,

    pub fn as_array(self: *const IntervalDayTimeArray) Array {
        return .{
            .data_type = .interval_year_month,
            .arr = self,
        };
    }
};

pub const DurationArray = struct {
    inner: Int64Array,
    unit: TimeUnit,

    pub fn as_array(self: *const DurationArray) Array {
        return .{
            .data_type = .{ .duration = self.unit },
            .arr = self,
        };
    }
};

pub const NullArray = struct {
    len: i64,
    offset: i64,

    pub fn as_array(self: *const NullArray) Array {
        return .{
            .data_type = .null,
            .arr = self,
        };
    }
};

pub const BinaryViewArray = struct {};
pub const Utf8ViewArray = struct {};
pub const ListViewArray = struct {};
pub const LargeListViewArray = struct {};

pub const MapArray = struct {};
