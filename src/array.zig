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

pub const Array = union(enum) {
    null: NullArray,
    i8: Int8Array,
    i16: Int16Array,
    i32: Int32Array,
    i64: Int64Array,
    u8: UInt8Array,
    u16: UInt16Array,
    u32: UInt32Array,
    u64: UInt64Array,
    f16: Float16Array,
    f32: Float32Array,
    f64: Float64Array,
    binary: BinaryArray,
    utf8: Utf8Array,
    bool: BoolArray,
    decimal32: Decimal32Array,
    decimal64: Decimal64Array,
    decimal128: Decimal128Array,
    decimal256: Decimal256Array,
    date32: Date32Array,
    date64: Date64Array,
    time32: Time32Array,
    time64: Time64Array,
    timestamp: TimestampArray,
    interval_year_month: IntervalYearMonthArray,
    interval_day_time: IntervalDayTimeArray,
    interval_month_day_nano: IntervalMonthDayNanoArray,
    list: ListArray,
    struct_: StructArray,
    dense_union: DenseUnionArray,
    sparse_union: SparseUnionArray,
    fixed_size_binary: FixedSizeBinaryArray,
    fixed_size_list: FixedSizeListArray,
    map: MapArray,
    duration: DurationArray,
    large_binary: LargeBinaryArray,
    large_utf8: LargeUtf8Array,
    large_list: LargeListArray,
    run_end_encoded: RunEndArray,
    binary_view: BinaryViewArray,
    utf8_view: Utf8ViewArray,
    list_view: ListViewArray,
    large_list_view: LargeListViewArray,
    dict: DictArray,
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

pub const FixedSizeBinaryArray = struct {
    data: []const u8,
    validity: ?[]const u8,
    byte_width: u32,
    len: u32,
    offset: u32,
    null_count: u32,
};

pub const DecimalInt = enum {
    i32,
    i64,
    i128,
    i256,

    pub fn to_type(comptime self: DecimalInt) type {
        return comptime switch (self) {
            .i32 => i32,
            .i64 => i64,
            .i128 => i128,
            .i256 => i256,
        };
    }
};

pub fn DecimalArr(comptime int: DecimalInt) type {
    return struct {
        inner: PrimitiveArr(int.to_type()),
        params: DecimalParams,
    };
}

pub const Decimal32Array = DecimalArr(.i32);
pub const Decimal64Array = DecimalArr(.i64);
pub const Decimal128Array = DecimalArr(.i128);
pub const Decimal256Array = DecimalArr(.i256);

pub const DictArray = struct {
    keys: *const Array,
    values: *const Array,
    is_ordered: bool,
};

pub const RunEndArray = struct {
    run_ends: *const Array,
    values: *const Array,
    len: u32,
    offset: u32,
};

pub const IndexType = enum {
    i32,
    i64,

    pub fn to_type(self: IndexType) type {
        return switch (self) {
            .i32 => i32,
            .i64 => i64,
        };
    }
};

pub fn BinaryArr(comptime index_type: IndexType) type {
    const I = index_type.to_type();

    return struct {
        data: []const u8,
        offsets: []const I,
        validity: ?[]const u8,
        len: u32,
        offset: u32,
        null_count: u32,
    };
}

pub const BinaryArray = BinaryArr(.i32);
pub const LargeBinaryArray = BinaryArr(.i64);

pub fn Utf8Arr(comptime index_type: IndexType) type {
    return struct {
        inner: BinaryArr(index_type),
    };
}

pub const Utf8Array = Utf8Arr(.i32);
pub const LargeUtf8Array = Utf8Arr(.i64);

pub const StructArray = struct {
    field_names: []const [:0]const u8,
    field_values: []const Array,
    validity: ?[]const u8,
    len: u32,
    offset: u32,
    null_count: u32,
};

pub const FixedSizeListArray = struct {
    inner: *const Array,
    validity: ?[]const u8,
    len: u32,
    offset: u32,
    null_count: u32,
    item_width: i32,
};

pub fn ListArr(comptime index_type: IndexType) type {
    const I = index_type.to_type();

    return struct {
        inner: *const Array,
        offsets: []const I,
        validity: ?[]const u8,
        len: u32,
        offset: u32,
        null_count: u32,
    };
}

pub const ListArray = ListArr(.i32);
pub const LargeListArray = ListArr(.i64);

pub const UnionArr = struct {
    type_id_set: []const i8,
    type_ids: []const i8,
    children: []const Array,
    len: u32,
    offset: u32,
};

pub const DenseUnionArray = struct {
    offsets: []const i32,
    inner: UnionArr,
};

pub const SparseUnionArray = struct {
    inner: UnionArr,
};

pub fn DateArr(comptime backing_t: IndexType) type {
    return struct {
        inner: PrimitiveArr(backing_t.to_type()),
    };
}

pub const Date32Array = DateArr(.i32);
pub const Date64Array = DateArr(.i64);

pub fn TimeArr(comptime backing_t: IndexType) type {
    const T = backing_t.to_type();

    const Unit = comptime switch (backing_t) {
        .i32 => Time32Unit,
        .i64 => Time64Unit,
    };

    return struct {
        inner: PrimitiveArr(T),
        unit: Unit,
    };
}

pub const Time32Array = TimeArr(.i32);
pub const Time64Array = TimeArr(.i64);

pub const TimestampArray = struct {
    inner: Int64Array,
    ts: Timestamp,
};

pub const MonthDayNano = extern struct {
    months: i32,
    days: i32,
    nanoseconds: i64,
};

pub const IntervalType = enum {
    month_day_nano,
    year_month,
    day_time,

    pub fn to_type(comptime self: IntervalType) type {
        return comptime switch (self) {
            .month_day_nano => MonthDayNano,
            .year_month => i32,
            .day_time => [2]i32,
        };
    }
};

pub fn IntervalArr(comptime interval_type: IntervalType) type {
    return struct {
        inner: PrimitiveArr(interval_type.to_type()),
    };
}

pub const IntervalDayTimeArray = IntervalArr(.day_time);
pub const IntervalMonthDayNanoArray = IntervalArr(.month_day_nano);
pub const IntervalYearMonthArray = IntervalArr(.year_month);

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

pub fn ListViewArr(comptime index_type: IndexType) type {
    const I = index_type.to_type();

    return struct {
        inner: *const Array,
        offsets: []const I,
        sizes: []const I,
        validity: ?[]const u8,
        len: u32,
        offset: u32,
        null_count: u32,
    };
}

pub const ListViewArray = ListViewArr(.i32);
pub const LargeListViewArray = ListViewArr(.i64);

pub const MapArray = struct {
    entries: *const StructArray,
    offsets: []const i32,
    validity: ?[]const u8,
    len: u32,
    offset: u32,
    null_count: u32,
    keys_are_sorted: bool,
};
