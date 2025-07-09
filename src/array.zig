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

pub const Array = union(enum(u8)) {
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

pub fn PrimitiveArray(comptime T: type) type {
    return struct {
        values: []const T,
        validity: ?[]const u8,
        len: u32,
        offset: u32,
        null_count: u32,
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

pub const FixedSizeBinaryArray = struct {
    data: []const u8,
    validity: ?[]const u8,
    byte_width: i32,
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

pub fn DecimalArray(comptime int: DecimalInt) type {
    return struct {
        inner: PrimitiveArray(int.to_type()),
        params: DecimalParams,
    };
}

pub const Decimal32Array = DecimalArray(.i32);
pub const Decimal64Array = DecimalArray(.i64);
pub const Decimal128Array = DecimalArray(.i128);
pub const Decimal256Array = DecimalArray(.i256);

pub const DictArray = struct {
    keys: *const Array,
    values: *const Array,
    is_ordered: bool,
    /// Not in arrow spec but the len and offset here are applied on top of the len and offset of `keys` similar to how it would work in a struct array.
    len: u32,
    /// Not in arrow spec but the len and offset here are applied on top of the len and offset of `keys` similar to how it would work in a struct array.
    offset: u32,
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

pub fn GenericBinaryArray(comptime index_type: IndexType) type {
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

pub const BinaryArray = GenericBinaryArray(.i32);
pub const LargeBinaryArray = GenericBinaryArray(.i64);

pub fn GenericUtf8Array(comptime index_type: IndexType) type {
    return struct {
        inner: GenericBinaryArray(index_type),
    };
}

pub const Utf8Array = GenericUtf8Array(.i32);
pub const LargeUtf8Array = GenericUtf8Array(.i64);

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

pub fn GenericListArray(comptime index_type: IndexType) type {
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

pub const ListArray = GenericListArray(.i32);
pub const LargeListArray = GenericListArray(.i64);

pub const UnionArray = struct {
    type_id_set: []const i8,
    field_names: []const [:0]const u8,
    type_ids: []const i8,
    children: []const Array,
    len: u32,
    offset: u32,
};

pub const DenseUnionArray = struct {
    offsets: []const i32,
    inner: UnionArray,
};

pub const SparseUnionArray = struct {
    inner: UnionArray,
};

pub fn DateArray(comptime backing_t: IndexType) type {
    return struct {
        inner: PrimitiveArray(backing_t.to_type()),
    };
}

pub const Date32Array = DateArray(.i32);
pub const Date64Array = DateArray(.i64);

pub fn TimeArray(comptime backing_t: IndexType) type {
    const T = backing_t.to_type();

    return struct {
        pub const Unit = switch (backing_t) {
            .i32 => Time32Unit,
            .i64 => Time64Unit,
        };

        inner: PrimitiveArray(T),
        unit: Unit,
    };
}

pub const Time32Array = TimeArray(.i32);
pub const Time64Array = TimeArray(.i64);

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

pub fn IntervalArray(comptime interval_type: IntervalType) type {
    return struct {
        inner: PrimitiveArray(interval_type.to_type()),
    };
}

pub const IntervalDayTimeArray = IntervalArray(.day_time);
pub const IntervalMonthDayNanoArray = IntervalArray(.month_day_nano);
pub const IntervalYearMonthArray = IntervalArray(.year_month);

pub const DurationArray = struct {
    inner: Int64Array,
    unit: TimestampUnit,
};

pub const NullArray = struct {
    len: u32,
};

pub const BinaryView = extern struct {
    length: i32,
    prefix: i32,
    buffer_idx: i32,
    offset: i32,
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

pub fn GenericListViewArray(comptime index_type: IndexType) type {
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

pub const ListViewArray = GenericListViewArray(.i32);
pub const LargeListViewArray = GenericListViewArray(.i64);

pub const MapArray = struct {
    entries: *const StructArray,
    offsets: []const i32,
    validity: ?[]const u8,
    len: u32,
    offset: u32,
    null_count: u32,
    keys_are_sorted: bool,
};
