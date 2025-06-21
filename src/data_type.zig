const arr = @import("./array.zig");

pub const StructType = struct {
    field_names: []const [:0]const u8,
    field_types: []const DataType,
};

pub const UnionType = struct {
    type_id_set: []const i8,
    field_names: []const [:0]const u8,
    field_types: []const DataType,
};

pub const MapKeyType = enum {
    binary,
    large_binary,
    utf8,
    large_utf8,
    binary_view,
    utf8_view,
};

pub const MapType = struct {
    key: MapKeyType,
    value: DataType,
};

pub const RunEndType = enum {
    i16,
    i32,
    i64,
};

pub const RunEndEncodedType = struct {
    run_end: RunEndType,
    value: DataType,
};

pub const DictKeyType = enum {
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
};

pub const DictType = struct {
    key: DictKeyType,
    value: DataType,
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
    decimal32: arr.DecimalParams,
    decimal64: arr.DecimalParams,
    decimal128: arr.DecimalParams,
    decimal256: arr.DecimalParams,
    date32,
    date64,
    time32: arr.Time32Unit,
    time64: arr.Time64Unit,
    timestamp: arr.Timestamp,
    interval_year_month,
    interval_day_time,
    interval_month_day_nano,
    list: *const DataType,
    struct_: *const StructType,
    dense_union: *const UnionType,
    sparse_union: *const UnionType,
    fixed_size_binary,
    fixed_size_list: *const DataType,
    map: *const MapType,
    duration: arr.TimestampUnit,
    large_binary,
    large_utf8,
    large_list: *const DataType,
    run_end_encoded: *const RunEndEncodedType,
    binary_view,
    utf8_view,
    list_view: *const DataType,
    large_list_view: *const DataType,
    dict: *const DictType,
};
