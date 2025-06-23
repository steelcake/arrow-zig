const arr = @import("./array.zig");
const std = @import("std");
const testing = std.testing;

pub const StructType = struct {
    field_names: []const [:0]const u8,
    field_types: []const DataType,

    pub fn eql(self: *const StructType, other: *const StructType) bool {
        std.debug.assert(self.field_names.len == self.field_types.len);
        if (self.field_names.len != other.field_names.len or self.field_types.len != other.field_types.len) {
            return false;
        }

        for (self.field_names, other.field_names) |sfn, ofn| {
            if (!std.mem.eql(u8, sfn, ofn)) {
                return false;
            }
        }

        for (self.field_types, other.field_types) |*sft, *oft| {
            if (!sft.eql(oft)) {
                return false;
            }
        }

        return true;
    }
};

pub const UnionType = struct {
    type_id_set: []const i8,
    field_names: []const [:0]const u8,
    field_types: []const DataType,

    pub fn eql(self: *const UnionType, other: *const UnionType) bool {
        std.debug.assert(self.field_names.len == self.field_types.len);

        if (self.field_names.len != other.field_names.len or self.field_types.len != other.field_types.len) {
            return false;
        }

        for (self.field_names, other.field_names) |sfn, ofn| {
            if (!std.mem.eql(u8, sfn, ofn)) {
                return false;
            }
        }

        for (self.field_types, other.field_types) |*sft, *oft| {
            if (!sft.eql(oft)) {
                return false;
            }
        }

        return std.mem.eql(i8, self.type_id_set, other.type_id_set);
    }
};

pub const MapKeyType = enum {
    binary,
    large_binary,
    utf8,
    large_utf8,
    binary_view,
    utf8_view,

    pub fn to_data_type(self: MapKeyType) DataType {
        return switch (self) {
            .binary => DataType{ .binary = {} },
            .large_binary => DataType{ .large_binary = {} },
            .utf8 => DataType{ .utf8 = {} },
            .large_utf8 => DataType{ .large_utf8 = {} },
            .binary_view => DataType{ .binary_view = {} },
            .utf8_view => DataType{ .utf8_view = {} },
        };
    }
};

pub const MapType = struct {
    key: MapKeyType,
    value: DataType,

    pub fn eql(self: *const MapType, other: *const MapType) bool {
        return self.key == other.key and self.value.eql(&other.value);
    }
};

pub const RunEndType = enum {
    i16,
    i32,
    i64,

    pub fn to_data_type(self: RunEndType) DataType {
        return switch (self) {
            .i16 => .{ .i16 = {} },
            .i32 => .{ .i32 = {} },
            .i64 => .{ .i64 = {} },
        };
    }
};

pub const RunEndEncodedType = struct {
    run_end: RunEndType,
    value: DataType,

    pub fn eql(self: *const RunEndEncodedType, other: *const RunEndEncodedType) bool {
        return self.run_end == other.run_end and self.value.eql(&other.value);
    }
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

    pub fn to_data_type(self: DictKeyType) DataType {
        return switch (self) {
            i8 => .{ .i8 = {} },
            i16 => .{ .i16 = {} },
            i32 => .{ .i32 = {} },
            i64 => .{ .i64 = {} },
            u8 => .{ .u8 = {} },
            u16 => .{ .u16 = {} },
            u32 => .{ .u32 = {} },
            u64 => .{ .u64 = {} },
        };
    }
};

pub const DictType = struct {
    key: DictKeyType,
    value: DataType,

    pub fn eql(self: *const DictType, other: *const DictType) bool {
        return self.key == other.key and self.value.eql(&other.value);
    }
};

pub const FixedSizeListType = struct {
    inner: DataType,
    item_width: i32,

    pub fn eql(self: *const FixedSizeListType, other: *const FixedSizeListType) bool {
        return self.item_width == other.item_width and self.inner.eql(&other.inner);
    }
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
    /// Contains byte_width
    fixed_size_binary: i32,
    fixed_size_list: *const FixedSizeListType,
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

    pub fn eql(self: *const DataType, other: *const DataType) bool {
        if (@intFromEnum(self.*) != @intFromEnum(other.*)) {
            return false;
        }

        switch (self.*) {
            .null, .i8, .i16, .i32, .i64, .u8, .u16, .u32, .u64, .f16, .f32, .f64, .binary, .utf8, .bool, .date32, .date64, .interval_year_month, .interval_day_time, .interval_month_day_nano, .fixed_size_binary, .large_binary, .large_utf8, .binary_view, .utf8_view => return true,
            .decimal32 => |params| {
                const o_params = other.decimal32;
                return params.scale == o_params.scale and params.precision == o_params.precision;
            },
            .decimal64 => |params| {
                const o_params = other.decimal64;
                return params.scale == o_params.scale and params.precision == o_params.precision;
            },
            .decimal128 => |params| {
                const o_params = other.decimal128;
                return params.scale == o_params.scale and params.precision == o_params.precision;
            },
            .decimal256 => |params| {
                const o_params = other.decimal256;
                return params.scale == o_params.scale and params.precision == o_params.precision;
            },
            .time32 => |self_unit| return self_unit == other.time32,
            .time64 => |self_unit| return self_unit == other.time64,
            .timestamp => |self_ts| {
                const o_ts = other.timestamp;

                if (self_ts.timezone) |stz| {
                    if (o_ts.timezone) |otz| {
                        if (!std.mem.eql(u8, stz, otz)) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                } else if (o_ts.timezone != null) {
                    return false;
                }

                return self_ts.unit == o_ts.unit;
            },
            .duration => |self_unit| return self_unit == other.duration,
            .list => |self_dt| return self_dt.eql(other.list),
            .fixed_size_list => |self_dt| return self_dt.eql(other.fixed_size_list),
            .large_list => |self_dt| return self_dt.eql(other.large_list),
            .list_view => |self_dt| return self_dt.eql(other.list_view),
            .large_list_view => |self_dt| return self_dt.eql(other.large_list_view),
            .struct_ => |self_struct| return self_struct.eql(other.struct_),
            .dense_union => |self_union| return self_union.eql(other.dense_union),
            .sparse_union => |self_union| return self_union.eql(other.sparse_union),
            .map => |self_map| return self_map.eql(other.map),
            .run_end_encoded => |self_ree| return self_ree.eql(other.run_end_encoded),
            .dict => |self_dict| return self_dict.eql(other.dict),
        }
    }
};

fn check_union_data_type(array: *const arr.UnionArray, dt: *const UnionType) bool {
    if (!std.mem.eql(i8, dt.type_id_set, array.type_id_set)) {
        return false;
    }

    std.debug.assert(dt.field_names.len == dt.field_types.len);
    if (dt.field_names.len != array.field_names.len or dt.field_types.len != array.children.len) {
        return false;
    }

    for (dt.field_names, array.field_names) |dtfn, afn| {
        if (!std.mem.eql(u8, dtfn, afn)) {
            return false;
        }
    }

    for (dt.field_types, array.children) |*dtft, *afv| {
        if (!check_data_type(afv, dtft)) {
            return false;
        }
    }

    return true;
}

pub fn check_data_type(array: *const arr.Array, expected: *const DataType) bool {
    switch (array.*) {
        .null => {
            return expected.eql(&.{ .null = {} });
        },
        .i8 => {
            return expected.eql(&.{ .i8 = {} });
        },
        .i16 => {
            return expected.eql(&.{ .i16 = {} });
        },
        .i32 => {
            return expected.eql(&.{ .i32 = {} });
        },
        .i64 => {
            return expected.eql(&.{ .i64 = {} });
        },
        .u8 => {
            return expected.eql(&.{ .u8 = {} });
        },
        .u16 => {
            return expected.eql(&.{ .u16 = {} });
        },
        .u32 => {
            return expected.eql(&.{ .u32 = {} });
        },
        .u64 => {
            return expected.eql(&.{ .u64 = {} });
        },
        .f16 => {
            return expected.eql(&.{ .f16 = {} });
        },
        .f32 => {
            return expected.eql(&.{ .f32 = {} });
        },
        .f64 => {
            return expected.eql(&.{ .f64 = {} });
        },
        .binary => {
            return expected.eql(&.{ .binary = {} });
        },
        .large_binary => {
            return expected.eql(&.{ .large_binary = {} });
        },
        .utf8 => {
            return expected.eql(&.{ .utf8 = {} });
        },
        .large_utf8 => {
            return expected.eql(&.{ .large_utf8 = {} });
        },
        .bool => {
            return expected.eql(&.{ .bool = {} });
        },
        .binary_view => {
            return expected.eql(&.{ .binary_view = {} });
        },
        .utf8_view => {
            return expected.eql(&.{ .utf8_view = {} });
        },
        .decimal32 => |*a| {
            return expected.eql(&.{ .decimal32 = a.params });
        },
        .decimal64 => |*a| {
            return expected.eql(&.{ .decimal64 = a.params });
        },
        .decimal128 => |*a| {
            return expected.eql(&.{ .decimal128 = a.params });
        },
        .decimal256 => |*a| {
            return expected.eql(&.{ .decimal256 = a.params });
        },
        .fixed_size_binary => |*a| {
            return expected.eql(&.{ .fixed_size_binary = a.byte_width });
        },
        .date32 => {
            return expected.eql(&.{ .date32 = {} });
        },
        .date64 => {
            return expected.eql(&.{ .date64 = {} });
        },
        .time32 => |*a| {
            return expected.eql(&.{ .time32 = a.unit });
        },
        .time64 => |*a| {
            return expected.eql(&.{ .time64 = a.unit });
        },
        .timestamp => |*a| {
            return expected.eql(&.{ .timestamp = a.ts });
        },
        .duration => |*a| {
            return expected.eql(&.{ .duration = a.unit });
        },
        .interval_year_month => {
            return expected.eql(&.{ .interval_year_month = {} });
        },
        .interval_day_time => {
            return expected.eql(&.{ .interval_day_time = {} });
        },
        .interval_month_day_nano => {
            return expected.eql(&.{ .interval_month_day_nano = {} });
        },
        .list => |*a| {
            switch (expected.*) {
                .list => |dt| {
                    return check_data_type(a.inner, dt);
                },
                else => return false,
            }
        },
        .large_list => |*a| {
            switch (expected.*) {
                .large_list => |dt| {
                    return check_data_type(a.inner, dt);
                },
                else => return false,
            }
        },
        .list_view => |*a| {
            switch (expected.*) {
                .list_view => |dt| {
                    return check_data_type(a.inner, dt);
                },
                else => return false,
            }
        },
        .large_list_view => |*a| {
            switch (expected.*) {
                .large_list_view => |dt| {
                    return check_data_type(a.inner, dt);
                },
                else => return false,
            }
        },
        .fixed_size_list => |*a| {
            switch (expected.*) {
                .fixed_size_list => |dt| {
                    return dt.item_width == a.item_width and check_data_type(a.inner, &dt.inner);
                },
                else => return false,
            }
        },
        .struct_ => |*a| {
            switch (expected.*) {
                .struct_ => |dt| {
                    std.debug.assert(dt.field_names.len == dt.field_types.len);
                    if (dt.field_names.len != a.field_names.len or dt.field_types.len != a.field_values.len) {
                        return false;
                    }

                    for (dt.field_names, a.field_names) |dtfn, afn| {
                        if (!std.mem.eql(u8, dtfn, afn)) {
                            return false;
                        }
                    }

                    for (dt.field_types, a.field_values) |*dtft, *afv| {
                        if (!check_data_type(afv, dtft)) {
                            return false;
                        }
                    }

                    return true;
                },
                else => return false,
            }
        },
        .map => |*a| {
            switch (expected.*) {
                .map => |dt| {
                    return check_data_type(&a.entries.field_values[0], &dt.key.to_data_type()) and check_data_type(&a.entries.field_values[1], &dt.value);
                },
                else => return false,
            }
        },
        .dense_union => |*a| {
            switch (expected.*) {
                .dense_union => |dt| {
                    return check_union_data_type(&a.inner, dt);
                },
                else => return false,
            }
        },
        .sparse_union => |*a| {
            switch (expected.*) {
                .sparse_union => |dt| {
                    return check_union_data_type(&a.inner, dt);
                },
                else => return false,
            }
        },
        .run_end_encoded => |*a| {
            switch (expected.*) {
                .run_end_encoded => |dt| {
                    return check_data_type(a.run_ends, &dt.run_end.to_data_type()) and check_data_type(a.values, &dt.value);
                },
                else => return false,
            }
        },
        .dict => |*a| {
            switch (expected.*) {
                .dict => |dt| {
                    switch (a.keys) {
                        .i8 => {
                            if (dt.key != DictKeyType.i8) {
                                return false;
                            }
                        },
                        .i16 => {
                            if (dt.key != DictKeyType.i16) {
                                return false;
                            }
                        },
                        .i32 => {
                            if (dt.key != DictKeyType.i32) {
                                return false;
                            }
                        },
                        .i64 => {
                            if (dt.key != DictKeyType.i64) {
                                return false;
                            }
                        },
                        .u8 => {
                            if (dt.key != DictKeyType.u8) {
                                return false;
                            }
                        },
                        .u16 => {
                            if (dt.key != DictKeyType.u16) {
                                return false;
                            }
                        },
                        .u32 => {
                            if (dt.key != DictKeyType.u32) {
                                return false;
                            }
                        },
                        .u64 => {
                            if (dt.key != DictKeyType.u64) {
                                return false;
                            }
                        },
                    }

                    return check_data_type(a.values, &dt.value);
                },
                else => return false,
            }
        },
    }
}

test "DataType.eql - simple types" {
    const null_dt = DataType{ .null = {} };
    const i32_dt = DataType{ .i32 = {} };
    const bool_dt = DataType{ .bool = {} };

    // Same type
    try testing.expect(null_dt.eql(&null_dt));
    try testing.expect(i32_dt.eql(&i32_dt));
    try testing.expect(bool_dt.eql(&bool_dt));

    // Different types
    try testing.expect(!null_dt.eql(&i32_dt));
    try testing.expect(!i32_dt.eql(&bool_dt));
    try testing.expect(!bool_dt.eql(&null_dt));
}

test "DataType.eql - decimal types" {
    const decimal32_same = DataType{ .decimal32 = .{ .precision = 10, .scale = 2 } };
    const decimal32_diff = DataType{ .decimal32 = .{ .precision = 10, .scale = 3 } };
    const decimal64 = DataType{ .decimal64 = .{ .precision = 10, .scale = 2 } };

    // Same decimal32
    try testing.expect(decimal32_same.eql(&decimal32_same));
    // Different decimal32
    try testing.expect(!decimal32_same.eql(&decimal32_diff));
    // Different decimal type
    try testing.expect(!decimal32_same.eql(&decimal64));
}

test "DataType.eql - time and timestamp" {
    const time32_same = DataType{ .time32 = .second };
    const time32_diff = DataType{ .time32 = .millisecond };
    const timestamp_same = DataType{ .timestamp = .{ .unit = .microsecond, .timezone = "asd" } };
    const timestamp_diff = DataType{ .timestamp = .{ .unit = .nanosecond, .timezone = "qwe" } };

    // Same time32
    try testing.expect(time32_same.eql(&time32_same));
    // Different time32
    try testing.expect(!time32_same.eql(&time32_diff));
    // Same timestamp
    try testing.expect(timestamp_same.eql(&timestamp_same));
    // Different timestamp
    try testing.expect(!timestamp_same.eql(&timestamp_diff));
}

test "DataType.eql - recursive list types" {
    var inner_dt = DataType{ .i32 = {} };
    var list_dt = DataType{ .list = &inner_dt };
    var list_dt_same = DataType{ .list = &inner_dt };
    var inner_dt_diff = DataType{ .i64 = {} };
    var list_dt_diff = DataType{ .list = &inner_dt_diff };

    // Same list
    try testing.expect(list_dt.eql(&list_dt_same));
    // Different list
    try testing.expect(!list_dt.eql(&list_dt_diff));
    // Different type
    try testing.expect(!list_dt.eql(&inner_dt));
}

test "DataType.eql - struct type" {
    const field1 = DataType{ .i32 = {} };
    const field2 = DataType{ .f64 = {} };
    const fields = [_]DataType{ field1, field2 };
    const struct1 = StructType{ .field_types = &fields, .field_names = &.{ "asd", "qwe" } };
    const struct2 = StructType{ .field_types = &fields, .field_names = &.{ "asd", "qwe" } };
    const field3 = DataType{ .bool = {} };
    const fields_diff = [_]DataType{ field1, field3 };
    var struct_diff = StructType{ .field_types = &fields_diff, .field_names = &.{ "qwe", "asd" } };

    var struct_dt1 = DataType{ .struct_ = &struct1 };
    var struct_dt2 = DataType{ .struct_ = &struct2 };
    var struct_dt_diff = DataType{ .struct_ = &struct_diff };

    // Same struct
    try testing.expect(struct_dt1.eql(&struct_dt2));
    // Different struct
    try testing.expect(!struct_dt1.eql(&struct_dt_diff));
}

test "DataType.eql - run_end_encoded" {
    const run_ends = RunEndType.i32;
    const values = DataType{ .utf8 = {} };
    const ree1 = RunEndEncodedType{ .run_end = run_ends, .value = values };
    const ree2 = RunEndEncodedType{ .run_end = run_ends, .value = values };
    const values_diff = DataType{ .binary = {} };
    const ree_diff = RunEndEncodedType{ .run_end = run_ends, .value = values_diff };

    const ree_dt1 = DataType{ .run_end_encoded = &ree1 };
    const ree_dt2 = DataType{ .run_end_encoded = &ree2 };
    const ree_dt_diff = DataType{ .run_end_encoded = &ree_diff };

    // Same REE
    try testing.expect(ree_dt1.eql(&ree_dt2));
    // Different REE
    try testing.expect(!ree_dt1.eql(&ree_dt_diff));
}

test "DataType.eql - map type" {
    const key_dt = MapKeyType.utf8;
    const value_dt = DataType{ .i32 = {} };
    const map1 = MapType{ .key = key_dt, .value = value_dt };
    const map2 = MapType{ .key = key_dt, .value = value_dt };
    const value_dt_diff = DataType{ .i64 = {} };
    const map_diff = MapType{ .key = key_dt, .value = value_dt_diff };

    const map_dt1 = DataType{ .map = &map1 };
    const map_dt2 = DataType{ .map = &map2 };
    const map_dt_diff = DataType{ .map = &map_diff };

    // Same map
    try testing.expect(map_dt1.eql(&map_dt2));
    // Different map
    try testing.expect(!map_dt1.eql(&map_dt_diff));
}

test "DataType.eql - fixed_size_list type" {
    const same = DataType{ .fixed_size_list = &FixedSizeListType{ .item_width = 69, .inner = .{ .i32 = {} } } };
    const diff = DataType{ .fixed_size_list = &FixedSizeListType{ .item_width = 69, .inner = .{ .i64 = {} } } };

    try testing.expect(same.eql(&same));
    try testing.expect(diff.eql(&diff));

    try testing.expect(!same.eql(&diff));
    try testing.expect(!diff.eql(&same));
}

test check_data_type {
    try testing.expect(check_data_type(&arr.Array{ .i8 = arr.Int8Array{ .values = &.{}, .len = 0, .offset = 0, .validity = null, .null_count = 0 } }, &DataType{ .i8 = {} }));
}
