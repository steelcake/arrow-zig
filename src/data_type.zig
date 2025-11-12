const arr = @import("./array.zig");
const std = @import("std");
const Allocator = std.mem.Allocator;
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

    pub fn check(self: *const UnionType, array: *const arr.UnionArray) bool {
        return check_union_data_type(array, self);
    }
};

pub const MapKeyType = union(enum) {
    binary,
    large_binary,
    utf8,
    large_utf8,
    binary_view,
    utf8_view,
    // has byte_width
    fixed_size_binary: i32,
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,

    pub fn to_data_type(self: MapKeyType) DataType {
        return switch (self) {
            .binary => DataType{ .binary = {} },
            .large_binary => DataType{ .large_binary = {} },
            .utf8 => DataType{ .utf8 = {} },
            .large_utf8 => DataType{ .large_utf8 = {} },
            .binary_view => DataType{ .binary_view = {} },
            .utf8_view => DataType{ .utf8_view = {} },
            .fixed_size_binary => |byte_width| .{ .fixed_size_binary = byte_width },
            .i8 => .{ .i8 = {} },
            .i16 => .{ .i16 = {} },
            .i32 => .{ .i32 = {} },
            .i64 => .{ .i64 = {} },
            .u8 => .{ .u8 = {} },
            .u16 => .{ .u16 = {} },
            .u32 => .{ .u32 = {} },
            .u64 => .{ .u64 = {} },
        };
    }

    pub fn eql(self: *const MapKeyType, other: *const MapKeyType) bool {
        if (@intFromEnum(self.*) != @intFromEnum(other.*)) {
            return false;
        }

        switch (self.*) {
            .fixed_size_binary => |sbw| {
                if (sbw != other.fixed_size_binary) {
                    return false;
                }
            },
            else => {},
        }

        return true;
    }
};

pub const MapType = struct {
    key: MapKeyType,
    value: DataType,

    pub fn eql(self: *const MapType, other: *const MapType) bool {
        return self.key.eql(&other.key) and self.value.eql(&other.value);
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
            .i8 => .{ .i8 = {} },
            .i16 => .{ .i16 = {} },
            .i32 => .{ .i32 = {} },
            .i64 => .{ .i64 = {} },
            .u8 => .{ .u8 = {} },
            .u16 => .{ .u16 = {} },
            .u32 => .{ .u32 = {} },
            .u64 => .{ .u64 = {} },
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

fn get_union_type(array: *const arr.UnionArray, alloc: Allocator) Error!*const UnionType {
    const field_types = try alloc.alloc(DataType, array.children.len);
    for (array.children, 0..) |*field, idx| {
        field_types[idx] = try get_data_type(field, alloc);
    }

    const union_type = try alloc.create(UnionType);
    union_type.* = .{
        .type_id_set = array.type_id_set,
        .field_names = array.field_names,
        .field_types = field_types,
    };

    return union_type;
}

const Error = error{OutOfMemory};

/// Get data type of given struct.
/// Lifetime of the returned data type is tied to the lifetime of the given array
pub fn get_data_type(array: *const arr.Array, alloc: Allocator) Error!DataType {
    switch (array.*) {
        .null => {
            return .{ .null = {} };
        },
        .i8 => {
            return .{ .i8 = {} };
        },
        .i16 => {
            return .{ .i16 = {} };
        },
        .i32 => {
            return .{ .i32 = {} };
        },
        .i64 => {
            return .{ .i64 = {} };
        },
        .u8 => {
            return .{ .u8 = {} };
        },
        .u16 => {
            return .{ .u16 = {} };
        },
        .u32 => {
            return .{ .u32 = {} };
        },
        .u64 => {
            return .{ .u64 = {} };
        },
        .f16 => {
            return .{ .f16 = {} };
        },
        .f32 => {
            return .{ .f32 = {} };
        },
        .f64 => {
            return .{ .f64 = {} };
        },
        .binary => {
            return .{ .binary = {} };
        },
        .large_binary => {
            return .{ .large_binary = {} };
        },
        .utf8 => {
            return .{ .utf8 = {} };
        },
        .large_utf8 => {
            return .{ .large_utf8 = {} };
        },
        .bool => {
            return .{ .bool = {} };
        },
        .binary_view => {
            return .{ .binary_view = {} };
        },
        .utf8_view => {
            return .{ .utf8_view = {} };
        },
        .decimal32 => |*a| {
            return .{ .decimal32 = a.params };
        },
        .decimal64 => |*a| {
            return .{ .decimal64 = a.params };
        },
        .decimal128 => |*a| {
            return .{ .decimal128 = a.params };
        },
        .decimal256 => |*a| {
            return .{ .decimal256 = a.params };
        },
        .fixed_size_binary => |*a| {
            return .{ .fixed_size_binary = a.byte_width };
        },
        .date32 => {
            return .{ .date32 = {} };
        },
        .date64 => {
            return .{ .date64 = {} };
        },
        .time32 => |*a| {
            return .{ .time32 = a.unit };
        },
        .time64 => |*a| {
            return .{ .time64 = a.unit };
        },
        .timestamp => |*a| {
            return .{ .timestamp = a.ts };
        },
        .duration => |*a| {
            return .{ .duration = a.unit };
        },
        .interval_year_month => {
            return .{ .interval_year_month = {} };
        },
        .interval_day_time => {
            return .{ .interval_day_time = {} };
        },
        .interval_month_day_nano => {
            return .{ .interval_month_day_nano = {} };
        },
        .list => |*a| {
            const inner = try alloc.create(DataType);
            inner.* = try get_data_type(a.inner, alloc);
            return .{ .list = inner };
        },
        .large_list => |*a| {
            const inner = try alloc.create(DataType);
            inner.* = try get_data_type(a.inner, alloc);
            return .{ .large_list = inner };
        },
        .list_view => |*a| {
            const inner = try alloc.create(DataType);
            inner.* = try get_data_type(a.inner, alloc);
            return .{ .list_view = inner };
        },
        .large_list_view => |*a| {
            const inner = try alloc.create(DataType);
            inner.* = try get_data_type(a.inner, alloc);
            return .{ .large_list_view = inner };
        },
        .fixed_size_list => |*a| {
            const fsl_type = try alloc.create(FixedSizeListType);
            fsl_type.* = .{
                .inner = try get_data_type(a.inner, alloc),
                .item_width = a.item_width,
            };
            return .{ .fixed_size_list = fsl_type };
        },
        .struct_ => |*a| {
            const field_types = try alloc.alloc(DataType, a.field_values.len);
            for (a.field_values, 0..) |*field, idx| {
                field_types[idx] = try get_data_type(field, alloc);
            }

            const struct_type = try alloc.create(StructType);
            struct_type.* = StructType{ .field_names = a.field_names, .field_types = field_types };
            return .{ .struct_ = struct_type };
        },
        .map => |*a| {
            const key: MapKeyType = switch (a.entries.field_values[0]) {
                .binary => .binary,
                .large_binary => .large_binary,
                .utf8 => .utf8,
                .large_utf8 => .large_utf8,
                .binary_view => .binary_view,
                .utf8_view => .utf8_view,
                .fixed_size_binary => |*k| .{ .fixed_size_binary = k.byte_width },
                .i8 => .i8,
                .i16 => .i16,
                .i32 => .i32,
                .i64 => .i64,
                .u8 => .u8,
                .u16 => .u16,
                .u32 => .u32,
                .u64 => .u64,
                else => unreachable,
            };

            const value = try get_data_type(&a.entries.field_values[1], alloc);

            const map_type = try alloc.create(MapType);
            map_type.* = .{
                .key = key,
                .value = value,
            };

            return .{ .map = map_type };
        },
        .dense_union => |*a| {
            return .{ .dense_union = try get_union_type(&a.inner, alloc) };
        },
        .sparse_union => |*a| {
            return .{ .sparse_union = try get_union_type(&a.inner, alloc) };
        },
        .run_end_encoded => |*a| {
            const run_end: RunEndType = switch (a.run_ends.*) {
                .i16 => .i16,
                .i32 => .i32,
                .i64 => .i64,
                else => unreachable,
            };

            const value = try get_data_type(a.values, alloc);

            const ree_type = try alloc.create(RunEndEncodedType);
            ree_type.* = .{
                .run_end = run_end,
                .value = value,
            };

            return .{ .run_end_encoded = ree_type };
        },
        .dict => |*a| {
            const key: DictKeyType = switch (a.keys.*) {
                .i8 => .i8,
                .i16 => .i16,
                .i32 => .i32,
                .i64 => .i64,
                .u8 => .u8,
                .u16 => .u16,
                .u32 => .u32,
                .u64 => .u64,
                else => unreachable,
            };

            const value = try get_data_type(a.values, alloc);

            const dict_type = try alloc.create(DictType);
            dict_type.* = .{
                .key = key,
                .value = value,
            };

            return .{ .dict = dict_type };
        },
    }
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
                    return check_data_type(a.keys, &dt.key.to_data_type()) and check_data_type(a.values, &dt.value);
                },
                else => return false,
            }
        },
    }
}
