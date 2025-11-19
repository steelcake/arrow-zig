const arr = @import("./array.zig");
const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;

pub const StructType = struct {
    field_names: []const [:0]const u8,
    field_types: []const DataType,

    pub fn eql(self: *const StructType, other: *const StructType) Mismatch!void {
        std.debug.assert(self.field_names.len == self.field_types.len);
        if (self.field_names.len != other.field_names.len or self.field_types.len != other.field_types.len) {
            return Mismatch.Mismatch;
        }

        for (self.field_names, other.field_names) |sfn, ofn| {
            if (!std.mem.eql(u8, sfn, ofn)) {
                return Mismatch.Mismatch;
            }
        }

        for (self.field_types, other.field_types) |*sft, *oft| {
            try sft.eql(oft);
        }
    }
};

pub const UnionType = struct {
    type_id_set: []const i8,
    field_names: []const [:0]const u8,
    field_types: []const DataType,

    pub fn eql(self: *const UnionType, other: *const UnionType) Mismatch!void {
        std.debug.assert(self.field_names.len == self.field_types.len);

        if (self.field_names.len != other.field_names.len or self.field_types.len != other.field_types.len) {
            return Mismatch.Mismatch;
        }

        for (self.field_names, other.field_names) |sfn, ofn| {
            if (!std.mem.eql(u8, sfn, ofn)) {
                return Mismatch.Mismatch;
            }
        }

        for (self.field_types, other.field_types) |*sft, *oft| {
            try sft.eql(oft);
        }

        if (!std.mem.eql(i8, self.type_id_set, other.type_id_set)) {
            return Mismatch.Mismatch;
        }
    }

    pub fn check(self: *const UnionType, array: *const arr.UnionArray) Mismatch!void {
        try check_union_data_type(array, self);
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

    pub fn eql(self: *const MapKeyType, other: *const MapKeyType) Mismatch!void {
        if (@intFromEnum(self.*) != @intFromEnum(other.*)) {
            return Mismatch.Mismatch;
        }

        switch (self.*) {
            .fixed_size_binary => |sbw| {
                if (sbw != other.fixed_size_binary) {
                    return Mismatch.Mismatch;
                }
            },
            else => {},
        }
    }
};

pub const MapType = struct {
    key: MapKeyType,
    value: DataType,

    pub fn eql(self: *const MapType, other: *const MapType) Mismatch!void {
        try self.key.eql(&other.key);
        try self.value.eql(&other.value);
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

    pub fn eql(self: *const RunEndEncodedType, other: *const RunEndEncodedType) Mismatch!void {
        if (self.run_end != other.run_end) {
            return Mismatch.Mismatch;
        }
        try self.value.eql(&other.value);
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

    pub fn eql(self: *const DictType, other: *const DictType) Mismatch!void {
        if (self.key != other.key) {
            return Mismatch.Mismatch;
        }

        try self.value.eql(&other.value);
    }
};

pub const FixedSizeListType = struct {
    inner: DataType,
    item_width: i32,

    pub fn eql(self: *const FixedSizeListType, other: *const FixedSizeListType) Mismatch!void {
        if (self.item_width != other.item_width) {
            return Mismatch.Mismatch;
        }

        try self.inner.eql(&other.inner);
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

    pub fn eql(self: *const DataType, other: *const DataType) Mismatch!void {
        if (@intFromEnum(self.*) != @intFromEnum(other.*)) {
            return Mismatch.Mismatch;
        }

        switch (self.*) {
            .null,
            .i8,
            .i16,
            .i32,
            .i64,
            .u8,
            .u16,
            .u32,
            .u64,
            .f16,
            .f32,
            .f64,
            .binary,
            .utf8,
            .bool,
            .date32,
            .date64,
            .interval_year_month,
            .interval_day_time,
            .interval_month_day_nano,
            .fixed_size_binary,
            .large_binary,
            .large_utf8,
            .binary_view,
            .utf8_view,
            => {},
            .decimal32 => |params| {
                const o_params = other.decimal32;
                if (params.scale != o_params.scale or params.precision != o_params.precision) {
                    return Mismatch.Mismatch;
                }
            },
            .decimal64 => |params| {
                const o_params = other.decimal64;
                if (params.scale != o_params.scale or params.precision != o_params.precision) {
                    return Mismatch.Mismatch;
                }
            },
            .decimal128 => |params| {
                const o_params = other.decimal128;
                if (params.scale != o_params.scale or params.precision != o_params.precision) {
                    return Mismatch.Mismatch;
                }
            },
            .decimal256 => |params| {
                const o_params = other.decimal256;
                if (params.scale != o_params.scale or params.precision != o_params.precision) {
                    return Mismatch.Mismatch;
                }
            },
            .time32 => |self_unit| {
                if (self_unit != other.time32) {
                    return Mismatch.Mismatch;
                }
            },
            .time64 => |self_unit| {
                if (self_unit != other.time64) {
                    return Mismatch.Mismatch;
                }
            },
            .timestamp => |self_ts| {
                const o_ts = other.timestamp;

                if (self_ts.timezone) |stz| {
                    if (o_ts.timezone) |otz| {
                        if (!std.mem.eql(u8, stz, otz)) {
                            return Mismatch.Mismatch;
                        }
                    } else {
                        return Mismatch.Mismatch;
                    }
                } else if (o_ts.timezone != null) {
                    return Mismatch.Mismatch;
                }

                if (self_ts.unit != o_ts.unit) {
                    return Mismatch.Mismatch;
                }
            },
            .duration => |self_unit| {
                if (self_unit != other.duration) {
                    return Mismatch.Mismatch;
                }
            },
            .list => |self_dt| try self_dt.eql(other.list),
            .fixed_size_list => |self_dt| try self_dt.eql(other.fixed_size_list),
            .large_list => |self_dt| try self_dt.eql(other.large_list),
            .list_view => |self_dt| try self_dt.eql(other.list_view),
            .large_list_view => |self_dt| try self_dt.eql(other.large_list_view),
            .struct_ => |self_struct| try self_struct.eql(other.struct_),
            .dense_union => |self_union| try self_union.eql(other.dense_union),
            .sparse_union => |self_union| try self_union.eql(other.sparse_union),
            .map => |self_map| try self_map.eql(other.map),
            .run_end_encoded => |self_ree| try self_ree.eql(other.run_end_encoded),
            .dict => |self_dict| try self_dict.eql(other.dict),
        }
    }
};

fn check_union_data_type(array: *const arr.UnionArray, dt: *const UnionType) Mismatch!void {
    if (!std.mem.eql(i8, dt.type_id_set, array.type_id_set)) {
        return Mismatch.Mismatch;
    }

    std.debug.assert(dt.field_names.len == dt.field_types.len);
    if (dt.field_names.len != array.field_names.len or dt.field_types.len != array.children.len) {
        return Mismatch.Mismatch;
    }

    for (dt.field_names, array.field_names) |dtfn, afn| {
        if (!std.mem.eql(u8, dtfn, afn)) {
            return Mismatch.Mismatch;
        }
    }

    for (dt.field_types, array.children) |*dtft, *afv| {
        try check_data_type(afv, dtft);
    }
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

pub const Mismatch = error{Mismatch};

pub fn check_data_type(array: *const arr.Array, expected: *const DataType) Mismatch!void {
    switch (array.*) {
        .null => {
            try expected.eql(&.{ .null = {} });
        },
        .i8 => {
            try expected.eql(&.{ .i8 = {} });
        },
        .i16 => {
            try expected.eql(&.{ .i16 = {} });
        },
        .i32 => {
            try expected.eql(&.{ .i32 = {} });
        },
        .i64 => {
            try expected.eql(&.{ .i64 = {} });
        },
        .u8 => {
            try expected.eql(&.{ .u8 = {} });
        },
        .u16 => {
            try expected.eql(&.{ .u16 = {} });
        },
        .u32 => {
            try expected.eql(&.{ .u32 = {} });
        },
        .u64 => {
            try expected.eql(&.{ .u64 = {} });
        },
        .f16 => {
            try expected.eql(&.{ .f16 = {} });
        },
        .f32 => {
            try expected.eql(&.{ .f32 = {} });
        },
        .f64 => {
            try expected.eql(&.{ .f64 = {} });
        },
        .binary => {
            try expected.eql(&.{ .binary = {} });
        },
        .large_binary => {
            try expected.eql(&.{ .large_binary = {} });
        },
        .utf8 => {
            try expected.eql(&.{ .utf8 = {} });
        },
        .large_utf8 => {
            try expected.eql(&.{ .large_utf8 = {} });
        },
        .bool => {
            try expected.eql(&.{ .bool = {} });
        },
        .binary_view => {
            try expected.eql(&.{ .binary_view = {} });
        },
        .utf8_view => {
            try expected.eql(&.{ .utf8_view = {} });
        },
        .decimal32 => |*a| {
            try expected.eql(&.{ .decimal32 = a.params });
        },
        .decimal64 => |*a| {
            try expected.eql(&.{ .decimal64 = a.params });
        },
        .decimal128 => |*a| {
            try expected.eql(&.{ .decimal128 = a.params });
        },
        .decimal256 => |*a| {
            try expected.eql(&.{ .decimal256 = a.params });
        },
        .fixed_size_binary => |*a| {
            try expected.eql(&.{ .fixed_size_binary = a.byte_width });
        },
        .date32 => {
            try expected.eql(&.{ .date32 = {} });
        },
        .date64 => {
            try expected.eql(&.{ .date64 = {} });
        },
        .time32 => |*a| {
            try expected.eql(&.{ .time32 = a.unit });
        },
        .time64 => |*a| {
            try expected.eql(&.{ .time64 = a.unit });
        },
        .timestamp => |*a| {
            try expected.eql(&.{ .timestamp = a.ts });
        },
        .duration => |*a| {
            try expected.eql(&.{ .duration = a.unit });
        },
        .interval_year_month => {
            try expected.eql(&.{ .interval_year_month = {} });
        },
        .interval_day_time => {
            try expected.eql(&.{ .interval_day_time = {} });
        },
        .interval_month_day_nano => {
            try expected.eql(&.{ .interval_month_day_nano = {} });
        },
        .list => |*a| {
            switch (expected.*) {
                .list => |dt| {
                    try check_data_type(a.inner, dt);
                },
                else => return Mismatch.Mismatch,
            }
        },
        .large_list => |*a| {
            switch (expected.*) {
                .large_list => |dt| {
                    try check_data_type(a.inner, dt);
                },
                else => return Mismatch.Mismatch,
            }
        },
        .list_view => |*a| {
            switch (expected.*) {
                .list_view => |dt| {
                    try check_data_type(a.inner, dt);
                },
                else => return Mismatch.Mismatch,
            }
        },
        .large_list_view => |*a| {
            switch (expected.*) {
                .large_list_view => |dt| {
                    try check_data_type(a.inner, dt);
                },
                else => return Mismatch.Mismatch,
            }
        },
        .fixed_size_list => |*a| {
            switch (expected.*) {
                .fixed_size_list => |dt| {
                    if (dt.item_width != a.item_width) {
                        return Mismatch.Mismatch;
                    }
                    try check_data_type(a.inner, &dt.inner);
                },
                else => return Mismatch.Mismatch,
            }
        },
        .struct_ => |*a| {
            switch (expected.*) {
                .struct_ => |dt| {
                    std.debug.assert(dt.field_names.len == dt.field_types.len);
                    if (dt.field_names.len != a.field_names.len or dt.field_types.len != a.field_values.len) {
                        return Mismatch.Mismatch;
                    }

                    for (dt.field_names, a.field_names) |dtfn, afn| {
                        if (!std.mem.eql(u8, dtfn, afn)) {
                            return Mismatch.Mismatch;
                        }
                    }

                    for (dt.field_types, a.field_values) |*dtft, *afv| {
                        try check_data_type(afv, dtft);
                    }
                },
                else => return Mismatch.Mismatch,
            }
        },
        .map => |*a| {
            switch (expected.*) {
                .map => |dt| {
                    try check_data_type(&a.entries.field_values[0], &dt.key.to_data_type());
                    try check_data_type(&a.entries.field_values[1], &dt.value);
                },
                else => return Mismatch.Mismatch,
            }
        },
        .dense_union => |*a| {
            switch (expected.*) {
                .dense_union => |dt| {
                    try check_union_data_type(&a.inner, dt);
                },
                else => return Mismatch.Mismatch,
            }
        },
        .sparse_union => |*a| {
            switch (expected.*) {
                .sparse_union => |dt| {
                    try check_union_data_type(&a.inner, dt);
                },
                else => return Mismatch.Mismatch,
            }
        },
        .run_end_encoded => |*a| {
            switch (expected.*) {
                .run_end_encoded => |dt| {
                    try check_data_type(a.run_ends, &dt.run_end.to_data_type());
                    try check_data_type(a.values, &dt.value);
                },
                else => return Mismatch.Mismatch,
            }
        },
        .dict => |*a| {
            switch (expected.*) {
                .dict => |dt| {
                    try check_data_type(a.keys, &dt.key.to_data_type());
                    try check_data_type(a.values, &dt.value);
                },
                else => return Mismatch.Mismatch,
            }
        },
    }
}

pub fn empty_primitive_array(comptime T: type) arr.PrimitiveArray(T) {
    return arr.PrimitiveArray(T){
        .len = 0,
        .values = &.{},
        .offset = 0,
        .validity = null,
        .null_count = 0,
    };
}

pub fn empty_binary_array(
    comptime index_t: arr.IndexType,
    alloc: Allocator,
) error{OutOfMemory}!arr.GenericBinaryArray(index_t) {
    const offsets = try alloc.alloc(index_t.to_type(), 1);
    offsets[0] = 0;

    return arr.GenericBinaryArray(index_t){
        .validity = null,
        .offset = 0,
        .len = 0,
        .offsets = offsets,
        .data = &.{},
        .null_count = 0,
    };
}

pub fn empty_null_array() arr.NullArray {
    return arr.NullArray{
        .len = 0,
    };
}

pub fn empty_bool_array() arr.BoolArray {
    return arr.BoolArray{
        .null_count = 0,
        .len = 0,
        .offset = 0,
        .validity = null,
        .values = &.{},
    };
}

pub fn empty_binary_view_array() arr.BinaryViewArray {
    return arr.BinaryViewArray{
        .validity = null,
        .offset = 0,
        .len = 0,
        .null_count = 0,
        .views = &.{},
        .buffers = &.{},
    };
}

pub fn empty_decimal_array(comptime dec_t: arr.DecimalInt, params: arr.DecimalParams) arr.DecimalArray(dec_t) {
    return arr.DecimalArray(dec_t){
        .inner = empty_primitive_array(dec_t.to_type()),
        .params = params,
    };
}

pub fn empty_fixed_size_binary_array(byte_width: i32) arr.FixedSizeBinaryArray {
    return arr.FixedSizeBinaryArray{
        .null_count = 0,
        .len = 0,
        .offset = 0,
        .validity = null,
        .data = &.{},
        .byte_width = byte_width,
    };
}

pub fn empty_date_array(comptime backing_t: arr.IndexType) arr.DateArray(backing_t) {
    return arr.DateArray(backing_t){
        .inner = empty_primitive_array(backing_t.to_type()),
    };
}

pub fn empty_time_array(
    comptime backing_t: arr.IndexType,
    unit: arr.TimeArray(backing_t).Unit,
) arr.TimeArray(backing_t) {
    return arr.TimeArray(backing_t){
        .inner = empty_primitive_array(backing_t.to_type()),
        .unit = unit,
    };
}

pub fn empty_timestamp_array(ts: arr.Timestamp) arr.TimestampArray {
    return arr.TimestampArray{
        .inner = empty_primitive_array(i64),
        .ts = ts,
    };
}

pub fn empty_duration_array(unit: arr.TimestampUnit) arr.DurationArray {
    return arr.DurationArray{
        .unit = unit,
        .inner = empty_primitive_array(i64),
    };
}

pub fn empty_interval_array(comptime interval_t: arr.IntervalType) arr.IntervalArray(interval_t) {
    return arr.IntervalArray(interval_t){
        .inner = empty_primitive_array(interval_t.to_type()),
    };
}

pub fn empty_list_array(
    comptime index_t: arr.IndexType,
    inner_t: *const DataType,
    alloc: Allocator,
) error{OutOfMemory}!arr.GenericListArray(index_t) {
    const offsets = try alloc.alloc(index_t.to_type(), 1);
    offsets[0] = 0;

    const inner = try alloc.create(arr.Array);
    inner.* = try empty_array(inner_t, alloc);

    return arr.GenericListArray(index_t){
        .inner = inner,
        .validity = null,
        .offset = 0,
        .len = 0,
        .null_count = 0,
        .offsets = offsets,
    };
}

pub fn empty_list_view_array(
    comptime index_t: arr.IndexType,
    inner_t: *const DataType,
    alloc: Allocator,
) error{OutOfMemory}!arr.GenericListViewArray(index_t) {
    const inner = try alloc.create(arr.Array);
    inner.* = try empty_array(inner_t, alloc);

    return arr.GenericListViewArray(index_t){
        .inner = inner,
        .null_count = 0,
        .len = 0,
        .offset = 0,
        .validity = null,
        .offsets = &.{},
        .sizes = &.{},
    };
}

pub fn empty_fixed_size_list_array(
    dt: *const FixedSizeListType,
    alloc: Allocator,
) error{OutOfMemory}!arr.FixedSizeListArray {
    const inner = try alloc.create(arr.Array);
    inner.* = try empty_array(&dt.inner, alloc);

    return arr.FixedSizeListArray{
        .validity = null,
        .offset = 0,
        .len = 0,
        .null_count = 0,
        .inner = inner,
        .item_width = dt.item_width,
    };
}

pub fn empty_struct_array(
    dt: *const StructType,
    alloc: Allocator,
) error{OutOfMemory}!arr.StructArray {
    const field_names = try alloc.alloc([:0]const u8, dt.field_names.len);
    for (0..field_names.len) |fn_idx| {
        field_names[fn_idx] = try alloc.dupeZ(u8, dt.field_names[fn_idx]);
    }

    const field_values = try alloc.alloc(arr.Array, dt.field_types.len);
    for (0..field_values.len) |fv_idx| {
        field_values[fv_idx] = try empty_array(&dt.field_types[fv_idx], alloc);
    }

    return arr.StructArray{
        .null_count = 0,
        .len = 0,
        .offset = 0,
        .validity = null,
        .field_names = field_names,
        .field_values = field_values,
    };
}

pub fn empty_map_array(
    dt: *const MapType,
    alloc: Allocator,
) error{OutOfMemory}!arr.MapArray {
    const offsets = try alloc.alloc(i32, 1);
    offsets[0] = 0;

    const entries = try alloc.create(arr.StructArray);
    entries.* = try empty_struct_array(
        &StructType{
            .field_types = &.{ dt.key.to_data_type(), dt.value },
            .field_names = &.{ "keys", "values" },
        },
        alloc,
    );

    return arr.MapArray{
        .keys_are_sorted = false,
        .offset = 0,
        .len = 0,
        .null_count = 0,
        .validity = null,
        .offsets = offsets,
        .entries = entries,
    };
}

pub fn empty_union_array(dt: *const UnionType, alloc: Allocator) error{OutOfMemory}!arr.UnionArray {
    const field_names = try alloc.alloc([:0]const u8, dt.field_names.len);
    for (0..field_names.len) |fn_idx| {
        field_names[fn_idx] = try alloc.dupeZ(u8, dt.field_names[fn_idx]);
    }

    const children = try alloc.alloc(arr.Array, dt.field_types.len);
    for (0..children.len) |c_idx| {
        children[c_idx] = try empty_array(&dt.field_types[c_idx], alloc);
    }

    const type_id_set = try alloc.dupe(i8, dt.type_id_set);

    return arr.UnionArray{
        .len = 0,
        .offset = 0,
        .field_names = field_names,
        .children = children,
        .type_id_set = type_id_set,
        .type_ids = &.{},
    };
}

pub fn empty_run_end_encoded_array(
    dt: *const RunEndEncodedType,
    alloc: Allocator,
) error{OutOfMemory}!arr.RunEndArray {
    const run_ends = try alloc.create(arr.Array);
    run_ends.* = try empty_array(&dt.run_end.to_data_type(), alloc);

    const values = try alloc.create(arr.Array);
    values.* = try empty_array(&dt.value, alloc);

    return arr.RunEndArray{
        .run_ends = run_ends,
        .values = values,
        .offset = 0,
        .len = 0,
    };
}

pub fn empty_dict_array(
    dt: *const DictType,
    alloc: Allocator,
) error{OutOfMemory}!arr.DictArray {
    const keys = try alloc.create(arr.Array);
    keys.* = try empty_array(&dt.key.to_data_type(), alloc);

    const values = try alloc.create(arr.Array);
    values.* = try empty_array(&dt.value, alloc);

    return arr.DictArray{
        .is_ordered = false,
        .offset = 0,
        .len = 0,
        .keys = keys,
        .values = values,
    };
}

pub fn empty_array(dt: *const DataType, alloc: Allocator) error{OutOfMemory}!arr.Array {
    return switch (dt.*) {
        .null => .{ .null = empty_null_array() },
        .i8 => .{ .i8 = empty_primitive_array(i8) },
        .i16 => .{ .i16 = empty_primitive_array(i16) },
        .i32 => .{ .i32 = empty_primitive_array(i32) },
        .i64 => .{ .i64 = empty_primitive_array(i64) },
        .u8 => .{ .u8 = empty_primitive_array(u8) },
        .u16 => .{ .u16 = empty_primitive_array(u16) },
        .u32 => .{ .u32 = empty_primitive_array(u32) },
        .u64 => .{ .u64 = empty_primitive_array(u64) },
        .f16 => .{ .f16 = empty_primitive_array(f16) },
        .f32 => .{ .f32 = empty_primitive_array(f32) },
        .f64 => .{ .f64 = empty_primitive_array(f64) },
        .binary => .{ .binary = try empty_binary_array(.i32, alloc) },
        .large_binary => .{ .large_binary = try empty_binary_array(.i64, alloc) },
        .utf8 => .{ .utf8 = .{ .inner = try empty_binary_array(.i32, alloc) } },
        .large_utf8 => .{ .large_utf8 = .{ .inner = try empty_binary_array(.i64, alloc) } },
        .bool => .{ .bool = empty_bool_array() },
        .binary_view => .{ .binary_view = empty_binary_view_array() },
        .utf8_view => .{ .utf8_view = .{ .inner = empty_binary_view_array() } },
        .decimal32 => |a| .{ .decimal32 = empty_decimal_array(.i32, a) },
        .decimal64 => |a| .{ .decimal64 = empty_decimal_array(.i64, a) },
        .decimal128 => |a| .{ .decimal128 = empty_decimal_array(.i128, a) },
        .decimal256 => |a| .{ .decimal256 = empty_decimal_array(.i256, a) },
        .fixed_size_binary => |a| .{ .fixed_size_binary = empty_fixed_size_binary_array(a) },
        .date32 => .{ .date32 = empty_date_array(.i32) },
        .date64 => .{ .date64 = empty_date_array(.i64) },
        .time32 => |a| .{ .time32 = empty_time_array(.i32, a) },
        .time64 => |a| .{ .time64 = empty_time_array(.i64, a) },
        .timestamp => |a| .{ .timestamp = empty_timestamp_array(a) },
        .duration => |a| .{ .duration = empty_duration_array(a) },
        .interval_year_month => .{ .interval_year_month = empty_interval_array(.year_month) },
        .interval_day_time => .{
            .interval_day_time = empty_interval_array(.day_time),
        },
        .interval_month_day_nano => .{
            .interval_month_day_nano = empty_interval_array(.month_day_nano),
        },
        .list => |a| .{ .list = try empty_list_array(.i32, a, alloc) },
        .large_list => |a| .{ .large_list = try empty_list_array(.i64, a, alloc) },
        .list_view => |a| .{ .list_view = try empty_list_view_array(.i32, a, alloc) },
        .large_list_view => |a| .{ .large_list_view = try empty_list_view_array(.i64, a, alloc) },
        .fixed_size_list => |a| .{ .fixed_size_list = try empty_fixed_size_list_array(a, alloc) },
        .struct_ => |a| .{ .struct_ = try empty_struct_array(a, alloc) },
        .map => |a| .{ .map = try empty_map_array(a, alloc) },
        .dense_union => |a| .{
            .dense_union = arr.DenseUnionArray{
                .offsets = &.{},
                .inner = try empty_union_array(a, alloc),
            },
        },
        .sparse_union => |a| .{
            .sparse_union = arr.SparseUnionArray{
                .inner = try empty_union_array(a, alloc),
            },
        },
        .run_end_encoded => |a| .{ .run_end_encoded = try empty_run_end_encoded_array(a, alloc) },
        .dict => |a| .{ .dict = try empty_dict_array(a, alloc) },
    };
}

pub fn all_null_validity(len: u32, alloc: Allocator) error{OutOfMemory}![]const u8 {
    const v = try alloc.alloc(u8, (len + 7) / 8);
    @memset(v, 0);
    return v;
}

pub fn all_null_primitive_array(
    comptime T: type,
    len: u32,
    alloc: Allocator,
) error{OutOfMemory}!arr.PrimitiveArray(T) {
    const values = try alloc.alloc(T, len);
    @memset(@as([]u8, @ptrCast(values)), 0);

    return arr.PrimitiveArray(T){
        .len = len,
        .values = values,
        .offset = 0,
        .validity = try all_null_validity(len, alloc),
        .null_count = len,
    };
}

pub fn all_null_binary_array(
    comptime index_t: arr.IndexType,
    len: u32,
    alloc: Allocator,
) error{OutOfMemory}!arr.GenericBinaryArray(index_t) {
    const offsets = try alloc.alloc(index_t.to_type(), len + 1);
    @memset(offsets, 0);

    return arr.GenericBinaryArray(index_t){
        .validity = try all_null_validity(len, alloc),
        .offset = 0,
        .len = len,
        .offsets = offsets,
        .data = &.{},
        .null_count = len,
    };
}

pub fn all_null_null_array(len: u32) arr.NullArray {
    return arr.NullArray{
        .len = len,
    };
}

pub fn all_null_bool_array(len: u32, alloc: Allocator) error{OutOfMemory}!arr.BoolArray {
    return arr.BoolArray{
        .null_count = len,
        .len = len,
        .offset = 0,
        .validity = try all_null_validity(len, alloc),
        .values = try all_null_validity(len, alloc),
    };
}

pub fn all_null_binary_view_array(len: u32, alloc: Allocator) error{OutOfMemory}!arr.BinaryViewArray {
    const views = try alloc.alloc(arr.BinaryView, len);
    @memset(@as([]u8, @ptrCast(views)), 0);

    return arr.BinaryViewArray{
        .validity = try all_null_validity(len, alloc),
        .offset = 0,
        .len = len,
        .null_count = len,
        .views = views,
        .buffers = &.{},
    };
}

pub fn all_null_decimal_array(
    comptime dec_t: arr.DecimalInt,
    params: arr.DecimalParams,
    len: u32,
    alloc: Allocator,
) error{OutOfMemory}!arr.DecimalArray(dec_t) {
    return arr.DecimalArray(dec_t){
        .inner = all_null_primitive_array(dec_t.to_type(), len, alloc),
        .params = params,
    };
}

pub fn all_null_fixed_size_binary_array(
    byte_width: i32,
    len: u32,
    alloc: Allocator,
) error{OutOfMemory}!arr.FixedSizeBinaryArray {
    const bw: u32 = @intCast(byte_width);
    const data = try alloc.alloc(u8, bw * len);
    @memset(data, 0);

    return arr.FixedSizeBinaryArray{
        .null_count = len,
        .len = len,
        .offset = 0,
        .validity = try all_null_validity(len, alloc),
        .data = data,
        .byte_width = byte_width,
    };
}

pub fn all_null_date_array(
    comptime backing_t: arr.IndexType,
    len: u32,
    alloc: Allocator,
) error{OutOfMemory}!arr.DateArray(backing_t) {
    return arr.DateArray(backing_t){
        .inner = try empty_primitive_array(backing_t.to_type(), len, alloc),
    };
}

pub fn all_null_time_array(
    comptime backing_t: arr.IndexType,
    unit: arr.TimeArray(backing_t).Unit,
    len: u32,
    alloc: Allocator,
) error{OutOfMemory}!arr.TimeArray(backing_t) {
    return arr.TimeArray(backing_t){
        .inner = try empty_primitive_array(backing_t.to_type(), len, alloc),
        .unit = unit,
    };
}

pub fn all_null_timestamp_array(
    ts: arr.Timestamp,
    len: u32,
    alloc: Allocator,
) error{OutOfMemory}!arr.TimestampArray {
    return arr.TimestampArray{
        .inner = try empty_primitive_array(i64, len, alloc),
        .ts = ts,
    };
}

pub fn all_null_duration_array(
    unit: arr.TimestampUnit,
    len: u32,
    alloc: Allocator,
) error{OutOfMemory}!arr.DurationArray {
    return arr.DurationArray{
        .unit = unit,
        .inner = try empty_primitive_array(i64, len, alloc),
    };
}

pub fn all_null_interval_array(
    comptime interval_t: arr.IntervalType,
    len: u32,
    alloc: Allocator,
) error{OutOfMemory}!arr.IntervalArray(interval_t) {
    return arr.IntervalArray(interval_t){
        .inner = try empty_primitive_array(interval_t.to_type(), len, alloc),
    };
}

pub fn all_null_list_array(
    comptime index_t: arr.IndexType,
    inner_t: *const DataType,
    len: u32,
    alloc: Allocator,
) error{OutOfMemory}!arr.GenericListArray(index_t) {
    const offsets = try alloc.alloc(index_t.to_type(), len + 1);
    @memset(offsets, 0);

    const inner = try alloc.create(arr.Array);
    inner.* = try empty_array(inner_t, alloc);

    return arr.GenericListArray(index_t){
        .inner = inner,
        .validity = try all_null_validity(len, alloc),
        .offset = 0,
        .len = len,
        .null_count = len,
        .offsets = offsets,
    };
}

pub fn all_null_list_view_array(
    comptime index_t: arr.IndexType,
    inner_t: *const DataType,
    len: u32,
    alloc: Allocator,
) error{OutOfMemory}!arr.GenericListViewArray(index_t) {
    const I = index_t.to_type();

    const inner = try alloc.create(arr.Array);
    inner.* = try empty_array(inner_t, alloc);

    const offsets = try alloc.alloc(I, len);
    @memset(offsets, 0);

    const sizes = try alloc.alloc(I, len);
    @memset(offsets, 0);

    return arr.GenericListViewArray(index_t){
        .inner = inner,
        .null_count = len,
        .len = len,
        .offset = 0,
        .validity = try all_null_validity(len, alloc),
        .offsets = offsets,
        .sizes = sizes,
    };
}

pub fn all_null_fixed_size_list_array(
    dt: *const FixedSizeListType,
    len: u32,
    alloc: Allocator,
) error{OutOfMemory}!arr.FixedSizeListArray {
    const iw: u32 = @intCast(dt.item_width);

    const inner = try alloc.create(arr.Array);
    inner.* = try all_null_array(&dt.inner, len * iw, alloc);

    return arr.FixedSizeListArray{
        .validity = try all_null_validity(len, alloc),
        .offset = 0,
        .len = len,
        .null_count = len,
        .inner = inner,
        .item_width = dt.item_width,
    };
}

pub fn all_null_struct_array(
    dt: *const StructType,
    len: u32,
    alloc: Allocator,
) error{OutOfMemory}!arr.StructArray {
    const field_names = try alloc.alloc([:0]const u8, dt.field_names.len);
    for (0..field_names.len) |fn_idx| {
        field_names[fn_idx] = try alloc.dupeZ(u8, dt.field_names[fn_idx]);
    }

    const field_values = try alloc.alloc(arr.Array, dt.field_types.len);
    for (0..field_values.len) |fv_idx| {
        field_values[fv_idx] = try all_null_array(&dt.field_types[fv_idx], len, alloc);
    }

    return arr.StructArray{
        .null_count = len,
        .len = len,
        .offset = 0,
        .validity = try all_null_validity(len, alloc),
        .field_names = field_names,
        .field_values = field_values,
    };
}

pub fn all_null_map_array(
    dt: *const MapType,
    len: u32,
    alloc: Allocator,
) error{OutOfMemory}!arr.MapArray {
    const offsets = try alloc.alloc(i32, len + 1);
    @memset(offsets, 0);

    const entries = try alloc.create(arr.StructArray);
    entries.* = try empty_struct_array(
        &StructType{
            .field_types = &.{ dt.key.to_data_type(), dt.value },
            .field_names = &.{ "keys", "values" },
        },
        alloc,
    );

    return arr.MapArray{
        .keys_are_sorted = false,
        .offset = 0,
        .len = len,
        .null_count = len,
        .validity = try all_null_validity(len, alloc),
        .offsets = offsets,
        .entries = entries,
    };
}

pub fn all_null_sparse_union_array(
    dt: *const UnionType,
    len: u32,
    alloc: Allocator,
) error{OutOfMemory}!arr.SparseUnionArray {
    const field_names = try alloc.alloc([:0]const u8, dt.field_names.len);
    for (0..field_names.len) |fn_idx| {
        field_names[fn_idx] = try alloc.dupeZ(u8, dt.field_names[fn_idx]);
    }

    const children = try alloc.alloc(arr.Array, dt.field_types.len);
    for (0..children.len) |c_idx| {
        children[c_idx] = try all_null_array(&dt.field_types[c_idx], len, alloc);
    }

    const type_id_set = try alloc.dupe(i8, dt.type_id_set);

    const type_ids = try alloc.alloc(i8, len);
    @memset(type_ids, type_id_set[0]);

    const inner = arr.UnionArray{
        .len = len,
        .offset = 0,
        .field_names = field_names,
        .children = children,
        .type_id_set = type_id_set,
        .type_ids = type_ids,
    };

    return arr.SparseUnionArray{
        .inner = inner,
    };
}

pub fn all_null_dense_union_array(
    dt: *const UnionType,
    len: u32,
    alloc: Allocator,
) error{OutOfMemory}!arr.DenseUnionArray {
    const field_names = try alloc.alloc([:0]const u8, dt.field_names.len);
    for (0..field_names.len) |fn_idx| {
        field_names[fn_idx] = try alloc.dupeZ(u8, dt.field_names[fn_idx]);
    }

    const children = try alloc.alloc(arr.Array, dt.field_types.len);
    children[0] = try all_null_array(&dt.field_types[0], 1, alloc);
    for (1..children.len) |c_idx| {
        children[c_idx] = try empty_array(&dt.field_types[c_idx], alloc);
    }

    const type_id_set = try alloc.dupe(i8, dt.type_id_set);

    const type_ids = try alloc.alloc(i8, len);
    @memset(type_ids, type_id_set[0]);

    const inner = arr.UnionArray{
        .len = len,
        .offset = 0,
        .field_names = field_names,
        .children = children,
        .type_id_set = type_id_set,
        .type_ids = type_ids,
    };

    const offsets = try alloc.alloc(i32, len);
    @memset(offsets, 0);

    return arr.DenseUnionArray{
        .inner = inner,
        .offsets = offsets,
    };
}

pub fn all_null_run_end_encoded_array(
    dt: *const RunEndEncodedType,
    len: u32,
    alloc: Allocator,
) error{OutOfMemory}!arr.RunEndArray {
    const MakeRunEnds = struct {
        fn make_run_ends(
            comptime T: type,
            len_i: u32,
            alloc_i: Allocator,
        ) error{OutOfMemory}!arr.PrimitiveArray(T) {
            const re = try alloc_i.alloc(T, len_i);
            var v: T = 1;
            for (0..re.len) |idx| {
                re[idx] = v;
                v += 1;
            }

            return arr.PrimitiveArray(T){
                .len = len_i,
                .values = re,
                .offset = 0,
                .validity = null,
                .null_count = 0,
            };
        }
    };

    const run_ends = try alloc.create(arr.Array);
    run_ends.* = switch (dt.run_end) {
        .i16 => try MakeRunEnds.make_run_ends(i16, len, alloc),
        .i32 => try MakeRunEnds.make_run_ends(i32, len, alloc),
        .i64 => try MakeRunEnds.make_run_ends(i64, len, alloc),
    };

    const values = try alloc.create(arr.Array);
    values.* = try all_null_array(&dt.value, alloc);

    return arr.RunEndArray{
        .run_ends = run_ends,
        .values = values,
        .offset = 0,
        .len = len,
    };
}

pub fn all_null_dict_array(
    dt: *const DictType,
    len: u32,
    alloc: Allocator,
) error{OutOfMemory}!arr.DictArray {
    const keys = try alloc.create(arr.Array);
    keys.* = try all_null_array(&dt.key.to_data_type(), alloc);

    const values = try alloc.create(arr.Array);
    values.* = try empty_array(&dt.value, alloc);

    return arr.DictArray{
        .is_ordered = false,
        .offset = 0,
        .len = len,
        .keys = keys,
        .values = values,
    };
}

pub fn all_null_array(dt: *const DataType, len: u32, alloc: Allocator) error{OutOfMemory}!arr.Array {
    return switch (dt.*) {
        .null => .{ .null = arr.NullArray{ .len = len } },
        .i8 => .{ .i8 = try all_null_primitive_array(i8, len, alloc) },
        .i16 => .{ .i16 = try all_null_primitive_array(i16, len, alloc) },
        .i32 => .{ .i32 = try all_null_primitive_array(i32, len, alloc) },
        .i64 => .{ .i64 = try all_null_primitive_array(i64, len, alloc) },
        .u8 => .{ .u8 = try all_null_primitive_array(u8, len, alloc) },
        .u16 => .{ .u16 = try all_null_primitive_array(u16, len, alloc) },
        .u32 => .{ .u32 = try all_null_primitive_array(u32, len, alloc) },
        .u64 => .{ .u64 = try all_null_primitive_array(u64, len, alloc) },
        .f16 => .{ .f16 = try all_null_primitive_array(f16, len, alloc) },
        .f32 => .{ .f32 = try all_null_primitive_array(f32, len, alloc) },
        .f64 => .{ .f64 = try all_null_primitive_array(f64, len, alloc) },
        .binary => .{ .binary = try all_null_binary_array(.i32, len, alloc) },
        .large_binary => .{ .large_binary = try all_null_binary_array(.i64, len, alloc) },
        .utf8 => .{ .utf8 = .{ .inner = try all_null_binary_array(.i32, len, alloc) } },
        .large_utf8 => .{ .large_utf8 = .{ .inner = try all_null_binary_array(.i64, len, alloc) } },
        .bool => .{ .bool = try all_null_bool_array(len, alloc) },
        .binary_view => .{ .binary_view = try all_null_binary_view_array(len, alloc) },
        .utf8_view => .{ .utf8_view = .{ .inner = try all_null_binary_view_array(len, alloc) } },
        .decimal32 => |a| .{ .decimal32 = try all_null_decimal_array(.i32, a, len, alloc) },
        .decimal64 => |a| .{ .decimal64 = try all_null_decimal_array(.i64, a, len, alloc) },
        .decimal128 => |a| .{ .decimal128 = try all_null_decimal_array(.i128, a, len, alloc) },
        .decimal256 => |a| .{ .decimal256 = try all_null_decimal_array(.i256, a, len, alloc) },
        .fixed_size_binary => |a| .{
            .fixed_size_binary = try all_null_fixed_size_binary_array(a, len, alloc),
        },
        .date32 => .{ .date32 = try all_null_date_array(.i32, len, alloc) },
        .date64 => .{ .date64 = try all_null_date_array(.i64, len, alloc) },
        .time32 => |a| .{ .time32 = try all_null_time_array(.i32, a, len, alloc) },
        .time64 => |a| .{ .time64 = try all_null_time_array(.i64, a, len, alloc) },
        .timestamp => |a| .{ .timestamp = try all_null_timestamp_array(a, len, alloc) },
        .duration => |a| .{ .duration = try all_null_duration_array(a, len, alloc) },
        .interval_year_month => .{ .interval_year_month = try all_null_interval_array(.year_month, len, alloc) },
        .interval_day_time => .{
            .interval_day_time = try all_null_interval_array(.day_time, len, alloc),
        },
        .interval_month_day_nano => .{
            .interval_month_day_nano = try all_null_interval_array(.month_day_nano, len, alloc),
        },
        .list => |a| .{ .list = try all_null_list_array(.i32, a, len, alloc) },
        .large_list => |a| .{ .large_list = try all_null_list_array(.i64, a, len, alloc) },
        .list_view => |a| .{ .list_view = try all_null_list_view_array(.i32, a, len, alloc) },
        .large_list_view => |a| .{ .large_list_view = try all_null_list_view_array(.i64, a, len, alloc) },
        .fixed_size_list => |a| .{ .fixed_size_list = try all_null_fixed_size_list_array(a, len, alloc) },
        .struct_ => |a| .{ .struct_ = try all_null_struct_array(a, len, alloc) },
        .map => |a| .{ .map = try all_null_map_array(a, len, alloc) },
        .dense_union => |a| .{
            .dense_union = try all_null_dense_union_array(a, len, alloc),
        },
        .sparse_union => |a| .{
            .sparse_union = try all_null_sparse_union_array(a, len, alloc),
        },
        .run_end_encoded => |a| .{ .run_end_encoded = try all_null_run_end_encoded_array(a, len, alloc) },
        .dict => |a| .{ .dict = try all_null_dict_array(a, len, alloc) },
    };
}
