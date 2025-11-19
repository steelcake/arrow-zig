const arr = @import("./array.zig");

/// This is just a field access for most arrays but requires more work for
///     some arrays like the dense/sparse union arrays, dict array and run end encoded array
pub fn null_count(array: *const arr.Array) u32 {
    switch (array.*) {
        .null => |*a| {
            return a.len;
        },
        .i8 => |*a| {
            return a.null_count;
        },
        .i16 => |*a| {
            return a.null_count;
        },
        .i32 => |*a| {
            return a.null_count;
        },
        .i64 => |*a| {
            return a.null_count;
        },
        .u8 => |*a| {
            return a.null_count;
        },
        .u16 => |*a| {
            return a.null_count;
        },
        .u32 => |*a| {
            return a.null_count;
        },
        .u64 => |*a| {
            return a.null_count;
        },
        .f16 => |*a| {
            return a.null_count;
        },
        .f32 => |*a| {
            return a.null_count;
        },
        .f64 => |*a| {
            return a.null_count;
        },
        .binary => |*a| {
            return a.null_count;
        },
        .large_binary => |*a| {
            return a.null_count;
        },
        .utf8 => |*a| {
            return a.inner.null_count;
        },
        .large_utf8 => |*a| {
            return a.inner.null_count;
        },
        .bool => |*a| {
            return a.null_count;
        },
        .binary_view => |*a| {
            return a.null_count;
        },
        .utf8_view => |*a| {
            return a.inner.null_count;
        },
        .decimal32 => |*a| {
            return a.inner.null_count;
        },
        .decimal64 => |*a| {
            return a.inner.null_count;
        },
        .decimal128 => |*a| {
            return a.inner.null_count;
        },
        .decimal256 => |*a| {
            return a.inner.null_count;
        },
        .fixed_size_binary => |*a| {
            return a.null_count;
        },
        .date32 => |*a| {
            return a.inner.null_count;
        },
        .date64 => |*a| {
            return a.inner.null_count;
        },
        .time32 => |*a| {
            return a.inner.null_count;
        },
        .time64 => |*a| {
            return a.inner.null_count;
        },
        .timestamp => |*a| {
            return a.inner.null_count;
        },
        .duration => |*a| {
            return a.inner.null_count;
        },
        .interval_year_month => |*a| {
            return a.inner.null_count;
        },
        .interval_day_time => |*a| {
            return a.inner.null_count;
        },
        .interval_month_day_nano => |*a| {
            return a.inner.null_count;
        },
        .list => |*a| {
            return a.null_count;
        },
        .large_list => |*a| {
            return a.null_count;
        },
        .list_view => |*a| {
            return a.null_count;
        },
        .large_list_view => |*a| {
            return a.null_count;
        },
        .fixed_size_list => |*a| {
            return a.null_count;
        },
        .struct_ => |*a| {
            return a.null_count;
        },
        .map => |*a| {
            return a.null_count;
        },
        .dense_union => |*a| {
            return a.inner.null_count;
        },
        .sparse_union => |*a| {
            return a.inner.null_count;
        },
        .run_end_encoded => |*a| {
            return a.null_count;
        },
        .dict => |*a| {
            return a.null_count;
        },
    }
}
