const arr = @import("./array.zig");

pub fn length(array: *const arr.Array) u32 {
    switch (array.*) {
        .null => |*a| {
            return a.len;
        },
        .i8 => |*a| {
            return a.len;
        },
        .i16 => |*a| {
            return a.len;
        },
        .i32 => |*a| {
            return a.len;
        },
        .i64 => |*a| {
            return a.len;
        },
        .u8 => |*a| {
            return a.len;
        },
        .u16 => |*a| {
            return a.len;
        },
        .u32 => |*a| {
            return a.len;
        },
        .u64 => |*a| {
            return a.len;
        },
        .f16 => |*a| {
            return a.len;
        },
        .f32 => |*a| {
            return a.len;
        },
        .f64 => |*a| {
            return a.len;
        },
        .binary => |*a| {
            return a.len;
        },
        .large_binary => |*a| {
            return a.len;
        },
        .utf8 => |*a| {
            return a.inner.len;
        },
        .large_utf8 => |*a| {
            return a.inner.len;
        },
        .bool => |*a| {
            return a.len;
        },
        .binary_view => |*a| {
            return a.len;
        },
        .utf8_view => |*a| {
            return a.inner.len;
        },
        .decimal32 => |*a| {
            return a.inner.len;
        },
        .decimal64 => |*a| {
            return a.inner.len;
        },
        .decimal128 => |*a| {
            return a.inner.len;
        },
        .decimal256 => |*a| {
            return a.inner.len;
        },
        .fixed_size_binary => |*a| {
            return a.len;
        },
        .date32 => |*a| {
            return a.inner.len;
        },
        .date64 => |*a| {
            return a.inner.len;
        },
        .time32 => |*a| {
            return a.inner.len;
        },
        .time64 => |*a| {
            return a.inner.len;
        },
        .timestamp => |*a| {
            return a.inner.len;
        },
        .duration => |*a| {
            return a.inner.len;
        },
        .interval_year_month => |*a| {
            return a.inner.len;
        },
        .interval_day_time => |*a| {
            return a.inner.len;
        },
        .interval_month_day_nano => |*a| {
            return a.inner.len;
        },
        .list => |*a| {
            return a.len;
        },
        .large_list => |*a| {
            return a.len;
        },
        .list_view => |*a| {
            return a.len;
        },
        .large_list_view => |*a| {
            return a.len;
        },
        .fixed_size_list => |*a| {
            return a.len;
        },
        .struct_ => |*a| {
            return a.len;
        },
        .map => |*a| {
            return a.len;
        },
        .dense_union => |*a| {
            return a.offsets.len;
        },
        .sparse_union => |*a| {
            return a.inner.len;
        },
        .run_end_encoded => |*a| {
            return a.len;
        },
        .dict => |*a| {
            return length(a.keys);
        },
    }
}
