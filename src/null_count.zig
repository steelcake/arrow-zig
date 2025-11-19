const arr = @import("./array.zig");
const slice = @import("./slice.zig");
const bitmap = @import("./bitmap.zig");

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
            return dense_union_null_count(a);
        },
        .sparse_union => |*a| {
            return sparse_union_null_count(a);
        },
        .run_end_encoded => |*a| {
            return run_end_encoded_null_count(a);
        },
        .dict => |*a| {
            return dict_null_count(a);
        },
    }
}

fn dict_null_count_impl(
    comptime T: type,
    keys: *const arr.PrimitiveArray(T),
    values: *const arr.Array,
    offset: u32,
    len: u32,
) u32 {
    if (keys.null_count > 0) {
        const validity = keys.validity orelse unreachable;

        var count: u32 = keys.null_count;

        const Closure = struct {
            cnt: *u32,
            keys_: []const T,
            vals: *const arr.Array,

            fn process(self: @This(), idx: u32) void {
                self.cnt.* += null_count(
                    &slice.slice(
                        self.vals,
                        @intCast(self.keys_[idx]),
                        1,
                    ),
                );
            }
        };

        bitmap.for_each(
            Closure,
            Closure.process,
            Closure{
                .keys_ = keys.values,
                .vals = values,
                .cnt = &count,
            },
            validity,
            keys.offset + offset,
            len,
        );

        return count;
    } else {
        var count: u32 = 0;

        var idx: u32 = offset;
        while (idx < offset + len) : (idx += 1) {
            count += null_count(&slice.slice(
                values,
                @intCast(keys.values[keys.offset + idx]),
                1,
            ));
        }

        return count;
    }
}

pub fn dict_null_count(array: *const arr.DictArray) u32 {
    return switch (array.keys.*) {
        .i8 => |*a| dict_null_count_impl(i8, a, array.values, array.offset, array.len),
        .i16 => |*a| dict_null_count_impl(i16, a, array.values, array.offset, array.len),
        .i32 => |*a| dict_null_count_impl(i32, a, array.values, array.offset, array.len),
        .i64 => |*a| dict_null_count_impl(i64, a, array.values, array.offset, array.len),
        .u8 => |*a| dict_null_count_impl(u8, a, array.values, array.offset, array.len),
        .u16 => |*a| dict_null_count_impl(u16, a, array.values, array.offset, array.len),
        .u32 => |*a| dict_null_count_impl(u32, a, array.values, array.offset, array.len),
        .u64 => |*a| dict_null_count_impl(u64, a, array.values, array.offset, array.len),
        else => unreachable,
    };
}

fn run_end_encoded_null_count(array: *const arr.RunEndArray) u32 {
    return switch (array.run_ends.*) {
        .i16 => |*a| run_end_encoded_null_count_impl(i16, a, array.values, array.offset, array.len),
        .i32 => |*a| run_end_encoded_null_count_impl(i32, a, array.values, array.offset, array.len),
        .i64 => |*a| run_end_encoded_null_count_impl(i64, a, array.values, array.offset, array.len),
        else => unreachable,
    };
}

pub fn run_end_encoded_null_count_impl(
    comptime T: type,
    run_ends: *const arr.PrimitiveArray(T),
    values: *const arr.Array,
    offset: u32,
    len: u32,
) u32 {
    var count: u32 = 0;

    var idx: u32 = offset;
    var start: T = 0;
    while (idx < offset + len) : (idx += 1) {
        const cnt = null_count(&slice.slice(values, idx, 1));
        const end = run_ends.values[run_ends.offset + idx];
        count += cnt * @as(u32, @intCast(end - start));
        start = end;
    }

    return count;
}

pub fn dense_union_null_count(array: *const arr.DenseUnionArray) u32 {
    var count: u32 = 0;

    for (array.inner.children, array.inner.type_id_set) |*c, tid| {
        var idx: u32 = array.inner.offset;
        while (idx < array.inner.offset + array.inner.len) : (idx += 1) {
            if (tid == array.inner.type_ids[idx]) {
                const off = array.offsets[idx];
                count += null_count(&slice.slice(c, @intCast(off), 1));
            }
        }
    }

    return count;
}

pub fn sparse_union_null_count(array: *const arr.SparseUnionArray) u32 {
    var count: u32 = 0;

    for (array.inner.children, array.inner.type_id_set) |*c, tid| {
        var idx: u32 = array.inner.offset;
        while (idx < array.inner.offset + array.inner.len) : (idx += 1) {
            if (tid == array.inner.type_ids[idx]) {
                count += null_count(&slice.slice(c, idx, 1));
            }
        }
    }

    return count;
}
