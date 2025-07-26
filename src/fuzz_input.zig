const std = @import("std");
const Allocator = std.mem.Allocator;
const Prng = std.Random.DefaultPrng;

const arr = @import("./array.zig");
const bitmap = @import("./bitmap.zig");
const length = @import("./length.zig");
const slice_array_impl = @import("./slice.zig").slice;

const Error = error{ OutOfMemory, ShortInput };

pub const Validity = struct {
    validity: []const u8,
    null_count: u32,
};

const MAX_DEPTH = 10;

/// This struct implements structured fuzzing.
///
/// It generates arrow arrays based on given fuzzer generated random data.
/// Normal random number generator seeded by fuzzer input is used in some places where the specific values don't change execution but we want them to be random.
pub const FuzzInput = struct {
    data: []const u8,

    pub fn int(self: *FuzzInput, comptime T: type) Error!T {
        const size = @sizeOf(T);
        if (self.data.len < size) {
            return Error.ShortInput;
        }
        const i = std.mem.readVarInt(T, self.data[0..size], .little);
        self.data = self.data[size..];
        return i;
    }

    pub fn boolean(self: *FuzzInput) Error!bool {
        if (self.data.len == 0) {
            return Error.ShortInput;
        }
        const byte = self.data[0];
        self.data = self.data[1..];
        return byte % 2 == 0;
    }

    pub fn make_prng(self: *FuzzInput) Error!Prng {
        const seed = try self.int(u64);
        return Prng.init(seed);
    }

    pub fn bytes(self: *FuzzInput, len: u32) Error![]const u8 {
        if (self.data.len < len) {
            return Error.ShortInput;
        }
        const b = self.data[0..len];
        self.data = self.data[len..];
        return b;
    }

    pub fn slice(self: *FuzzInput, comptime T: type, len: u32, alloc: Allocator) Error![]T {
        const out = try alloc.alloc(T, len);
        const out_raw: []u8 = @ptrCast(out);
        @memcpy(out_raw, try self.bytes(@intCast(out_raw.len)));
        return out;
    }

    pub fn validity(self: *FuzzInput, offset: u32, len: u32, alloc: Allocator) Error!?Validity {
        const has_validity = try self.boolean();

        const total_len = offset + len;

        if (!has_validity) {
            return null;
        }

        const v_len = (total_len + 7) / 8;
        const v = try alloc.alloc(u8, v_len);
        @memset(v, 0);

        var prng = try self.make_prng();
        const rand = prng.random();

        var idx: u32 = 0;
        while (idx < total_len) : (idx += 1) {
            if (rand.boolean()) {
                bitmap.set(v.ptr, idx);
            }
        }

        return .{ .validity = v, .null_count = bitmap.count_nulls(v, offset, len) };
    }

    pub fn null_array(len: u32) arr.NullArray {
        return .{ .len = len };
    }

    pub fn primitive_array(self: *FuzzInput, comptime T: type, len: u32, alloc: Allocator) Error!arr.PrimitiveArray(T) {
        const offset: u32 = try self.int(u8);
        const total_len = len + offset;

        var prng = try self.make_prng();
        const rand = prng.random();
        const values = try alloc.alloc(T, total_len);

        switch (T) {
            f16, f32, f64 => {
                for (0..total_len) |idx| {
                    values.ptr[idx] = @floatCast(rand.float(f64));
                }
            },
            else => {
                const values_raw: []u8 = @ptrCast(values);
                rand.bytes(values_raw);
            },
        }

        var array = arr.PrimitiveArray(T){
            .len = len,
            .offset = offset,
            .values = values,
            .validity = null,
            .null_count = 0,
        };

        if (try self.validity(offset, len, alloc)) |v| {
            array.validity = v.validity;
            array.null_count = v.null_count;
        }

        return array;
    }

    pub fn binary_array(self: *FuzzInput, comptime index_t: arr.IndexType, len: u32, alloc: Allocator) Error!arr.GenericBinaryArray(index_t) {
        const I = index_t.to_type();

        const offset: u32 = try self.int(u8);
        const total_len: u32 = len + offset;

        const sizes = try self.bytes(total_len);

        const offsets = try alloc.alloc(I, total_len + 1);
        {
            var start_offset: I = 0;
            for (0..total_len) |idx| {
                offsets.ptr[idx] = start_offset;
                start_offset +%= sizes.ptr[idx];
            }
            offsets.ptr[total_len] = start_offset;
        }

        var prng = try self.make_prng();
        const rand = prng.random();

        const data_len = offsets[total_len];

        const data = try alloc.alloc(u8, @intCast(data_len));
        rand.bytes(data);

        var array = arr.GenericBinaryArray(index_t){
            .len = len,
            .offset = offset,
            .data = data,
            .offsets = offsets,
            .validity = null,
            .null_count = 0,
        };

        if (try self.validity(offset, len, alloc)) |v| {
            array.validity = v.validity;
            array.null_count = v.null_count;
        }

        return array;
    }

    pub fn utf8_array(self: *FuzzInput, comptime index_t: arr.IndexType, len: u32, alloc: Allocator) Error!arr.GenericUtf8Array(index_t) {
        return .{ .inner = try self.binary_array(index_t, len, alloc) };
    }

    pub fn bool_array(self: *FuzzInput, len: u32, alloc: Allocator) Error!arr.BoolArray {
        const offset: u32 = try self.int(u8);
        const total_len: u32 = len + offset;

        var prng = try self.make_prng();
        const rand = prng.random();
        const bitmap_len = (total_len + 7) / 8;
        const values = try alloc.alloc(u8, bitmap_len);
        @memset(values, 0);
        {
            var idx: u32 = 0;
            while (idx < total_len) : (idx += 1) {
                if (rand.boolean()) {
                    bitmap.set(values.ptr, idx);
                }
            }
        }

        var array = arr.BoolArray{
            .len = len,
            .offset = offset,
            .values = values,
            .validity = null,
            .null_count = 0,
        };

        if (try self.validity(offset, len, alloc)) |v| {
            array.validity = v.validity;
            array.null_count = v.null_count;
        }

        return array;
    }

    pub fn decimal_array(self: *FuzzInput, comptime decimal_t: arr.DecimalInt, len: u32, alloc: Allocator) Error!arr.DecimalArray(decimal_t) {
        const max_precision = switch (decimal_t) {
            .i32 => 9,
            .i64 => 19,
            .i128 => 38,
            .i256 => 76,
        };

        const scale = try self.int(i8);
        const precision = (try self.int(u8)) % (max_precision + 1);

        const params = arr.DecimalParams{ .scale = scale, .precision = precision };

        const inner = try self.primitive_array(decimal_t.to_type(), len, alloc);

        return .{ .params = params, .inner = inner };
    }

    pub fn date_array(self: *FuzzInput, comptime backing_t: arr.IndexType, len: u32, alloc: Allocator) Error!arr.DateArray(backing_t) {
        return .{ .inner = try self.primitive_array(backing_t.to_type(), len, alloc) };
    }

    pub fn time_array(self: *FuzzInput, comptime backing_t: arr.IndexType, len: u32, alloc: Allocator) Error!arr.TimeArray(backing_t) {
        const Unit = arr.TimeArray(backing_t).Unit;

        const unit_bit = (try self.int(u8)) % 2 == 0;

        const unit: Unit = switch (backing_t) {
            .i32 => if (unit_bit) .second else .millisecond,
            .i64 => if (unit_bit) .microsecond else .nanosecond,
        };

        return .{ .unit = unit, .inner = try self.primitive_array(backing_t.to_type(), len, alloc) };
    }

    fn timestamp_unit(self: *FuzzInput) Error!arr.TimestampUnit {
        const unit_int: u8 = (try self.int(u8)) % 4;
        return switch (unit_int) {
            0 => .second,
            1 => .millisecond,
            2 => .microsecond,
            3 => .nanosecond,
            else => unreachable,
        };
    }

    pub fn timestamp_array(self: *FuzzInput, len: u32, alloc: Allocator) Error!arr.TimestampArray {
        const unit = try self.timestamp_unit();
        var timestamp = arr.Timestamp{
            .unit = unit,
            .timezone = null,
        };

        const timezone_int = try self.int(u8);
        const has_timezone = timezone_int % 2 == 0;
        const alloc_timezone = timezone_int % 4 == 0;
        if (has_timezone) {
            const tz = "Africa/Abidjan";
            if (alloc_timezone) {
                const timezone = try alloc.alloc(u8, tz.len);
                @memcpy(timezone, tz);
                timestamp.timezone = timezone;
            } else {
                timestamp.timezone = tz;
            }
        }

        return .{ .ts = timestamp, .inner = try self.primitive_array(i64, len, alloc) };
    }

    pub fn interval_array(self: *FuzzInput, comptime interval_t: arr.IntervalType, len: u32, alloc: Allocator) Error!arr.IntervalArray(interval_t) {
        return .{
            .inner = try self.primitive_array(interval_t.to_type(), len, alloc),
        };
    }

    pub fn fixed_size_binary_array(self: *FuzzInput, len: u32, alloc: Allocator) Error!arr.FixedSizeBinaryArray {
        const offset: u32 = try self.int(u8);
        const total_len: u32 = len + offset;

        const byte_width = (try self.int(u8)) + 1;

        var prng = try self.make_prng();
        const rand = prng.random();

        const data = try alloc.alloc(u8, byte_width * total_len);
        rand.bytes(data);

        var array = arr.FixedSizeBinaryArray{
            .len = len,
            .offset = offset,
            .validity = null,
            .null_count = 0,
            .data = data,
            .byte_width = byte_width,
        };

        if (try self.validity(offset, len, alloc)) |v| {
            array.validity = v.validity;
            array.null_count = v.null_count;
        }

        return array;
    }

    pub fn duration_array(self: *FuzzInput, len: u32, alloc: Allocator) Error!arr.DurationArray {
        return .{ .unit = try self.timestamp_unit(), .inner = try self.primitive_array(i64, len, alloc) };
    }

    pub fn binary_view_array(self: *FuzzInput, len: u32, alloc: Allocator) Error!arr.BinaryViewArray {
        const offset: u32 = try self.int(u8);
        const total_len: u32 = len + offset;

        const max_str_len = 40;
        const num_buffers = (try self.int(u8)) % 5 + 1;
        const buffer_len = @max(max_str_len + 5, (try self.int(u16)) % (1 << 8) + 1);

        var prng = try self.make_prng();
        const rand = prng.random();

        const buffers = try alloc.alloc([]const u8, num_buffers);
        for (0..num_buffers) |buffer_idx| {
            const buffer = try alloc.alloc(u8, buffer_len);
            rand.bytes(buffer);
            buffers[buffer_idx] = buffer;
        }

        const views = try self.slice(arr.BinaryView, total_len, alloc);
        for (0..views.len) |view_idx| {
            var view = views.ptr[view_idx];
            const view_len = @as(u32, @bitCast(view.length)) % (max_str_len + 1);
            view.length = @bitCast(view_len);

            if (view_len > 12) {
                const buffer_idx = @as(u32, @bitCast(view.buffer_idx)) % num_buffers;
                view.buffer_idx = @bitCast(buffer_idx);
                const max_offset = buffer_len - view_len;
                const voffset = @as(u32, @bitCast(view.offset)) % max_offset;
                view.offset = @intCast(voffset);
                view.prefix = std.mem.readVarInt(i32, buffers[buffer_idx][voffset .. voffset + 4], .little);
            }

            views.ptr[view_idx] = view;
        }

        var array = arr.BinaryViewArray{
            .len = len,
            .offset = offset,
            .buffers = buffers,
            .views = views,
            .validity = null,
            .null_count = 0,
        };

        if (try self.validity(offset, len, alloc)) |v| {
            array.validity = v.validity;
            array.null_count = v.null_count;
        }

        return array;
    }

    pub fn utf8_view_array(self: *FuzzInput, len: u32, alloc: Allocator) Error!arr.Utf8ViewArray {
        return .{ .inner = try self.binary_view_array(len, alloc) };
    }

    pub fn make_array(self: *FuzzInput, len: u32, alloc: Allocator) Error!arr.Array {
        return make_array_impl(self, len, alloc, 0);
    }

    fn make_array_impl(self: *FuzzInput, len: u32, alloc: Allocator, depth: u8) Error!arr.Array {
        if (depth > MAX_DEPTH) {
            return .{ .null = null_array(len) };
        }

        const kind = (try self.int(u8)) % 44;

        switch (kind) {
            0 => return .{ .null = null_array(len) },
            1 => return .{ .i8 = try self.primitive_array(i8, len, alloc) },
            2 => return .{ .i16 = try self.primitive_array(i16, len, alloc) },
            3 => return .{ .i32 = try self.primitive_array(i32, len, alloc) },
            4 => return .{ .i64 = try self.primitive_array(i64, len, alloc) },
            5 => return .{ .u8 = try self.primitive_array(u8, len, alloc) },
            6 => return .{ .u16 = try self.primitive_array(u16, len, alloc) },
            7 => return .{ .u32 = try self.primitive_array(u32, len, alloc) },
            8 => return .{ .u64 = try self.primitive_array(u64, len, alloc) },
            9 => return .{ .f16 = try self.primitive_array(f16, len, alloc) },
            10 => return .{ .f32 = try self.primitive_array(f32, len, alloc) },
            11 => return .{ .f64 = try self.primitive_array(f64, len, alloc) },
            12 => return .{ .binary = try self.binary_array(.i32, len, alloc) },
            13 => return .{ .utf8 = try self.utf8_array(.i32, len, alloc) },
            14 => return .{ .bool = try self.bool_array(len, alloc) },
            15 => return .{ .decimal32 = try self.decimal_array(.i32, len, alloc) },
            16 => return .{ .decimal64 = try self.decimal_array(.i64, len, alloc) },
            17 => return .{ .decimal128 = try self.decimal_array(.i128, len, alloc) },
            18 => return .{ .decimal256 = try self.decimal_array(.i256, len, alloc) },
            19 => return .{ .date32 = try self.date_array(.i32, len, alloc) },
            20 => return .{ .date64 = try self.date_array(.i64, len, alloc) },
            21 => return .{ .time32 = try self.time_array(.i32, len, alloc) },
            22 => return .{ .time64 = try self.time_array(.i64, len, alloc) },
            23 => return .{ .timestamp = try self.timestamp_array(len, alloc) },
            24 => return .{ .interval_year_month = try self.interval_array(.year_month, len, alloc) },
            25 => return .{ .interval_day_time = try self.interval_array(.day_time, len, alloc) },
            26 => return .{ .interval_month_day_nano = try self.interval_array(.month_day_nano, len, alloc) },
            27 => return .{ .list = try self.list_array(.i32, len, alloc, depth) },
            28 => return .{ .struct_ = try self.struct_array(len, alloc, depth) },
            29 => return .{ .dense_union = try self.dense_union_array(len, alloc, depth) },
            30 => return .{ .sparse_union = try self.sparse_union_array(len, alloc, depth) },
            31 => return .{ .fixed_size_binary = try self.fixed_size_binary_array(len, alloc) },
            32 => return .{ .fixed_size_list = try self.fixed_size_list_array(len, alloc, depth) },
            33 => return .{ .map = try self.map_array(len, alloc, depth) },
            34 => return .{ .duration = try self.duration_array(len, alloc) },
            35 => return .{ .large_binary = try self.binary_array(.i64, len, alloc) },
            36 => return .{ .large_utf8 = try self.utf8_array(.i64, len, alloc) },
            37 => return .{ .large_list = try self.list_array(.i64, len, alloc, depth) },
            38 => return .{ .run_end_encoded = try self.run_end_encoded_array(len, alloc) },
            39 => return .{ .binary_view = try self.binary_view_array(len, alloc) },
            40 => return .{ .utf8_view = try self.utf8_view_array(len, alloc) },
            41 => return .{ .list_view = try self.list_view_array(.i32, len, alloc, depth) },
            42 => return .{ .large_list_view = try self.list_view_array(.i64, len, alloc, depth) },
            43 => return .{ .dict = try self.dict_array(len, alloc) },
            else => unreachable,
        }
    }

    pub fn list_array(self: *FuzzInput, comptime index_t: arr.IndexType, len: u32, alloc: Allocator, depth: u8) Error!arr.GenericListArray(index_t) {
        const I = index_t.to_type();

        const offset: u32 = try self.int(u8);
        const total_len: u32 = len + offset;

        const sizes_b = try self.bytes(total_len);

        const sizes = try alloc.alloc(u8, sizes_b.len);
        for (0..total_len) |idx| {
            sizes.ptr[idx] = sizes_b.ptr[idx] % 10;
        }

        const offsets = try alloc.alloc(I, total_len + 1);
        {
            var start_offset: I = 0;
            for (0..total_len) |idx| {
                offsets.ptr[idx] = start_offset;
                start_offset +%= sizes.ptr[idx];
            }
            offsets.ptr[total_len] = start_offset;
        }

        const inner_len = offsets[total_len];

        const inner = try alloc.create(arr.Array);
        inner.* = try self.make_array_impl(@intCast(inner_len), alloc, depth + 1);

        var array = arr.GenericListArray(index_t){
            .len = len,
            .offset = offset,
            .inner = inner,
            .offsets = offsets,
            .validity = null,
            .null_count = 0,
        };

        if (try self.validity(offset, len, alloc)) |v| {
            array.validity = v.validity;
            array.null_count = v.null_count;
        }

        return array;
    }

    pub fn list_view_array(self: *FuzzInput, comptime index_t: arr.IndexType, len: u32, alloc: Allocator, depth: u8) Error!arr.GenericListViewArray(index_t) {
        const I = index_t.to_type();
        const U = switch (index_t) {
            .i32 => u32,
            .i64 => u64,
        };

        const offset: u32 = try self.int(u8);
        const total_len: u32 = len + offset;

        const sizes_b = try self.bytes(total_len);

        const sizes = try alloc.alloc(I, total_len);
        for (0..total_len) |idx| {
            sizes.ptr[idx] = sizes_b.ptr[idx] % 10;
        }

        var total_size: I = 0;
        for (0..total_len) |idx| {
            total_size += sizes.ptr[idx];
        }

        const offsets = try self.slice(I, total_len, alloc);
        if (total_len == 1) {
            offsets[0] = 0;
        } else {
            for (0..total_len) |idx| {
                offsets.ptr[idx] = @bitCast(@as(U, @bitCast(offsets.ptr[idx])) % @as(U, @bitCast(total_size -% sizes.ptr[idx] +% 1)));
            }
        }

        const inner_len = total_size;

        const inner = try alloc.create(arr.Array);
        inner.* = try self.make_array_impl(@intCast(inner_len), alloc, depth + 1);

        var array = arr.GenericListViewArray(index_t){
            .len = len,
            .offset = offset,
            .inner = inner,
            .offsets = offsets,
            .sizes = sizes,
            .validity = null,
            .null_count = 0,
        };

        if (try self.validity(offset, len, alloc)) |v| {
            array.validity = v.validity;
            array.null_count = v.null_count;
        }

        return array;
    }

    pub fn struct_array(self: *FuzzInput, len: u32, alloc: Allocator, depth: u8) Error!arr.StructArray {
        const offset: u32 = try self.int(u8);
        const total_len: u32 = len + offset;

        const num_fields = (try self.int(u8)) % 5 + 1;

        const field_values = try alloc.alloc(arr.Array, num_fields);
        const field_names = try alloc.alloc([:0]const u8, num_fields);

        var prng = try self.make_prng();
        const rand = prng.random();

        for (0..num_fields) |field_idx| {
            field_values[field_idx] = try self.make_array_impl(total_len, alloc, depth + 1);

            const field_name_len = (try self.int(u8)) % 48;
            const field_name = try alloc.allocSentinel(u8, field_name_len, 0);
            rand_bytes_zero_sentinel(rand, field_name);

            field_names[field_idx] = field_name;
        }

        var array = arr.StructArray{
            .len = len,
            .offset = offset,
            .field_names = field_names,
            .field_values = field_values,
            .validity = null,
            .null_count = 0,
        };

        if (try self.validity(offset, len, alloc)) |v| {
            array.validity = v.validity;
            array.null_count = v.null_count;
        }

        return array;
    }

    pub fn dense_union_array(self: *FuzzInput, len: u32, alloc: Allocator, depth: u8) Error!arr.DenseUnionArray {
        const offset: u32 = try self.int(u8);
        const total_len: u32 = len + offset;

        const num_children = (try self.int(u8)) % 5 + 1;

        const children = try alloc.alloc(arr.Array, num_children);
        const type_id_set = try alloc.alloc(i8, num_children);
        for (0..num_children) |child_idx| {
            type_id_set[child_idx] = @intCast(child_idx);
        }
        const field_names = try alloc.alloc([:0]const u8, num_children);

        var prng = try self.make_prng();
        const rand = prng.random();

        const offsets = try alloc.alloc(i32, total_len);
        const type_ids = try alloc.alloc(i8, total_len);

        const current_offsets = try alloc.alloc(i32, num_children);
        @memset(current_offsets, 0);

        for (try self.bytes(total_len), 0..) |b, idx| {
            const child_idx = b % num_children;
            type_ids[idx] = type_id_set.ptr[child_idx];
            const current_offset = current_offsets[child_idx];
            current_offsets[child_idx] = current_offset + 1;
            offsets[idx] = current_offset;
        }

        for (0..num_children) |child_idx| {
            children[child_idx] = try self.make_array_impl(@as(u32, @intCast(current_offsets[child_idx])), alloc, depth + 1);

            const field_name_len = (try self.int(u8)) % 48;
            const field_name = try alloc.allocSentinel(u8, field_name_len, 0);
            rand_bytes_zero_sentinel(rand, field_name);

            field_names[child_idx] = field_name;
        }

        return .{
            .offsets = offsets,
            .inner = arr.UnionArray{
                .offset = offset,
                .len = len,
                .field_names = field_names,
                .children = children,
                .type_ids = type_ids,
                .type_id_set = type_id_set,
            },
        };
    }

    pub fn sparse_union_array(self: *FuzzInput, len: u32, alloc: Allocator, depth: u8) Error!arr.SparseUnionArray {
        const offset: u32 = try self.int(u8);
        const total_len: u32 = len + offset;

        const num_children = (try self.int(u8)) % 5 + 1;

        const children = try alloc.alloc(arr.Array, num_children);
        const type_id_set = try alloc.alloc(i8, num_children);
        for (0..num_children) |child_idx| {
            type_id_set[child_idx] = @intCast(child_idx);
        }
        const field_names = try alloc.alloc([:0]const u8, num_children);

        var prng = try self.make_prng();
        const rand = prng.random();

        for (0..num_children) |child_idx| {
            children[child_idx] = try self.make_array_impl(total_len, alloc, depth + 1);

            const field_name_len = (try self.int(u8)) % 48;
            const field_name = try alloc.allocSentinel(u8, field_name_len, 0);
            rand_bytes_zero_sentinel(rand, field_name);

            field_names[child_idx] = field_name;
        }

        const type_ids = try alloc.alloc(i8, total_len);

        for (try self.bytes(total_len), 0..) |b, idx| {
            const child_idx = b % num_children;
            type_ids[idx] = type_id_set.ptr[child_idx];
        }

        return .{
            .inner = arr.UnionArray{
                .offset = offset,
                .len = len,
                .field_names = field_names,
                .children = children,
                .type_ids = type_ids,
                .type_id_set = type_id_set,
            },
        };
    }

    pub fn fixed_size_list_array(self: *FuzzInput, len: u32, alloc: Allocator, depth: u8) Error!arr.FixedSizeListArray {
        const offset: u32 = try self.int(u8);
        const total_len: u32 = len + offset;

        const item_width = (try self.int(u8)) % 10 + 1;

        const inner = try alloc.create(arr.Array);
        inner.* = try self.make_array_impl(item_width * total_len, alloc, depth + 1);

        var array = arr.FixedSizeListArray{
            .len = len,
            .offset = offset,
            .validity = null,
            .null_count = 0,
            .inner = inner,
            .item_width = item_width,
        };

        if (try self.validity(offset, len, alloc)) |v| {
            array.validity = v.validity;
            array.null_count = v.null_count;
        }

        return array;
    }

    pub fn map_array(self: *FuzzInput, len: u32, alloc: Allocator, depth: u8) Error!arr.MapArray {
        const offset: u32 = try self.int(u8);
        const total_len: u32 = len + offset;

        const sizes = try self.bytes(total_len);

        var entries_len: u32 = 0;
        for (sizes) |sz| {
            entries_len += sz;
        }

        const entries_offset: u32 = try self.int(u8);
        const entries_total_len: u32 = entries_len + entries_offset;

        const field_names = try alloc.alloc([:0]const u8, 2);
        field_names[0] = "keys";
        field_names[1] = "values";

        const field_values = try alloc.alloc(arr.Array, 2);
        var keys = try self.binary_array(.i32, entries_total_len, alloc);
        keys.null_count = 0;
        keys.validity = null;
        field_values[0] = .{ .binary = keys };
        field_values[1] = try self.make_array_impl(entries_total_len, alloc, depth + 1);

        const entries = try alloc.create(arr.StructArray);
        entries.* = arr.StructArray{
            .field_names = field_names,
            .field_values = field_values,
            .len = entries_len,
            .offset = entries_offset,
            .validity = null,
            .null_count = 0,
        };

        const offsets = try alloc.alloc(i32, total_len + 1);
        {
            var start_offset: i32 = 0;
            for (0..total_len) |idx| {
                offsets.ptr[idx] = start_offset;
                start_offset +%= sizes.ptr[idx];
            }
            offsets.ptr[total_len] = start_offset;
        }

        var array = arr.MapArray{
            .len = len,
            .offset = offset,
            .offsets = offsets,
            .entries = entries,
            .keys_are_sorted = false,
            .validity = null,
            .null_count = 0,
        };

        if (try self.validity(offset, len, alloc)) |v| {
            array.validity = v.validity;
            array.null_count = v.null_count;
        }

        return array;
    }

    pub fn run_end_encoded_array(self: *FuzzInput, len: u32, alloc: Allocator) Error!arr.RunEndArray {
        const offset: u32 = try self.int(u8);
        const total_len: u32 = len + offset;

        const run_ends_offset: u32 = try self.int(u8);
        const run_ends_total_len: u32 = run_ends_offset + total_len;

        const sizes = try self.bytes(run_ends_total_len);
        const run_ends_values = try alloc.alloc(i32, run_ends_total_len);
        if (run_ends_total_len > 0) {
            var run_end: i32 = sizes[0];
            for (1..run_ends_total_len) |idx| {
                run_ends_values.ptr[idx] = run_end;
                run_end += sizes.ptr[idx];
            }
        }

        const run_ends = try alloc.create(arr.Array);
        run_ends.* = .{ .i32 = arr.Int32Array{ .len = total_len, .offset = run_ends_offset, .values = run_ends_values, .null_count = 0, .validity = null } };

        const values = try alloc.create(arr.Array);
        values.* = .{ .binary = try self.binary_array(.i32, run_ends_total_len, alloc) };

        return .{
            .len = len,
            .offset = offset,
            .run_ends = run_ends,
            .values = values,
        };
    }

    pub fn dict_array(self: *FuzzInput, len: u32, alloc: Allocator) Error!arr.DictArray {
        const offset: u32 = try self.int(u8);
        const total_len: u32 = len + offset;

        const num_values = @as(u32, try self.int(u8)) + 1;

        const keys_offset = try self.int(u8);
        const keys_total_len = keys_offset + total_len;

        const keys_data = try self.slice(u32, keys_total_len, alloc);
        for (0..keys_total_len) |idx| {
            keys_data.ptr[idx] %= num_values;
        }

        const keys = try alloc.create(arr.Array);
        keys.* = .{ .i32 = .{
            .values = @ptrCast(keys_data),
            .len = total_len,
            .offset = keys_offset,
            .validity = null,
            .null_count = 0,
        } };

        const values = try alloc.create(arr.Array);
        values.* = arr.Array{ .binary_view = try self.binary_view_array(num_values, alloc) };

        return arr.DictArray{
            .len = len,
            .offset = offset,
            .keys = keys,
            .values = values,
            .is_ordered = false,
        };
    }

    pub fn slice_array(self: *FuzzInput, array: *const arr.Array) Error!arr.Array {
        const array_len = length.length(array);
        if (array_len == 0) {
            return array.*;
        }
        const slice0_len = (try self.int(u8)) % array_len;
        const slice0_offset = (try self.int(u8)) % (array_len - slice0_len);
        const slice0 = slice_array_impl(array, slice0_offset, slice0_len);
        return slice0;
    }
};

fn rand_bytes_zero_sentinel(rand: std.Random, out: []u8) void {
    rand.bytes(out);

    for (0..out.len) |i| {
        if (out.ptr[i] == 0) {
            out.ptr[i] = 1;
        }
    }
}
