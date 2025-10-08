const std = @import("std");
const Allocator = std.mem.Allocator;
const Prng = std.Random.DefaultPrng;

const arr = @import("./array.zig");
const bitmap = @import("./bitmap.zig");
const length = @import("./length.zig");
const slice_array_impl = @import("./slice.zig").slice;
const data_type = @import("./data_type.zig");
const scalar = @import("./scalar.zig");

const Error = error{ OutOfMemory, ShortInput };

pub const Validity = struct {
    validity: []const u8,
    null_count: u32,
};

const MAX_DEPTH = 5;

/// This struct implements structured fuzzing.
///
/// It generates arrow arrays based on given fuzzer generated random data.
/// Normal random number generator seeded by fuzzer input is used in some places where the specific values don't change execution but we want them to be random.
pub const FuzzInput = struct {
    data: []const u8,

    pub fn init(data: []const u8) FuzzInput {
        return .{
            .data = data,
        };
    }

    pub fn float(self: *FuzzInput, comptime T: type) Error!T {
        var prng = try self.make_prng();
        const rand = prng.random();
        return @floatCast(rand.float(f64));
    }

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
        const params = try self.decimal_params(decimal_t);

        const inner = try self.primitive_array(decimal_t.to_type(), len, alloc);

        return .{ .params = params, .inner = inner };
    }

    pub fn date_array(self: *FuzzInput, comptime backing_t: arr.IndexType, len: u32, alloc: Allocator) Error!arr.DateArray(backing_t) {
        return .{ .inner = try self.primitive_array(backing_t.to_type(), len, alloc) };
    }

    pub fn time_array(self: *FuzzInput, comptime backing_t: arr.IndexType, len: u32, alloc: Allocator) Error!arr.TimeArray(backing_t) {
        return .{ .unit = try self.time_unit(backing_t), .inner = try self.primitive_array(backing_t.to_type(), len, alloc) };
    }

    pub fn timestamp_array(self: *FuzzInput, len: u32, alloc: Allocator) Error!arr.TimestampArray {
        return .{ .ts = try self.timestamp(alloc), .inner = try self.primitive_array(i64, len, alloc) };
    }

    pub fn interval_array(self: *FuzzInput, comptime interval_t: arr.IntervalType, len: u32, alloc: Allocator) Error!arr.IntervalArray(interval_t) {
        return .{
            .inner = try self.primitive_array(interval_t.to_type(), len, alloc),
        };
    }

    pub fn fixed_size_binary_array(self: *FuzzInput, len: u32, alloc: Allocator) Error!arr.FixedSizeBinaryArray {
        const offset: u32 = try self.int(u8);
        const total_len: u32 = len + offset;

        const byte_width = @min((try self.int(u8)) % 69 + 1, std.math.maxInt(u32) / @max(1, total_len));

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
            38 => return .{ .run_end_encoded = try self.run_end_encoded_array(len, alloc, depth) },
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

            const field_name = try make_name(field_names[0..field_idx], rand, alloc);
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

            const field_name = try make_name(field_names[0..child_idx], rand, alloc);
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

            const field_name = try make_name(field_names[0..child_idx], rand, alloc);
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

    pub fn run_end_encoded_array(self: *FuzzInput, len: u32, alloc: Allocator, depth: u8) Error!arr.RunEndArray {
        const offset: u32 = try self.int(u8);
        const total_len: u32 = len + offset;

        const run_ends_len = @as(u32, try self.int(u8)) + 1;
        const run_ends_offset: u32 = try self.int(u8);
        const run_ends_total_len: u32 = run_ends_len + run_ends_offset;

        const run_ends_values = try self.slice(i32, run_ends_total_len, alloc);
        var run_end: i32 = 0;
        const tl: i32 = @intCast(total_len);
        for (run_ends_values) |*x| {
            run_end += @as(i32, @bitCast(@as(u32, @bitCast(x.*)) % 512));
            run_end = @min(tl, run_end);
            x.* = run_end;
        }
        const last_re = &run_ends_values[run_ends_values.len - 1];
        last_re.* = @max(last_re.*, @as(i32, @intCast(total_len)));

        const values = try alloc.create(arr.Array);
        values.* = try self.make_array_impl(run_ends_len, alloc, depth + 1);
        const run_ends = try alloc.create(arr.Array);
        run_ends.* = .{
            .i32 = .{
                .values = run_ends_values,
                .len = run_ends_len,
                .offset = run_ends_offset,
                .validity = null,
                .null_count = 0,
            },
        };

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

    pub fn make_data_type(self: *FuzzInput, alloc: Allocator) Error!data_type.DataType {
        return try self.make_data_type_impl(alloc, 0);
    }

    fn make_data_type_impl(self: *FuzzInput, alloc: Allocator, depth: u8) Error!data_type.DataType {
        if (depth > MAX_DEPTH) {
            return .{ .null = {} };
        }

        const kind = (try self.int(u8)) % 44;

        return switch (kind) {
            0 => .{ .null = {} },
            1 => .{ .i8 = {} },
            2 => .{ .i16 = {} },
            3 => .{ .i32 = {} },
            4 => .{ .i64 = {} },
            5 => .{ .u8 = {} },
            6 => .{ .u16 = {} },
            7 => .{ .u32 = {} },
            8 => .{ .u64 = {} },
            9 => .{ .f16 = {} },
            10 => .{ .f32 = {} },
            11 => .{ .f64 = {} },
            12 => .{ .binary = {} },
            13 => .{ .utf8 = {} },
            14 => .{ .bool = {} },
            15 => .{ .decimal32 = try self.decimal_params(.i32) },
            16 => .{ .decimal64 = try self.decimal_params(.i64) },
            17 => .{ .decimal128 = try self.decimal_params(.i128) },
            18 => .{ .decimal256 = try self.decimal_params(.i256) },
            19 => .{ .date32 = {} },
            20 => .{ .date64 = {} },
            21 => .{ .time32 = try self.time_unit(.i32) },
            22 => .{ .time64 = try self.time_unit(.i64) },
            23 => .{ .timestamp = try self.timestamp(alloc) },
            24 => .{ .interval_year_month = {} },
            25 => .{ .interval_day_time = {} },
            26 => .{ .interval_month_day_nano = {} },
            27 => .{ .list = try make_ptr(data_type.DataType, try self.make_data_type_impl(alloc, depth + 1), alloc) },
            28 => .{ .struct_ = try make_ptr(data_type.StructType, try self.struct_type(alloc, depth), alloc) },
            29 => .{ .dense_union = try make_ptr(data_type.UnionType, try self.union_type(alloc, depth), alloc) },
            30 => .{ .sparse_union = try make_ptr(data_type.UnionType, try self.union_type(alloc, depth), alloc) },
            31 => .{ .fixed_size_binary = try self.fixed_size_binary_width() },
            32 => .{ .fixed_size_list = try make_ptr(data_type.FixedSizeListType, try self.fixed_size_list_type(alloc, depth), alloc) },
            33 => .{ .map = try make_ptr(data_type.MapType, try self.map_type(alloc, depth), alloc) },
            34 => .{ .duration = try self.timestamp_unit() },
            35 => .{ .large_binary = {} },
            36 => .{ .large_utf8 = {} },
            37 => .{ .large_list = try make_ptr(data_type.DataType, try self.make_data_type_impl(alloc, depth + 1), alloc) },
            38 => .{ .run_end_encoded = try make_ptr(data_type.RunEndEncodedType, try self.run_end_encoded_type(alloc, depth), alloc) },
            39 => .{ .binary_view = {} },
            40 => .{ .utf8_view = {} },
            41 => .{ .list_view = try make_ptr(data_type.DataType, try self.make_data_type_impl(alloc, depth + 1), alloc) },
            42 => .{ .large_list_view = try make_ptr(data_type.DataType, try self.make_data_type_impl(alloc, depth + 1), alloc) },
            43 => .{ .dict = try make_ptr(data_type.DictType, try self.dict_type(alloc, depth), alloc) },
            else => unreachable,
        };
    }

    pub fn dict_type(self: *FuzzInput, alloc: Allocator, depth: u8) Error!data_type.DictType {
        const value = try self.make_data_type_impl(alloc, depth + 1);

        return .{
            .key = .i32,
            .value = value,
        };
    }

    pub fn run_end_encoded_type(self: *FuzzInput, alloc: Allocator, depth: u8) Error!data_type.RunEndEncodedType {
        const value = try self.make_data_type_impl(alloc, depth + 1);

        return .{
            .run_end = .i32,
            .value = value,
        };
    }

    pub fn map_type(self: *FuzzInput, alloc: Allocator, depth: u8) Error!data_type.MapType {
        const value = try self.make_data_type_impl(alloc, depth + 1);

        return .{
            .key = .binary,
            .value = value,
        };
    }

    pub fn fixed_size_list_type(self: *FuzzInput, alloc: Allocator, depth: u8) Error!data_type.FixedSizeListType {
        const item_width = (try self.int(u8)) % 10 + 1;
        return .{
            .inner = try self.make_data_type_impl(alloc, depth + 1),
            .item_width = item_width,
        };
    }

    pub fn fixed_size_binary_width(self: *FuzzInput) Error!i32 {
        return (try self.int(u8)) % 69 + 1;
    }

    pub fn union_type(self: *FuzzInput, alloc: Allocator, depth: u8) Error!data_type.UnionType {
        const num_children = (try self.int(u8)) % 5 + 1;

        const type_id_set = try alloc.alloc(i8, num_children);
        for (0..num_children) |child_idx| {
            type_id_set[child_idx] = @intCast(child_idx);
        }

        const field_names = try alloc.alloc([:0]const u8, num_children);

        var prng = try self.make_prng();
        const rand = prng.random();

        for (0..num_children) |child_idx| {
            const field_name = try make_name(field_names[0..child_idx], rand, alloc);
            field_names[child_idx] = field_name;
        }

        const field_types = try alloc.alloc(data_type.DataType, num_children);

        for (0..num_children) |field_idx| {
            field_types[field_idx] = try self.make_data_type_impl(alloc, depth + 1);
        }

        return .{
            .field_names = field_names,
            .field_types = field_types,
            .type_id_set = type_id_set,
        };
    }

    pub fn struct_type(self: *FuzzInput, alloc: Allocator, depth: u8) Error!data_type.StructType {
        const num_fields = (try self.int(u8)) % 5 + 1;

        const field_names = try alloc.alloc([:0]const u8, num_fields);

        var prng = try self.make_prng();
        const rand = prng.random();

        for (0..num_fields) |field_idx| {
            const field_name = try make_name(field_names[0..field_idx], rand, alloc);
            field_names[field_idx] = field_name;
        }

        const field_types = try alloc.alloc(data_type.DataType, num_fields);

        for (0..num_fields) |field_idx| {
            field_types[field_idx] = try self.make_data_type_impl(alloc, depth + 1);
        }

        return .{
            .field_names = field_names,
            .field_types = field_types,
        };
    }

    pub fn decimal_params(self: *FuzzInput, comptime decimal_t: arr.DecimalInt) Error!arr.DecimalParams {
        const max_precision = switch (decimal_t) {
            .i32 => 9,
            .i64 => 19,
            .i128 => 38,
            .i256 => 76,
        };

        return .{
            .scale = try self.int(i8),
            .precision = (try self.int(u8)) % max_precision + 1,
        };
    }

    pub fn time_unit(self: *FuzzInput, comptime backing_t: arr.IndexType) Error!arr.TimeArray(backing_t).Unit {
        const unit_bit = (try self.int(u8)) % 2 == 0;

        return switch (backing_t) {
            .i32 => if (unit_bit) .second else .millisecond,
            .i64 => if (unit_bit) .microsecond else .nanosecond,
        };
    }

    pub fn timestamp_unit(self: *FuzzInput) Error!arr.TimestampUnit {
        const unit_int: u8 = (try self.int(u8)) % 4;
        return switch (unit_int) {
            0 => .second,
            1 => .millisecond,
            2 => .microsecond,
            3 => .nanosecond,
            else => unreachable,
        };
    }

    pub fn timestamp(self: *FuzzInput, alloc: Allocator) Error!arr.Timestamp {
        const unit = try self.timestamp_unit();
        var ts = arr.Timestamp{
            .unit = unit,
            .timezone = null,
        };

        const timezone_int = try self.int(u8);
        const has_timezone = timezone_int % 2 == 0;

        var prng = try self.make_prng();
        const rand = prng.random();

        if (has_timezone) {
            const tz_len = try self.int(u8) % 40 + 1;
            const tz = try alloc.alloc(u8, tz_len);
            rand_bytes_zero_sentinel(rand, tz);
            ts.timezone = tz;
        }

        return ts;
    }

    pub fn make_scalar(self: *FuzzInput, alloc: Allocator) Error!scalar.Scalar {
        const kind = (try self.int(u8)) % 17;

        return switch (kind) {
            0 => .{ .null = {} },
            1 => .{ .i8 = try self.int(i8) },
            2 => .{ .i16 = try self.int(i16) },
            3 => .{ .i32 = try self.int(i32) },
            4 => .{ .i64 = try self.int(i64) },
            5 => .{ .u8 = try self.int(u8) },
            6 => .{ .u16 = try self.int(u16) },
            7 => .{ .u32 = try self.int(u32) },
            8 => .{ .u64 = try self.int(u64) },
            9 => .{ .f16 = try self.float(f16) },
            10 => .{ .f32 = try self.float(f32) },
            11 => .{ .f64 = try self.float(f64) },
            12 => .{ .i128 = try self.int(i128) },
            13 => .{ .i256 = try self.int(i256) },
            14 => make_binary: {
                const len = try self.int(u8);
                const data = try self.slice(u8, len, alloc);
                break :make_binary .{ .binary = data };
            },
            15 => .{ .bool = try self.boolean() },
            16 => make_list: {
                const len = try self.int(u8);
                const array = try alloc.create(arr.Array);
                array.* = try self.make_array(len, alloc);
                break :make_list .{ .list = array };
            },
            else => unreachable,
        };
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

fn make_name(existing_names: []const []const u8, rand: std.Random, alloc: Allocator) ![:0]const u8 {
    const name_len = rand.int(u8) % 30 + 1;
    const name = try alloc.allocSentinel(u8, name_len, 0);

    namegen: while (true) {
        rand_bytes_zero_sentinel(rand, name);

        for (existing_names) |other_name| {
            if (std.mem.eql(u8, name, other_name)) {
                continue :namegen;
            }
        }
        break;
    }

    return name;
}

fn make_ptr(comptime T: type, v: T, alloc: Allocator) Error!*const T {
    const ptr = try alloc.create(T);
    ptr.* = v;
    return ptr;
}

test "smoke" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var input = FuzzInput.init(&.{ 1, 2, 3, 4, 5, 6 });

    _ = input.make_scalar(alloc) catch {};
}
