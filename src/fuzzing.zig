const std = @import("std");
const Allocator = std.mem.Allocator;
const arr = @import("./array.zig");
const Prng = std.Random.DefaultPrng;
const bitmap = @import("./bitmap.zig");

const Error = error{ OutOfMemory, ShortInput };

const Validity = struct {
    validity: []const u8,
    null_count: u32,
};

/// This struct implements structured fuzzing.
///
/// It generates arrow arrays based on given fuzzer generated random data.
/// Normal random number generator seeded by fuzzer input is used in some places where the specific values don't change execution but we want them to be random.
pub const FuzzInput = struct {
    data: []const u8,

    fn int(self: *FuzzInput, comptime T: type) Error!T {
        const size = @sizeOf(T);
        if (self.data.len < size) {
            return Error.ShortInput;
        }
        const i = std.mem.readVarInt(T, self.data[0..size]);
        self.data = self.data[size..];
        return i;
    }

    fn boolean(self: *FuzzInput) Error!bool {
        if (self.data.len == 0) {
            return Error.ShortInput;
        }
        const byte = self.data[0];
        self.data = self.data[1..];
        return byte % 2 == 0;
    }

    fn make_prng(self: *FuzzInput) Error!Prng {
        const seed = try self.next_int(u64);
        return Prng.init(seed);
    }

    fn bytes(self: *FuzzInput, len: u32) Error![]const u8 {
        if (self.bytes.len < len) {
            return Error.ShortInput;
        }
        const b = self.data[0..len];
        self.data = self.data[len..];
        return b;
    }

    fn slice(self: *FuzzInput, comptime T: type, len: u32, alloc: Allocator) Error![]T {
        const out = try alloc.alloc(T, len);
        const out_raw: []u8 = @ptrCast(out);
        @memcpy(out_raw, try self.bytes(out_raw.len));
        return out;
    }

    fn validity(self: *FuzzInput, len: u32, alloc: Allocator) Error!?Validity {
        const has_validity = try self.next_bool();

        if (!has_validity) {
            return null;
        }

        const v_len = (len + 7) / 8;
        const v = try alloc.alloc(u8, v_len);
        @memset(v, 0);
        var null_count = len;

        var prng = try self.make_prng();
        const rand = prng.random();

        var idx: u32 = 0;
        while (idx < len) : (idx += 1) {
            if (rand.boolean()) {
                null_count -%= 1;
                bitmap.set(v.ptr, idx);
            }
        }

        return .{ .validity = v, .null_count = null_count };
    }

    pub fn null_array(self: *FuzzInput) Error!arr.NullArray {
        const len = try self.next_int(u8);
        return .{ .len = len };
    }

    pub fn primitive_array(self: *FuzzInput, comptime T: type, alloc: Allocator) Error!arr.PrimitiveArray {
        const len: u32 = try self.next_int(u8);
        const offset: u32 = try self.next_int(u8);
        const total_len = len + offset;

        var prng = try self.make_prng();
        const rand = prng.random();
        const values = try alloc.alloc(T, total_len);

        const values_raw: []u8 = @ptrCast(values);
        rand.bytes(values_raw);

        var array = arr.PrimitiveArray{
            .len = len,
            .offset = offset,
            .values = values,
            .validity = null,
            .null_count = 0,
        };

        if (try self.validity(total_len, alloc)) |v| {
            array.validity = v.validity;
            array.null_count = v.null_count;
        }

        return array;
    }

    pub fn binary_array(self: *FuzzInput, comptime index_t: arr.IndexType, alloc: Allocator) Error!arr.GenericBinaryArray(index_t) {
        const I = index_t.to_type();

        const len: u32 = try self.next_int(u8);
        const offset: u32 = try self.next_int(u8);
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

        var prng = try self.next_prng();
        const rand = prng.random();

        const data_len = offsets[total_len];

        const data = try alloc.alloc(u8, data_len);
        rand.bytes(data);

        const array = arr.GenericBinaryArray(index_t){
            .len = len,
            .offset = offset,
            .data = data,
            .offsets = offsets,
            .validity = null,
            .null_count = 0,
        };

        if (try self.validity(total_len, alloc)) |v| {
            array.validity = v.validity;
            array.null_count = v.null_count;
        }

        return array;
    }

    pub fn utf8_array(self: *FuzzInput, comptime index_t: arr.IndexType, alloc: Allocator) Error!arr.GenericUtf8Array(index_t) {
        return .{ .inner = try self.binary_array(index_t, alloc) };
    }

    pub fn bool_array(self: *FuzzInput, alloc: Allocator) Error!arr.BoolArray {
        const len: u32 = try self.next_int(u8);
        const offset: u32 = try self.next_int(u8);
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

        if (try self.validity(total_len, alloc)) |v| {
            array.validity = v.validity;
            array.null_count = v.null_count;
        }

        return array;
    }

    pub fn decimal_array(self: *FuzzInput, comptime decimal_t: arr.DecimalInt, alloc: Allocator) Error!arr.DecimalArray(decimal_t) {
        const scale = try self.int(i8);
        const precision = try self.int(u8);

        const params = arr.DecimalParams{ .scale = scale, .precision = precision };

        const inner = try self.primitive_array(decimal_t.to_type(), alloc);

        return .{ .params = params, .inner = inner };
    }

    pub fn date_array(self: *FuzzInput, comptime backing_t: arr.IndexType, alloc: Allocator) Error!arr.DateArray(backing_t) {
        return .{ .inner = try self.primitive_array(backing_t.to_type(), alloc) };
    }

    pub fn time_array(self: *FuzzInput, comptime backing_t: arr.IndexType, alloc: Allocator) Error!arr.TimeArray(backing_t) {
        const Unit = arr.TimeArray(backing_t).Unit;

        const unit_bit = (try self.int(u8)) % 2 == 0;

        const unit: Unit = switch (backing_t) {
            .i32 => if (unit_bit) .second else .millisecond,
            .i64 => if (unit_bit) .microsecond else .nanosecond,
        };

        return .{ .unit = unit, .inner = try self.primitive_array(backing_t.to_type(), alloc) };
    }

    fn timestamp_unit(self: *FuzzInput) Error!arr.TimestampUnit {
        const unit_int: u2 = (try self.int(u8)) % 4;
        return switch (unit_int) {
            0 => .second,
            1 => .millisecond,
            2 => .microsecond,
            3 => .nanosecond,
        };
    }

    pub fn timestamp_array(self: *FuzzInput, alloc: Allocator) Error!arr.TimestampArray {
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

        return .{ .ts = timestamp, .inner = try self.primitive_array(i64, alloc) };
    }

    pub fn interval_array(self: *FuzzInput, comptime interval_t: arr.IntervalType, alloc: Allocator) Error!arr.IntervalArray(interval_t) {
        return .{
            .inner = try self.primitive_array(interval_t.to_type(), alloc),
        };
    }

    pub fn fixed_size_binary(self: *FuzzInput, alloc: Allocator) Error!arr.FixedSizeBinaryArray {
        const len: u32 = try self.next_int(u8);
        const offset: u32 = try self.next_int(u8);
        const total_len: u32 = len + offset;

        const byte_width = try self.next_int(u8);

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

        if (try self.validity(total_len, alloc)) |v| {
            array.validity = v.validity;
            array.null_count = v.null_count;
        }

        return array;
    }

    pub fn duration_array(self: *FuzzInput, alloc: Allocator) Error!arr.DurationArray {
        return .{ .unit = try self.timestamp_unit(), .inner = try self.primitive_array(i64, alloc) };
    }

    pub fn binary_view(self: *FuzzInput, alloc: Allocator) Error!arr.BinaryViewArray {
        const len: u32 = try self.next_int(u8);
        const offset: u32 = try self.next_int(u8);
        const total_len: u32 = len + offset;

        const max_str_len = 40;
        const num_buffers = (try self.int(u8)) % 5;
        const buffer_len = max_str_len * total_len;

        const views = try self.slice(arr.BinaryView, total_len);
        for (0..views.len) |view_idx| {
            var view = views.ptr[view_idx];
            const length = @as(u32, @bitCast(view.length)) % (max_str_len + 1);

            if (length > 12) {
                const buffer_idx = @as(u32, @bitCast(view.buffer_idx)) % num_buffers;
                view.buffer_idx = @bitCast(buffer_idx);
                const max_offset = buffer_len - length;
                view.offset = @intCast(view.offset % max_offset);
            }

            views.ptr[view_idx] = view;
        }

        var prng = try self.next_prng();
        const rand = prng.random();

        const buffers = try alloc.alloc([*]const u8, num_buffers);
        for (0..num_buffers) |buffer_idx| {
            const buffer = try alloc.alloc(u8, buffer_len);
            rand.bytes(buffer);
            buffers[buffer_idx] = buffer;
        }

        const array = arr.BinaryViewArray{
            .len = len,
            .offset = offset,
            .buffers = buffers,
            .views = views,
            .validity = null,
            .null_count = 0,
        };

        if (try self.validity(total_len, alloc)) |v| {
            array.validity = v.validity;
            array.null_count = v.null_count;
        }

        return array;
    }

    pub fn utf8_view(self: *FuzzInput, alloc: Allocator) Error!arr.Utf8ViewArray {
        return .{ .inner = try self.binary_view(alloc) };
    }
};
