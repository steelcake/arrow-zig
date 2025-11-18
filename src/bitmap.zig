const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

pub fn get(buf: []const u8, bit_index: u32) bool {
    const byte_index = bit_index / 8;
    const bit_shift: u3 = @truncate(bit_index % 8);
    const mask = @as(u8, 1) << bit_shift;
    return buf[byte_index] & mask != 0;
}

pub fn set(buf: []u8, bit_index: u32) void {
    const byte_index = bit_index / 8;
    const bit_shift: u3 = @truncate(bit_index % 8);
    const mask = @as(u8, 1) << bit_shift;
    buf[byte_index] |= mask;
}

pub fn unset(buf: []u8, bit_index: u32) void {
    const byte_index = bit_index / 8;
    const bit_shift: u3 = @truncate(bit_index % 8);
    const mask = @as(u8, 1) << bit_shift;
    buf[byte_index] &= ~mask;
}

pub fn num_bytes(num_bits: u32) u32 {
    return (num_bits +% 7) / 8;
}

pub fn count_nulls(validity: []const u8, offset: u32, len: u32) u32 {
    std.debug.assert((offset + len + 7) / 8 <= validity.len);

    if (len == 0) return 0;

    var null_count: u32 = 0;

    const start_padding = offset % 8;
    if (start_padding > 0) {
        const start_bits = @min(len, 8 - start_padding);
        var start_byte = std.math.shr(u8, validity[offset / 8], start_padding);
        start_byte &= BYTE_MASK[start_bits];
        null_count += (start_bits - @popCount(start_byte));
        if (start_bits == len) return null_count;
    }

    const bytes_start = (offset + 7) / 8;
    const bytes_end = (offset + len) / 8;

    if (bytes_end > bytes_start) {
        const n_bytes = bytes_end - bytes_start;

        const num_words = n_bytes / 64;
        const words: []align(1) const u64 = @ptrCast(validity[bytes_start .. bytes_start + num_words * 64]);
        var word_idx: u32 = 0;
        while (word_idx < words.len) : (word_idx += 1) {
            const word = words[word_idx];
            null_count += (64 - @popCount(word));
        }

        var byte_idx = bytes_start + num_words * 64;
        while (byte_idx < bytes_end) : (byte_idx += 1) {
            const byte = validity[byte_idx];
            null_count += (8 - @popCount(byte));
        }
    }

    if (bytes_end * 8 < offset + len) {
        const end_bits = (offset + len) % 8;
        const end_byte = validity[bytes_end] & BYTE_MASK[end_bits];
        null_count += (end_bits - @popCount(end_byte));
    }

    return null_count;
}

// pub fn copy(
//     validity: []const u8,
//     offset: u32,
//     len: u32,
//     alloc: Allocator,
// ) error{OutOfMemory}![]u8 {
//     const out = try alloc.alloc(u8, (len + 7) / 8);

//     if (offset % 8 == 0) {
//         const start = offset / 8;
//         const end = (offset + len + 7) / 8;
//         @memcpy(out, validity[start..end]);
//     } else {

//     }
// }

const BYTE_MASK: [8]u8 = .{
    0b00000000,
    0b00000001,
    0b00000011,
    0b00000111,
    0b00001111,
    0b00011111,
    0b00111111,
    0b01111111,
};

pub fn for_each(
    comptime Context: type,
    comptime process: fn (ctx: Context, idx: u32) void,
    ctx: Context,
    validity: []const u8,
    offset: u32,
    len: u32,
) void {
    if (len == 0) return;

    const start_padding = offset % 8;
    if (start_padding > 0) {
        const start_bits = @min(len, 8 - start_padding);
        var start_byte = std.math.shr(u8, validity[offset / 8], start_padding);
        start_byte &= BYTE_MASK[start_bits];
        while (start_byte != 0) {
            const t = start_byte & negate(start_byte);
            const r: u8 = @ctz(start_byte);
            process(ctx, offset + r);
            start_byte ^= t;
        }

        if (start_bits == len) return;
    }

    const bytes_start = (offset + 7) / 8;
    const bytes_end = (offset + len) / 8;

    if (bytes_end > bytes_start) {
        const n_bytes = bytes_end - bytes_start;

        const num_words = n_bytes / 64;
        const words: []align(1) const u64 = @ptrCast(validity[bytes_start .. bytes_start + num_words * 64]);
        var word_idx: u32 = 0;
        const base_offset = bytes_start * 8;
        while (word_idx < words.len) : (word_idx += 1) {
            var word = words[word_idx];
            while (word != 0) {
                const t = word & negate(word);
                const r: u8 = @ctz(word);
                process(ctx, base_offset + word_idx * 64 + r);
                word ^= t;
            }
        }

        var byte_idx = bytes_start + num_words * 64;
        while (byte_idx < bytes_end) : (byte_idx += 1) {
            var byte = validity[byte_idx];
            while (byte != 0) {
                const t = byte & negate(byte);
                const r: u8 = @ctz(byte);
                process(ctx, byte_idx * 8 + r);
                byte ^= t;
            }
        }
    }

    if (bytes_end * 8 < offset + len) {
        const end_bits = (offset + len) % 8;
        const base_offset = (offset + len) / 8 * 8;
        var end_byte = validity[bytes_end] & BYTE_MASK[end_bits];
        while (end_byte != 0) {
            const t = end_byte & negate(end_byte);
            const r: u8 = @ctz(end_byte);
            process(ctx, base_offset + r);
            end_byte ^= t;
        }
    }
}

fn negate(x: anytype) @TypeOf(x) {
    return ~x +% 1;
}

test "bitmap get set unset" {
    const len: u32 = 100;
    const byte_len = num_bytes(len);

    const bitmap = try testing.allocator.alloc(u8, byte_len);
    defer testing.allocator.free(bitmap);
    @memset(bitmap, 0);

    var i: u32 = 0;
    while (i < len) : (i += 1) {
        if (i % 2 == 0) {
            try testing.expectEqual(false, get(bitmap, i));
            set(bitmap, i);
            try testing.expectEqual(true, get(bitmap, i));
        }
    }

    i = 0;
    while (i < len) : (i += 1) {
        if (i % 2 == 0) {
            try testing.expectEqual(true, get(bitmap, i));
            unset(bitmap, i);
            try testing.expectEqual(false, get(bitmap, i));
        } else {
            try testing.expectEqual(false, get(bitmap, i));
            set(bitmap, i);
            try testing.expectEqual(true, get(bitmap, i));
        }
    }
}

test num_bytes {
    try testing.expectEqual(0, num_bytes(0));
    try testing.expectEqual(1, num_bytes(1));
    try testing.expectEqual(1, num_bytes(8));
    try testing.expectEqual(2, num_bytes(9));
    try testing.expectEqual(2, num_bytes(16));
}

test count_nulls {
    const validity = &[_]u8{ 0b11110110, 0b00000001 };

    try testing.expectEqual(0, count_nulls(validity, 1, 2));
    try testing.expectEqual(1, count_nulls(validity, 0, 3));
    try testing.expectEqual(1, count_nulls(validity, 8, 2));
    try testing.expectEqual(4, count_nulls(validity, 8, 5));
    try testing.expectEqual(6, count_nulls(validity, 8, 7));
    try testing.expectEqual(2, count_nulls(validity, 1, 9));
}
