const std = @import("std");
const testing = std.testing;

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
    return (num_bits + 7) / 8;
}

pub fn count_unset_bits(validity: []const u8, offset: u32, len: u32) u32 {
    return len - count_set_bits(validity, offset, len);
}

pub fn count_set_bits(validity: []const u8, offset: u32, len: u32) u32 {
    std.debug.assert((offset + len + 7) / 8 <= validity.len);

    if (len == 0) return 0;

    var n_set: u32 = 0;

    const start_padding = offset % 8;
    if (start_padding > 0) {
        const start_bits = @min(len, 8 - start_padding);
        var start_byte = std.math.shr(u8, validity[offset / 8], start_padding);
        start_byte &= BYTE_MASK[start_bits];
        n_set += @popCount(start_byte);
        if (start_bits == len) return n_set;
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
            n_set += @popCount(word);
        }

        var byte_idx = bytes_start + num_words * 64;
        while (byte_idx < bytes_end) : (byte_idx += 1) {
            const byte = validity[byte_idx];
            n_set += @popCount(byte);
        }
    }

    if (bytes_end * 8 < offset + len) {
        const end_bits = (offset + len) % 8;
        const end_byte = validity[bytes_end] & BYTE_MASK[end_bits];
        n_set += @popCount(end_byte);
    }

    return n_set;
}

pub fn copy(
    len: u32,
    dst: []u8,
    dst_offset: u32,
    src: []const u8,
    src_offset: u32,
) void {
    if (len == 0) return;

    if (dst_offset % 8 == 0 and src_offset % 8 == 0) {
        const src_byte_start = src_offset / 8;
        const dst_byte_start = dst_offset / 8;
        const n_bytes = len / 8;

        @memcpy(
            dst[dst_byte_start .. dst_byte_start + n_bytes],
            src[src_byte_start .. src_byte_start + n_bytes],
        );

        const postfix_bits = len % 8;
        var bit_idx: u32 = n_bytes * 8;
        while (bit_idx < n_bytes * 8 + postfix_bits) : (bit_idx += 1) {
            if (get(src, src_offset + bit_idx)) {
                set(dst, dst_offset + bit_idx);
            } else {
                unset(dst, dst_offset + bit_idx);
            }
        }

        return;
    }

    var bits_copied: u32 = 0;

    while (bits_copied < len) {
        const src_byte_idx = (src_offset + bits_copied) / 8;
        const src_bit_pos = (src_offset + bits_copied) % 8;
        const dst_byte_idx = (dst_offset + bits_copied) / 8;
        const dst_bit_pos = (dst_offset + bits_copied) % 8;

        const bits_in_src_byte = 8 - src_bit_pos;
        const bits_in_dst_byte = 8 - dst_bit_pos;
        const bits_left = len - bits_copied;
        const bits_to_copy = @min(bits_in_src_byte, bits_in_dst_byte, bits_left);

        const src_mask = std.math.shl(u8, std.math.shl(u8, 1, bits_to_copy) -% 1, src_bit_pos);
        const src_bits = std.math.shr(u8, src[src_byte_idx] & src_mask, src_bit_pos);

        const dst_mask = std.math.shl(u8, std.math.shl(u8, 1, bits_to_copy) -% 1, dst_bit_pos);
        dst[dst_byte_idx] &= ~dst_mask;

        dst[dst_byte_idx] |= std.math.shl(u8, src_bits, dst_bit_pos);

        bits_copied += bits_to_copy;
    }
}

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

/// Calls the given process function at every set bit index,
///     passing the given context in.
pub fn for_each(
    comptime Context: type,
    comptime process: fn (ctx: Context, idx: u32) void,
    ctx: Context,
    bitmap: []const u8,
    offset: u32,
    len: u32,
) void {
    if (len == 0) return;

    const start_padding = offset % 8;
    if (start_padding > 0) {
        const start_bits = @min(len, 8 - start_padding);
        var start_byte = std.math.shr(u8, bitmap[offset / 8], start_padding);
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
        const words: []align(1) const u64 = @ptrCast(bitmap[bytes_start .. bytes_start + num_words * 64]);
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
            var byte = bitmap[byte_idx];
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
        var end_byte = bitmap[bytes_end] & BYTE_MASK[end_bits];
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

test count_unset_bits {
    const validity = &[_]u8{ 0b11110110, 0b00000001 };

    try testing.expectEqual(0, count_unset_bits(validity, 1, 2));
    try testing.expectEqual(1, count_unset_bits(validity, 0, 3));
    try testing.expectEqual(1, count_unset_bits(validity, 8, 2));
    try testing.expectEqual(4, count_unset_bits(validity, 8, 5));
    try testing.expectEqual(6, count_unset_bits(validity, 8, 7));
    try testing.expectEqual(2, count_unset_bits(validity, 1, 9));
}
