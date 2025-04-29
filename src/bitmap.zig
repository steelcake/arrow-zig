const std = @import("std");
const testing = std.testing;

pub fn get(buf: [*]const u8, bit_index: u32) bool {
    const byte_index = bit_index / 8;
    const bit_shift: u3 = @intCast(bit_index % 8);
    const mask = @as(u8, 1) << bit_shift;
    return buf[byte_index] & mask != 0;
}

pub fn set(buf: [*]u8, bit_index: u32) void {
    const byte_index = bit_index / 8;
    const bit_shift: u3 = @intCast(bit_index % 8);
    const mask = @as(u8, 1) << bit_shift;
    buf[byte_index] |= mask;
}

pub fn unset(buf: [*]u8, bit_index: u32) void {
    const byte_index = bit_index / 8;
    const bit_shift: u3 = @intCast(bit_index % 8);
    const mask = @as(u8, 1) << bit_shift;
    buf[byte_index] &= ~mask;
}

pub fn num_bytes(num_bits: u32) u32 {
    return (num_bits + 7) / 8;
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
            try testing.expectEqual(false, get(bitmap.ptr, i));
            set(bitmap.ptr, i);
            try testing.expectEqual(true, get(bitmap.ptr, i));
        }
    }

    i = 0;
    while (i < len) : (i += 1) {
        if (i % 2 == 0) {
            try testing.expectEqual(true, get(bitmap.ptr, i));
            unset(bitmap.ptr, i);
            try testing.expectEqual(false, get(bitmap.ptr, i));
        } else {
            try testing.expectEqual(false, get(bitmap.ptr, i));
            set(bitmap.ptr, i);
            try testing.expectEqual(true, get(bitmap.ptr, i));
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
