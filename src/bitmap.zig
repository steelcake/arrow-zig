pub fn get(buf: []const u8, bit_index: u32) bool {
    const byte_index = bit_index / 8;
    const bit_shift: u3 = @intCast(bit_index % 8);
    const mask = @as(u8, 1) << bit_shift;
    return buf.ptr[byte_index] & mask != 0;
}

pub fn set(buf: []u8, bit_index: u32) void {
    const byte_index = bit_index / 8;
    const bit_shift: u3 = @intCast(bit_index % 8);
    const mask = @as(u8, 1) << bit_shift;
    buf.ptr[byte_index] |= mask;
}

pub fn unset(buf: []u8, bit_index: u32) void {
    const byte_index = bit_index / 8;
    const bit_shift: u3 = @intCast(bit_index % 8);
    const mask = @as(u8, 1) << bit_shift;
    buf.ptr[byte_index] &= ~mask;
}
