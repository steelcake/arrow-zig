const std = @import("std");
const testing = std.testing;
const arr = @import("./array.zig");
const bitmap = @import("./bitmap.zig");
const slice_mod = @import("./slice.zig");
const slice = slice_mod.slice;
const slice_struct = slice_mod.slice_struct;

pub fn get_bool(values: []const u8, index: u32) bool {
    return bitmap.get(values, index);
}

pub fn get_primitive(comptime T: type, values: []const T, index: u32) T {
    return values[index];
}

fn index_cast(comptime index_type: arr.IndexType, index: index_type.to_type()) usize {
    return switch (index_type) {
        .i32 => @intCast(@as(u32, @intCast(index))),
        .i64 => @intCast(@as(u64, @intCast(index))),
    };
}

pub fn get_binary(comptime index_type: arr.IndexType, data: []const u8, offsets: []const index_type.to_type(), index: u32) []const u8 {
    const start = index_cast(index_type, offsets[index]);
    const end = index_cast(index_type, offsets[index + 1]);

    return data[start..end];
}

pub fn get_binary_view(buffers: []const []const u8, views: []const arr.BinaryView, index: u32) []const u8 {
    const view = views[index];
    const vl = @as(u32, @intCast(view.length));

    if (view.length <= 12) {
        return @as([]const u8, @ptrCast(&views[index]))[4 .. vl + 4];
    } else {
        const vo = @as(u32, @intCast(view.offset));
        const vbi = @as(u32, @intCast(view.buffer_idx));
        return buffers[vbi][vo .. vo + vl];
    }
}

pub fn get_fixed_size_binary(data: []const u8, byte_width: i32, index: u32) []const u8 {
    const bw = @as(u32, @intCast(byte_width));
    const start = bw * index;
    const end = start + bw;
    return data[start..end];
}

pub fn get_list(comptime index_type: arr.IndexType, inner: *const arr.Array, offsets: []const index_type.to_type(), index: u32) arr.Array {
    const start: u32 = @intCast(index_cast(index_type, offsets[index]));
    const end: u32 = @intCast(index_cast(index_type, offsets[index + 1]));
    return slice(inner, start, end - start);
}

pub fn get_list_view(comptime index_type: arr.IndexType, inner: *const arr.Array, offsets: []const index_type.to_type(), sizes: []const index_type.to_type(), index: u32) arr.Array {
    const start: u32 = @intCast(index_cast(index_type, offsets[index]));
    const size: u32 = @intCast(index_cast(index_type, sizes[index]));
    return slice(inner, start, size);
}

pub fn get_fixed_size_list(inner: *const arr.Array, item_width: i32, index: u32) arr.Array {
    const iw = @as(u32, @intCast(item_width));
    const start = iw * index;
    return slice(inner, start, iw);
}

pub fn get_map(entries: *const arr.StructArray, offsets: []const i32, index: u32) arr.StructArray {
    const start: u32 = @intCast(index_cast(.i32, offsets[index]));
    const end: u32 = @intCast(index_cast(.i32, offsets[index + 1]));
    return slice_struct(entries, start, end - start);
}
