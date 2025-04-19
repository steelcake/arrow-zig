const std = @import("std");
const Allocator = std.mem.Allocator;

const arr = @import("./array.zig");
const abi = @cImport(@cInclude("arrow_abi.h"));

pub const FFI_Array = struct {
    schema: abi.ArrowSchema,
    array: abi.ArrowArray,

    fn release(self: *FFI_Array) void {
        if (self.schema.release) |rel| {
            rel(&self.schema);
        }
        if (self.array.release) |rel| {
            rel(&self.array);
        }
    }
};

fn validity_size(size: u32) u32 {
    return (size + 7) / 8;
}

fn import_buffer(comptime T: type, buf: ?*const anyopaque, size: u32) []const T {
    const ptr: [*]const T = @ptrCast(@alignCast(buf));
    return ptr[0..size];
}

pub fn import_(array: FFI_Array, allocator: Allocator) !arr.Array {
    const format: []const u8 = std.mem.span(array.schema.format);
    if (format.len == 0) {
        return error.InvalidFFIArray;
    }

    const start = format[0];

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;

    switch (start) {
        'n' => {
            const null_arr = try allocator.create(arr.NullArray);
            null_arr.* = arr.NullArray{
                .len = len,
                .offset = offset,
            };

            return arr.Array.from(null_arr);
        },
        'b' => {
            const buffers = array.array.buffers.?;
            if (array.array.n_buffers != 2) {
                return error.InvalidFFIArray;
            }

            const byte_size = validity_size(size);

            const bool_arr = try allocator.create(arr.BoolArray);
            bool_arr.* = arr.BoolArray{
                .values = import_buffer(u8, buffers[1], byte_size),
                .validity = import_buffer(
                    u8,
                    buffers[0],
                    byte_size,
                ),
                .len = len,
                .offset = offset,
            };

            return arr.Array.from(bool_arr);
        },
        else => {},
    }

    return error.UnexpectedFormatStr;
}

pub fn export_(_: arr.Array, _: Allocator) FFI_Array {
    unreachable;
}

test "roundtrip" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const values = [_]i32{ 3, 2, 1, 4, 5, 6 };

    const typed = arr.Int32Array{
        .len = 5,
        .offset = 1,
        .validity = null,
        .values = &values,
    };

    const array = arr.Array.from(&typed);

    var ffi_array = export_(array, allocator);
    defer ffi_array.release(); // not needed but doing this for testing

    const roundtrip_array = try import_(ffi_array, allocator);

    const roundtrip_typed = roundtrip_array.to(.i32);

    try std.testing.expectEqualDeep(roundtrip_typed, &typed);
}
