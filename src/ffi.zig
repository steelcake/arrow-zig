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
    const buf_ptr = buf orelse return &.{};
    const ptr: [*]const T = @ptrCast(@alignCast(buf_ptr));
    return ptr[0..size];
}

fn import_primitive(comptime T: type, comptime ArrT: type, array: *const FFI_Array, allocator: Allocator) !arr.Array {
    const buffers = array.array.buffers.?;
    if (array.array.n_buffers != 2) {
        return error.InvalidFFIArray;
    }

    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;
    const byte_size = validity_size(size);

    const validity = if (buffers[0]) |b| import_buffer(u8, b, byte_size) else null;

    const arr_ptr = try allocator.create(ArrT);
    arr_ptr.* = ArrT{
        .values = import_buffer(T, buffers[1], size),
        .validity = validity,
        .len = len,
        .offset = offset,
    };

    return arr.Array.from(arr_ptr);
}

pub fn import_(array: FFI_Array, allocator: Allocator) !arr.Array {
    const format_str = array.schema.format orelse return error.InvalidFFIArray;
    const format = std.mem.span(format_str);
    if (format.len == 0) {
        return error.InvalidFFIArray;
    }
    const len: u32 = @intCast(array.array.length);
    const offset: u32 = @intCast(array.array.offset);
    const size: u32 = len + offset;

    switch (format[0]) {
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
            const validity = if (buffers[0]) |b| import_buffer(u8, b, byte_size) else null;

            const bool_arr = try allocator.create(arr.BoolArray);
            bool_arr.* = arr.BoolArray{
                .values = import_buffer(u8, buffers[1], byte_size),
                .validity = validity,
                .len = len,
                .offset = offset,
            };

            return arr.Array.from(bool_arr);
        },
        'c' => {
            return import_primitive(i8, arr.Int8Array, &array, allocator);
        },
        'C' => {
            return import_primitive(u8, arr.UInt8Array, &array, allocator);
        },
        's' => {
            return import_primitive(i16, arr.Int16Array, &array, allocator);
        },
        'S' => {
            return import_primitive(u16, arr.UInt16Array, &array, allocator);
        },
        'i' => {
            return import_primitive(i32, arr.Int32Array, &array, allocator);
        },
        'I' => {
            return import_primitive(u32, arr.UInt32Array, &array, allocator);
        },
        'l' => {
            return import_primitive(i64, arr.Int64Array, &array, allocator);
        },
        'L' => {
            return import_primitive(u64, arr.UInt64Array, &array, allocator);
        },
        'e' => {
            return import_primitive(f16, arr.Float16Array, &array, allocator);
        },
        'f' => {
            return import_primitive(f32, arr.Float32Array, &array, allocator);
        },
        'g' => {
            return import_primitive(f64, arr.Float64Array, &array, allocator);
        },
        else => return error.UnexpectedFormatStr,
    }
}

fn export_primitive(comptime ArrT: arr.ArrayType) FFI_Array {

}

pub fn export_(array: arr.Array, _: Allocator) FFI_Array {
    switch (array.type_) {
        .null => {
            const a = array.to(.null);

            return .{
                .schema = .{},
                .array = .{
                    .length = a.len,
                    .offset = a.offset,
                },
            };
        },
        .i8 => {

        },
        i16,
        i32,
        i64,
        u8,
        u16,
        u32,
        u64,
        f16,
        f32,
        f64,
        binary,
        utf8,
        bool,
        else => unreachable,
    }
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
    defer ffi_array.release(); 

    const roundtrip_array = try import_(ffi_array, allocator);

    const roundtrip_typed = roundtrip_array.to(.i32);

    try std.testing.expectEqualDeep(roundtrip_typed, &typed);
}
