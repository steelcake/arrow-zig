const std = @import("std");
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const arr = @import("./array.zig");
pub const abi = @cImport(@cInclude("arrow_abi.h"));

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
    const null_count: u32 = @intCast(array.array.null_count);

    const validity = if (buffers[0]) |b| import_buffer(u8, b, byte_size) else null;

    const arr_ptr = try allocator.create(ArrT);
    arr_ptr.* = ArrT{
        .values = import_buffer(T, buffers[1], size),
        .validity = validity,
        .len = len,
        .offset = offset,
        .null_count = null_count,
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
    const null_count: u32 = @intCast(array.array.null_count);

    switch (format[0]) {
        'n' => {
            const null_arr = try allocator.create(arr.NullArray);
            null_arr.* = arr.NullArray{
                .len = len,
                .offset = offset,
                .null_count = null_count,
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
                .null_count = null_count,
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

fn release_array(array: [*c]abi.ArrowArray) callconv(.C) void {
    const ptr = array orelse unreachable;
    const arena: *ArenaAllocator = @ptrCast(@alignCast(ptr.*.private_data));
    const backing_alloc = arena.*.child_allocator;
    arena.deinit();
    backing_alloc.destroy(arena);
}

// This is no-op because actual releasing of memory happens
// when release_array is called
fn release_schema(_: [*c]abi.ArrowSchema) callconv(.C) void {}

/// calling arena.deinit should free all memory relating to this array
///
/// arena should be allocated using arena.child_allocator
///
/// arena.child_allocator should be alive whenever the consumer of FFI_Array decides to call release functions on abi array or schema
///
/// Generally arena should be allocated inside a global allocator like std.heap.GeneralPurposeAlloc and it should have that global alloc as it's child alloc.
/// and array should be allocated using this arena.
pub fn export_(array: arr.Array, arena: *ArenaAllocator) !FFI_Array {
    switch (array.type_) {
        .null => {
            const a = array.to(.null);

            return .{
                .schema = .{
                    .format = "n",
                    .private_data = arena,
                    .release = release_schema,
                },
                .array = .{
                    .length = a.len,
                    .offset = a.offset,
                    .private_data = arena,
                    .release = release_array,
                },
            };
        },
        .i8 => {
            return export_primitive(array.to(.i8), arena);
        },
        .i16 => {
            return export_primitive(array.to(.i16), arena);
        },
        .i32 => {
            return export_primitive(array.to(.i32), arena);
        },
        .i64 => {
            return export_primitive(array.to(.i64), arena);
        },
        .u8 => {
            return export_primitive(array.to(.u8), arena);
        },
        .u16 => {
            return export_primitive(array.to(.u16), arena);
        },
        .u32 => {
            return export_primitive(array.to(.u32), arena);
        },
        .u64 => {
            return export_primitive(array.to(.u64), arena);
        },
        .f16 => {
            return export_primitive(array.to(.f16), arena);
        },
        .f32 => {
            return export_primitive(array.to(.f32), arena);
        },
        .f64 => {
            return export_primitive(array.to(.f64), arena);
        },
        // binary,
        // utf8,
        .bool => {
            return export_primitive(array.to(.bool), arena);
        },
        else => unreachable,
    }
}

fn export_primitive(array: anytype, arena: *ArenaAllocator) !FFI_Array {
    const format = comptime switch (@TypeOf(array)) {
        *const arr.Int8Array => "c",
        *const arr.UInt8Array => "C",
        *const arr.Int16Array => "s",
        *const arr.UInt16Array => "S",
        *const arr.Int32Array => "i",
        *const arr.UInt32Array => "I",
        *const arr.Int64Array => "l",
        *const arr.UInt64Array => "L",
        *const arr.Float16Array => "e",
        *const arr.Float32Array => "f",
        *const arr.Float64Array => "g",
        *const arr.BoolArray => "b",
        else => @compileError("unexpected array type"),
    };

    const allocator = arena.allocator();
    const buffers = try allocator.alloc(?*const anyopaque, 2);
    buffers[0] = if (array.validity) |v| v.ptr else null;
    buffers[1] = array.values.ptr;

    return .{
        .array = .{
            .n_buffers = 2,
            .buffers = buffers.ptr,
            .offset = array.offset,
            .length = array.len,
            .null_count = array.null_count,
            .private_data = arena,
            .release = release_array,
        },
        .schema = .{
            .format = format,
            .private_data = arena,
            .release = release_schema,
        },
    };
}

test "roundtrip" {
    const original_data = [_]i32{ 3, 2, 1, 4, 5, 6 };

    const export_arena = std.testing.allocator.create(std.heap.ArenaAllocator) catch unreachable;
    export_arena.* = ArenaAllocator.init(std.testing.allocator);
    const export_alloc = export_arena.allocator();
    const typed = export_alloc.create(arr.Int32Array) catch unreachable;
    const values = export_alloc.alloc(i32, 6) catch unreachable;
    @memcpy(values, &original_data);

    typed.* = .{
        .len = 5,
        .offset = 1,
        .validity = null,
        .values = values,
        .null_count = 0,
    };

    const array = arr.Array.from(typed);

    // use 'catch unreachable' up to here beacuse we don't want everything to leak
    // and just using defer isn't feasible because all of that ownership is handed to the consumer
    // of FFI_Array
    //
    // would have to handle this in a more complete way in a real application.
    var ffi_array = export_(array, export_arena) catch unreachable;
    defer ffi_array.release();

    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const roundtrip_array = try import_(ffi_array, allocator);

    const roundtrip_typed = roundtrip_array.to(.i32);

    try std.testing.expectEqualDeep(
        original_data[1..],
        roundtrip_typed.values[roundtrip_typed.offset .. roundtrip_typed.offset + roundtrip_typed.len],
    );
}
