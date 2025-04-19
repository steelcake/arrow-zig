const std = @import("std");
const Allocator = std.mem.Allocator;

const arr = @import("./array.zig");

pub const ArrowSchema = extern struct {
    format: [*:0]const u8,
    name: ?[*:0]const u8,
    metadata: ?[*:0]const u8,
    flags: packed struct(i64) {
        dictionary_ordered: bool,
        nullable: bool,
        map_keys_sorted: bool,
        _padding: u61,
    },
    n_children: i64,
    children: ?[*]*Schema,
    dictionary: ?*Schema,
    release: ?*const fn (*Schema) callconv(.C) void,
    private_data: ?*anyopaque,
};

pub const ArrowArray = extern struct {
    length: i64,
    null_count: i64,
    offset: i64,
    n_buffers: i64,
    n_children: i64,
    buffers: ?[*][*] const u8,
    children: ?[*]*Array,
    dictionary: ?*Array,
    release: ?*const fn (*Array) callconv(.C) void,
    private_data: ?*anyopaque,
};

pub const FFI_Array = struct {
    schema: ArrowSchema,
    array: ArrowArray,

    fn release(self: FFI_Array) void {
        self.schema.release(&self.schema);
        self.array.release(&self.array);
    }
};

pub fn release_imported(array: arr.Array, allocator: Allocator) void {
    switch(array.type_) {
        .null or .bool => {
            allocator.destroy(array.arr);
        },
        else => {},
    }
}

// fn import_buffer(buf: )
//
//

fn validity_len(len: i64) i64 {
    return (len + 7) / 8;
}

fn import_primitive(comptime T: type) !arr.PrimiveArray() {
    if (array.array.null_count == 0 or)

}

pub fn import_(array: FFI_Array, allocator: Allocator) !arr.Array {
    const format: []const u8 = std.mem.span(array.schema.format);
    if (format.len == 0) {
        return error.InvalidFFIArray;
    }

    const start = format[0];

    switch (start) {
        'n' => {
            const null_arr = try allocator.create(arr.NullArray);
            null_arr.* = arr.NullArray {
                .len = array.array.length,
                .offset = array.array.offset,
            };

            return arr.Array.from(null_arr);
        },
        'b' => {
            const buffers = array.array.buffers.?;
            if (array.array.n_buffers != 2) {
                return error.InvalidFFIArray;
            }

            const validity: ?[]const u8 = if (array.array.null_count == 0 or buffers[0] == null)
                null
            else 
                @ptrCast(@alignCast(buffers[0]))[0..validity_len(array.array.length)]
            ;

            const bool_arr = try allocator.create(arr.BoolArray);
            bool_arr.* = arr.BoolArray {
                .values = @ptrCast(@alignCast(buffers[1])),
                .validity = @ptrCast(@alignCast(buffers[0]))[0..validity_len(array.array.length)],
                .len = array.array.length,
                .offset = array.array.offset,
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
    const allocator = std.testing.allocator;

    const values = [_]i32 {3, 2, 1, 4, 5, 6};

    const typed = arr.Int32Array {
        .len = 5,
        .offset = 1,
        .validity = null,
        .values = &values,
    };

    const array = arr.Array.from(&typed);

    const ffi_array = export_(array, allocator);
    defer ffi_array.release();

    const roundtrip_array = try import_(ffi_array, allocator);
    defer release_imported(roundtrip_array, allocator);

    const roundtrip_typed = roundtrip_array.to(.i32);

    std.testing.expectEqualDeep(roundtrip_typed, typed);
}

