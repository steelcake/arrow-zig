const arr = @import("./array.zig");

pub const Scalar = union(enum) {
    null,
    i8: i8,
    i16: i16,
    i32: i32,
    i64: i64,
    i128: i128,
    i256: i256,
    u8: u8,
    u16: u16,
    u32: u32,
    u64: u64,
    f16: f16,
    f32: f32,
    f64: f64,
    binary: []const u8,
    bool: bool,
    list: *const arr.Array,
};

pub const Datum = union(enum) {
    scalar: Scalar,
    arr: *const arr.Array,
};
