const arr = @import("./array.zig");

pub const Direction = enum {
    ascending,
    descending,
};

pub fn is_sorted(comptime direction: Direction, array: *const arr.Array) bool {
    switch (array.*) {}
}
