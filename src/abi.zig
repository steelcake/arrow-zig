pub const Flags = packed struct(i64) {
    dictionary_ordered: bool = false,
    nullable: bool = false,
    map_keys_sorted: bool = false,
    _padding: u61 = 0,
};

pub const ArrowSchema = extern struct {
    format: [*:0]const u8,
    name: ?[*:0]const u8 = null,
    metadata: ?[*:0]const u8 = null,
    flags: Flags = .{},
    n_children: i64 = 0,
    children: ?[*]?*ArrowSchema = null,
    dictionary: ?*ArrowSchema = null,
    release: ?*const fn (*ArrowSchema) callconv(.C) void = null,
    private_data: ?*anyopaque = null,
};

pub const ArrowArray = extern struct {
    length: i64 = 0,
    null_count: i64 = 0,
    offset: i64 = 0,
    n_buffers: i64 = 0,
    n_children: i64 = 0,
    buffers: ?[*]?*const anyopaque = null,
    children: ?[*]?*ArrowArray = null,
    dictionary: ?*ArrowArray = null,
    release: ?*const fn (*ArrowArray) callconv(.C) void = null,
    private_data: ?*anyopaque = null,
};
