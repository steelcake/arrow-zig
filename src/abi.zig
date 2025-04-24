pub const Flags = packed struct(i64) {
    dictionary_ordered: bool = false,
    nullable: bool = false,
    map_keys_sorted: bool = false,
    _padding: u61 = 0,
};

pub const ArrowSchema = extern struct {
    format: [*:0]const u8,
    name: ?[*:0]const u8,
    metadata: ?[*:0]const u8,
    flags: Flags,
    n_children: i64,
    children: ?[*]?*ArrowSchema,
    dictionary: ?*ArrowSchema,
    release: ?*const fn (?*ArrowSchema) callconv(.C) void,
    private_data: ?*anyopaque,
};

pub const ArrowArray = extern struct {
    length: i64,
    null_count: i64,
    offset: i64,
    n_buffers: i64,
    n_children: i64,
    buffers: ?[*]?*const anyopaque,
    children: ?[*]?*ArrowArray,
    dictionary: ?*ArrowArray,
    release: ?*const fn (?*ArrowArray) callconv(.C) void,
    private_data: ?*anyopaque,
};
