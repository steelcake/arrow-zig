const concat_fuzz = @import("./concat.zig").run_fuzz_test;
const ffi_fuzz = @import("./ffi_test.zig").run_fuzz_test;

pub export fn arrow_zig_run_fuzz_test(data: [*]const u8, size: usize) void {
    const input = data[0..size];
    concat_fuzz(input) catch unreachable;
    ffi_fuzz(input) catch unreachable;
}
