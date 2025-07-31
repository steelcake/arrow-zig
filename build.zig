const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lib_mod = b.addModule("arrow", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const lib_unit_tests = b.addTest(.{
        .root_module = lib_mod,
    });
    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);

    // Have to have single fuzz entrypoint until this is solved: https://github.com/ziglang/zig/issues/23738
    // So create one target per fuzz target using this function and run the commands seperately.
    add_fuzz_target(b, "fuzz_minmax", "src/fuzz_minmax.zig", "run fuzz tests for minmax", target, optimize);
    add_fuzz_target(b, "fuzz_ffi", "src/fuzz_ffi.zig", "run fuzz tests for ffi", target, optimize);
    add_fuzz_target(b, "fuzz_concat", "src/fuzz_concat.zig", "run fuzz tests for concat", target, optimize);
    add_fuzz_target(b, "fuzz_check_dt", "src/fuzz_check_dt.zig", "run fuzz tests for check_data_type", target, optimize);
}

fn add_fuzz_target(b: *std.Build, command_name: []const u8, root_source_file: []const u8, description: []const u8, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) void {
    const fuzz = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(root_source_file),
            .target = target,
            .optimize = optimize,
        }),
        // Required for running fuzz tests
        // https://github.com/ziglang/zig/issues/23423
        .use_llvm = true,
    });

    const run_fuzz = b.addRunArtifact(fuzz);

    const fuzz_step = b.step(command_name, description);
    fuzz_step.dependOn(&run_fuzz.step);
}
