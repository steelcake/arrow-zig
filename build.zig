const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const cargo_build_args: []const []const u8 = switch (optimize) {
        .Debug => &.{ "cargo", "build" },
        else => &.{ "cargo", "build", "--release" },
    };
    const cargo_build = b.addSystemCommand(cargo_build_args);
    cargo_build.setName("Build Rust Library(cargo)");
    cargo_build.setCwd(b.path("ffi_tester"));

    const lib_mod = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const lib_unit_tests = b.addTest(.{
        .root_module = lib_mod,
    });
    lib_unit_tests.step.dependOn(&cargo_build.step);
    lib_unit_tests.linkLibC();
    lib_unit_tests.linkSystemLibrary("unwind");

    const object_file_path = switch (optimize) {
        .Debug => "ffi_tester/target/debug/libffi_tester.a",
        else => "ffi_tester/target/release/libffi_tester.a",
    };
    lib_unit_tests.addObjectFile(b.path(object_file_path));

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);
}
