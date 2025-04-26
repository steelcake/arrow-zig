const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const nanoarrow = b.dependency("nanoarrow", .{
        .target = target,
        .optimize = optimize,
    });

    const nanoarrow_test_helper = b.addLibrary(.{
        .name = "nanoarrow_test_helper",
        .root_module = b.addModule("nanoarrow_test_helper_mod", .{
            .target = target,
            .optimize = optimize,
        }),
    });
    nanoarrow_test_helper.linkLibrary(nanoarrow.artifact("nanoarrow"));
    nanoarrow_test_helper.addCSourceFile(.{
        .file = b.path("nanoarrow_test_helper/root.c"),
    });

    const lib_mod = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const lib_unit_tests = b.addTest(.{
        .root_module = lib_mod,
    });
    lib_unit_tests.linkLibrary(nanoarrow_test_helper);

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    // Similar to creating the run step earlier, this exposes a `test` step to
    // the `zig build --help` menu, providing a way for the user to request
    // running the unit tests.
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);
}
