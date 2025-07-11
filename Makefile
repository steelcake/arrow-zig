fuzz:
	zig build fuzzlib && clang -g -O1 -fsanitize=fuzzer fuzz.c -Lzig-out/lib -larrow_zig_fuzz  -o fuzzer && ./fuzzer
