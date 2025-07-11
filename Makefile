fuzz:
	zig build fuzzlib && clang -g -O1 -fsanitize=fuzzer fuzz.c -Lzig-out/lib -larrow_zig_fuzz  -o fuzzer && mkdir -p fuzz_corpus && mkdir -p fuzz_out && ./fuzzer fuzz_corpus -artifact_prefix=./fuzz_out/
