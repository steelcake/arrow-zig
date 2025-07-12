fuzz:
	zig build fuzzlib && clang -g -O1 -fsanitize=fuzzer fuzz.c -Lzig-out/lib -larrow_zig_fuzz  -o fuzzer && mkdir -p fuzz_corpus && mkdir -p fuzz_out && ./fuzzer -artifact_prefix=./fuzz_out/ -rss_limit_mb=8192 -fork=1 -ignore_crashes=1 fuzz_corpus
