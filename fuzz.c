#import <stddef.h>
#import <stdint.h>

void arrow_zig_run_fuzz_test(const uint8_t* data, size_t size);

extern int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size) {
  arrow_zig_run_fuzz_test(Data, Size);
  return 0;  // Values other than 0 and -1 are reserved for future use.
}

