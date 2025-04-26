#include <nanoarrow/nanoarrow.h>
#include <stdio.h>

int test_helper_roundtrip_array(struct ArrowArray* input_array,
                        struct ArrowSchema* input_schema,
                        struct ArrowArray* output_array,
                        struct ArrowSchema* output_schema) {
    int result;

    struct ArrowSchemaView schema_view;
    struct ArrowError err;
    result = ArrowSchemaViewInit(&schema_view, input_schema, &err);
    if (result != NANOARROW_OK) {
        fprintf(stderr, "Failed to initialize schema view: %s\n", err.message);
        return result;
    }

    struct ArrowArrayView array_view;
    result = ArrowArrayViewInitFromSchema(&array_view, input_schema, &err);
    if (result != NANOARROW_OK) {
        fprintf(stderr, "Failed to initialize array view: %s\n", err.message);
        return result;
    }

    result = ArrowArrayViewSetArray(&array_view, input_array, &err);
    if (result != NANOARROW_OK) {
        fprintf(stderr, "Failed to set array: %s\n", err.message);
        ArrowArrayViewReset(&array_view);
        return result;
    }

    result = ArrowArrayViewValidate(&array_view, NANOARROW_VALIDATION_LEVEL_FULL, &err);
    if (result != NANOARROW_OK) {
        fprintf(stderr, "Validation failed: %s\n", err.message);
        ArrowArrayViewReset(&array_view);
        return result;
    }

    ArrowArrayMove(input_array, output_array);
    ArrowSchemaMove(input_schema, output_schema);

    return NANOARROW_OK;
}
