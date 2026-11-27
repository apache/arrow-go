// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build cgo
// +build test

#include <assert.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include "abi.h"
#include "helpers.h"
#include "utils.h"

int is_little_endian()
{
  unsigned int x = 1;
  char *c = (char*) &x;
  return (int)*c;
}

static const int64_t kDefaultFlags = ARROW_FLAG_NULLABLE;

extern void releaseTestArr(struct ArrowArray* array);
void goReleaseTestArray(struct ArrowArray* array) {
  releaseTestArr(array);
}

static void release_int32_type(struct ArrowSchema* schema) {
    // mark released
    schema->release = NULL;
}

void export_int32_type(struct ArrowSchema* schema) {
    const char* encoded_metadata;
    if (is_little_endian() == 1) {
        encoded_metadata = kEncodedMeta1LE;
    } else {
        encoded_metadata = kEncodedMeta1BE;
    }
    *schema = (struct ArrowSchema) {
        // Type description
        .format = "i",
        .name = "",
        .metadata = encoded_metadata,
        .flags = 0,
        .n_children = 0,
        .children = NULL,
        .dictionary = NULL,
        // bookkeeping
        .release = &release_int32_type,
    };
}

static bool test1_released = false;

int test1_is_released() { return test1_released; }

static void release_int32_array(struct ArrowArray* array) {
    assert(array->n_buffers == 2);
    // free the buffers and buffers array
    free((void *) array->buffers[1]);
    free(array->buffers);
    // mark released
    array->release = NULL;
    test1_released = true;
}

void export_int32_array(const int32_t* data, int64_t nitems, struct ArrowArray* array) {
    // initialize primitive fields
    *array = (struct ArrowArray) {
        .length = nitems,
        .offset = 0,
        .null_count = 0,
        .n_buffers = 2,
        .n_children = 0,
        .children = NULL,
        .dictionary = NULL,
        // bookkeeping
        .release = &release_int32_array
    };

    // allocate list of buffers
    array->buffers = (const void**)malloc(sizeof(void*) * array->n_buffers);
    assert(array->buffers != NULL);
    array->buffers[0] = NULL; // no nulls, null bitmap can be omitted
    array->buffers[1] = data;
}


static void release_primitive(struct ArrowSchema* schema) {
    free((void *)schema->format);
    schema->release = NULL;
}

static void release_nested_internal(struct ArrowSchema* schema,
                                    int is_dynamic) {
    assert(!ArrowSchemaIsReleased(schema));
    for (int i = 0; i < schema->n_children; ++i) {
        ArrowSchemaRelease(schema->children[i]);
        free(schema->children[i]);
    }
    if (is_dynamic) {
        free((void*)schema->format);
        free((void*)schema->name);
    } else {
        free(schema->children);
    }
    ArrowSchemaMarkReleased(schema);
}

static void release_nested_static(struct ArrowSchema* schema) {
    release_nested_internal(schema, /*is_dynamic=*/0);
}

static void release_nested_dynamic(struct ArrowSchema* schema) {
    release_nested_internal(schema, /*is_dynamic=*/1);
}

static void release_nested_dynamic_toplevel(struct ArrowSchema* schema) {
    assert(!ArrowSchemaIsReleased(schema));
    for (int i = 0; i < schema->n_children; ++i) {
        ArrowSchemaRelease(schema->children[i]);
        free(schema->children[i]);
    }
    free((void*)schema->format);
    if (strlen(schema->name) > 0) {
        free((void*)schema->name);
    }
    ArrowSchemaMarkReleased(schema);
}

void test_primitive(struct ArrowSchema* schema, const char* fmt) {
    *schema = (struct ArrowSchema) {
        // Type description
        .format = fmt,
        .name = "",
        .metadata = NULL,
        .flags = 0,
        .n_children = 0,
        .children = NULL,
        .dictionary = NULL,
        // bookkeeping
        .release = &release_primitive,
    };
}

// Since test_lists et al. allocate an entirely array of ArrowSchema pointers,
// need to expose a function to free it.
void free_malloced_schemas(struct ArrowSchema** schemas) {
    free(schemas[0]);
    free(schemas);
}

struct ArrowSchema** test_lists(const char** fmts, const char** names, const int* nullflags, const int n) {
    struct ArrowSchema** schemas = malloc(sizeof(struct ArrowSchema*)*n);
    for (int i = 0; i < n; ++i) {
        schemas[i] = malloc(sizeof(struct ArrowSchema));
        *schemas[i] = (struct ArrowSchema) {
            .format = fmts[i],
            .name = names[i],
            .metadata = NULL,
            .flags = 0,
            .children = NULL,
            .n_children = 0,
            .dictionary = NULL,
            .release = &release_nested_dynamic,
        };
        if (i != 0) {
            schemas[i-1]->n_children = 1;
            schemas[i-1]->children = &schemas[i];
            schemas[i]->flags = nullflags[i-1];
        }
    }
    return schemas;
}

struct ArrowSchema** fill_structs(const char** fmts, const char** names, int64_t* flags, const int n) {
    struct ArrowSchema** schemas = malloc(sizeof(struct ArrowSchema*)*n);
    for (int i = 0; i < n; ++i) {
        schemas[i] = malloc(sizeof(struct ArrowSchema));
        *schemas[i] = (struct ArrowSchema) {
            .format = fmts[i],
            .name = names[i],
            .metadata = NULL,
            .flags = flags[i],
            .children = NULL,
            .n_children = 0,
            .dictionary = NULL,
            .release = &release_nested_dynamic,
        };
    }

    schemas[0]->children = &schemas[1];
    schemas[0]->n_children = n-1;
    return schemas;
}

struct ArrowSchema** test_struct(const char** fmts, const char** names, int64_t* flags, const int n) {
    struct ArrowSchema** schemas = fill_structs(fmts, names, flags, n);

    if (is_little_endian() == 1) {
        schemas[n-1]->metadata = kEncodedMeta2LE;
    } else {
        schemas[n-1]->metadata = kEncodedMeta2BE;
    }

    return schemas;
}

struct ArrowSchema** test_schema(const char** fmts, const char** names, int64_t* flags, const int n) {
    struct ArrowSchema** schemas = fill_structs(fmts, names, flags, n);

    if (is_little_endian() == 1) {
        schemas[0]->metadata = kEncodedMeta2LE;
        schemas[n-1]->metadata = kEncodedMeta1LE;
    } else {
        schemas[0]->metadata = kEncodedMeta2BE;
        schemas[n-1]->metadata = kEncodedMeta1BE;
    }
    return schemas;
}

struct ArrowSchema** test_map(const char** fmts, const char** names, int64_t* flags, const int n) {
    struct ArrowSchema** schemas = malloc(sizeof(struct ArrowSchema*)*n);
    for (int i = 0; i < n; ++i) {
        schemas[i] = malloc(sizeof(struct ArrowSchema));
        *schemas[i] = (struct ArrowSchema) {
            .format = fmts[i],
            .name = names[i],
            .metadata = NULL,
            .flags = flags[i],
            .children = NULL,
            .n_children = 0,
            .dictionary = NULL,
            .release = &release_nested_dynamic,
        };
    }

    schemas[0]->n_children = 1;
    schemas[0]->children = &schemas[1];
    schemas[1]->n_children = n-2;
    schemas[1]->children = &schemas[2];

    return schemas;
}

struct ArrowSchema** test_union(const char** fmts, const char** names, int64_t* flags, const int n) {
    struct ArrowSchema** schemas = malloc(sizeof(struct ArrowSchema*)*n);
     for (int i = 0; i < n; ++i) {
        schemas[i] = malloc(sizeof(struct ArrowSchema));
        *schemas[i] = (struct ArrowSchema) {
            .format = fmts[i],
            .name = names[i],
            .metadata = NULL,
            .flags = flags[i],
            .children = NULL,
            .n_children = 0,
            .dictionary = NULL,
            .release = &release_nested_dynamic,
        };
    }

    schemas[0]->n_children = n-1;
    schemas[0]->children = &schemas[1];
    return schemas;
}

struct streamcounter {
    int n;
    int max;
};

static int stream_schema(struct ArrowArrayStream* st, struct ArrowSchema* out) {
    out->children = malloc(sizeof(struct ArrowSchema*)*2);
    out->n_children = 2;

    out->children[0] = malloc(sizeof(struct ArrowSchema));
    *out->children[0] = (struct ArrowSchema) {
        .format = "i",
        .name = "a",
        .metadata = NULL,
        .flags = ARROW_FLAG_NULLABLE,
        .children = NULL,
        .n_children = 0,
        .dictionary = NULL,
        .release = &release_nested_static,
    };

    out->children[1] = malloc(sizeof(struct ArrowSchema));
    *out->children[1] = (struct ArrowSchema) {
        .format = "u",
        .name = "b",
        .metadata = NULL,
        .flags = ARROW_FLAG_NULLABLE,
        .children = NULL,
        .n_children = 0,
        .dictionary = NULL,
        .release = &release_nested_static,
    };

    out->format = "+s";
    out->release = &release_nested_static;

    return 0;
}

static void release_stream(struct ArrowArrayStream* st) {
    free(st->private_data);
    ArrowArrayStreamMarkReleased(st);
}

static void release_the_array(struct ArrowArray* out) {
    for (int i = 0; i < out->n_children; ++i) {
        ArrowArrayRelease(out->children[i]);
        free(out->children[i]);
    }
    free((void*)out->children);
    free(out->buffers);
    out->release = NULL;
}

void export_int32_array(const int32_t*, int64_t, struct ArrowArray*);

static void release_str_array(struct ArrowArray* array) {
    assert(array->n_buffers == 3);
    if (array->buffers[0] != NULL) {
        free((void*) array->buffers[0]);
    }
    free((void*) array->buffers[1]);
    if (array->buffers[2] != NULL && array->buffers[2] != (void*)0x1) {
        free((void*) array->buffers[2]);
    }
    free(array->buffers);
    array->release = NULL;
}

void export_str_array(const char* data, const int32_t* offsets, int64_t nitems, struct ArrowArray* out) {
    *out = (struct ArrowArray) {
        .length = nitems,
        .offset = 0,
        .null_count = 0,
        .n_buffers = 3,
        .n_children = 0,
        .children = NULL,
        .dictionary = NULL,
        // bookkeeping
        .release = &release_str_array
    };

    out->buffers = (const void**)malloc(sizeof(void*) * out->n_buffers);
    assert(out->buffers != NULL);
    out->buffers[0] = NULL;
    out->buffers[1] = offsets;
    out->buffers[2] = data;
}

void export_str_array_with_nulls(int64_t nitems, struct ArrowArray* out) {
    *out = (struct ArrowArray) {
        .length = nitems,
        .offset = 0,
        .null_count = nitems,
        .n_buffers = 3,
        .n_children = 0,
        .children = NULL,
        .dictionary = NULL,
        // bookkeeping
        .release = &release_str_array
    };

    out->buffers = (const void**)malloc(sizeof(void*) * out->n_buffers);
    assert(out->buffers != NULL);
    int64_t bitmap_nbytes = (nitems + 7) / 8;
    out->buffers[0] = malloc(bitmap_nbytes);
    memset((void*)out->buffers[0], 0, bitmap_nbytes);
    out->buffers[1] = malloc((nitems + 1) * sizeof(int32_t));
    memset((void*)out->buffers[1], 0, (nitems + 1) * sizeof(int32_t));
    out->buffers[2] = (void*)0x1;
}

static int next_record(struct ArrowArrayStream* st, struct ArrowArray* out) {
    struct streamcounter* cnter = (struct streamcounter*)(st->private_data);
    if (cnter->n == cnter->max) {
        ArrowArrayMarkReleased(out);
        return 0;
    }

    cnter->n++;

    *out = (struct ArrowArray) {
        .offset = 0,
        .dictionary = NULL,
        .length = 3,
        .null_count = 0,
        .buffers = (const void**)malloc(sizeof(void*)),
        .n_children = 2,
        .n_buffers = 1,
        .release = &release_the_array
    };

    out->buffers[0] = NULL;
    out->children = (struct ArrowArray**)malloc(sizeof(struct ArrowArray*)*2);
    int32_t* intdata = malloc(sizeof(int32_t)*3);
    for (int i = 0; i < 3; ++i) {
        intdata[i] = cnter->n * (i+1);
    }

    out->children[0] = malloc(sizeof(struct ArrowArray));
    export_int32_array(intdata, 3, out->children[0]);
    out->children[1] = malloc(sizeof(struct ArrowArray));
    char* strdata = strdup("foobarbaz");
    int32_t* offsets = malloc(sizeof(int32_t)*4);
    offsets[0] = 0;
    offsets[1] = 3;
    offsets[2] = 6;
    offsets[3] = 9;
    export_str_array(strdata, offsets, 3, out->children[1]);

    return 0;
}

void setup_array_stream_test(const int n_batches, struct ArrowArrayStream* out) {
    struct streamcounter* cnt = malloc(sizeof(struct streamcounter));
    cnt->max = n_batches;
    cnt->n = 0;

    out->get_next = &next_record;
    out->get_schema = &stream_schema;
    out->release = &release_stream;
    out->private_data = cnt;
}

int test_exported_stream(struct ArrowArrayStream* stream) {
  while (1) {
    struct ArrowArray array;
    memset(&array, 0, sizeof(array));
    // Garbage - implementation should not try to call it, though!
    array.release = (void*)0xDEADBEEF;
    int rc = stream->get_next(stream, &array);
    if (rc != 0) return rc;

    if (array.release == NULL) {
      stream->release(stream);
      break;
    }
    array.release(&array);
  }
  return 0;
}

struct FallibleStream {
  // empty structs are a GNU extension
  int dummy;
};

const char* FallibleGetLastError(struct ArrowArrayStream* stream) {
  return "Expected error message";
}

int FallibleGetSchema(struct ArrowArrayStream* stream, struct ArrowSchema* schema) {
  return EINVAL;
}

int FallibleGetNext(struct ArrowArrayStream* stream, struct ArrowArray* array) {
  return EINVAL;
}

void FallibleRelease(struct ArrowArrayStream* stream) {
  memset(stream, 0, sizeof(*stream));
}

static struct FallibleStream kFallibleStream;

void test_stream_schema_fallible(struct ArrowArrayStream* stream) {
  stream->get_last_error = FallibleGetLastError;
  stream->get_schema = FallibleGetSchema;
  stream->get_next = FallibleGetNext;
  stream->private_data = &kFallibleStream;
  stream->release = FallibleRelease;
}

int confuse_go_gc(struct ArrowArrayStream* stream, unsigned int seed) {
  struct ArrowSchema schema;
  // Try to confuse the Go GC by putting what looks like a Go pointer here.
#ifdef _WIN32
  // Thread-safe on Windows with the multithread CRT
#define DORAND rand()
#else
#define DORAND rand_r(&seed)
#endif
  schema.name = (char*)(0xc000000000L + (DORAND % 0x2000));
  schema.format = (char*)(0xc000000000L + (DORAND % 0x2000));
  int rc = stream->get_schema(stream, &schema);
  if (rc != 0) return rc;
  schema.release(&schema);

  while (1) {
    struct ArrowArray array;
    array.release = (void*)(0xc000000000L + (DORAND % 0x2000));
    array.private_data = (void*)(0xc000000000L + (DORAND % 0x2000));
    int rc = stream->get_next(stream, &array);
    if (rc != 0) return rc;

    if (array.release == NULL) {
      stream->release(stream);
      break;
    }
    array.release(&array);
  }
  return 0;
#undef DORAND
}

typedef struct {
  void (*release)(struct ArrowArray*);
  void* private_data;
  int count;
} CountingReleaser;
static void counting_release(struct ArrowArray* arr) {
  CountingReleaser* cr = arr->private_data;
  cr->count++;
  arr->private_data = cr->private_data;
  arr->release = cr->release;
  arr->release(arr);
}
void* wrap_counting_release(struct ArrowArray* arr) {
  CountingReleaser* cr = calloc(1, sizeof(*cr));
  cr->release = arr->release;
  cr->private_data = arr->private_data;
  arr->release = counting_release;
  arr->private_data = cr;
  return cr;
}
int get_counting_release_count(void* cr) {
  return ((CountingReleaser*)cr)->count;
}
typedef struct {
  struct ArrowArray arr;
  struct ArrowSchema schema;
  int releases;
} SingleArrayStream;
static int single_get_schema(struct ArrowArrayStream* stream, struct ArrowSchema* out) {
  ArrowSchemaMove(&((SingleArrayStream*)stream->private_data)->schema, out);
  return 0;
}
static int single_get_next(struct ArrowArrayStream* stream, struct ArrowArray* out) {
  ArrowArrayMove(&((SingleArrayStream*)stream->private_data)->arr, out);
  return 0;
}
static void single_release(struct ArrowArrayStream* stream) {
  SingleArrayStream* state = stream->private_data;
  state->releases++;
  ArrowArrayRelease(&state->arr);
  ArrowSchemaRelease(&state->schema);
  ArrowArrayStreamMarkReleased(stream);
}
void* setup_single_array_stream(struct ArrowArray* arr, struct ArrowSchema* schema,
                                struct ArrowArrayStream* stream) {
  SingleArrayStream* state = calloc(1, sizeof(*state));
  ArrowArrayMove(arr, &state->arr);
  ArrowSchemaMove(schema, &state->schema);
  stream->get_schema = single_get_schema;
  stream->get_next = single_get_next;
  stream->release = single_release;
  stream->private_data = state;
  return state;
}
int get_single_array_stream_release_count(void* state) {
  return ((SingleArrayStream*)state)->releases;
}

// CountingStream wraps a stream and counts its release callback, the batches
// it hands out and the release callbacks of those batches. The state is freed
// by the test, so batches can still be released after the stream.
typedef struct {
  struct ArrowArrayStream inner;
  int fail_schema;
  int fail_next_at;
  int stream_releases;
  int batches_out;
  int batch_releases;
} CountingStream;
typedef struct {
  CountingStream* owner;
  void (*release)(struct ArrowArray*);
  void* private_data;
} CountingBatch;
static void counting_batch_release(struct ArrowArray* arr) {
  CountingBatch* cb = arr->private_data;
  cb->owner->batch_releases++;
  arr->release = cb->release;
  arr->private_data = cb->private_data;
  free(cb);
  arr->release(arr);
}
static int counting_get_schema(struct ArrowArrayStream* stream, struct ArrowSchema* out) {
  CountingStream* state = stream->private_data;
  if (state->fail_schema) return EIO;
  return state->inner.get_schema(&state->inner, out);
}
static int counting_get_next(struct ArrowArrayStream* stream, struct ArrowArray* out) {
  CountingStream* state = stream->private_data;
  if (state->batches_out == state->fail_next_at) return EIO;
  int rc = state->inner.get_next(&state->inner, out);
  if (rc != 0 || out->release == NULL) return rc;
  CountingBatch* cb = malloc(sizeof(*cb));
  cb->owner = state;
  cb->release = out->release;
  cb->private_data = out->private_data;
  out->release = counting_batch_release;
  out->private_data = cb;
  state->batches_out++;
  return 0;
}
static const char* counting_get_last_error(struct ArrowArrayStream* stream) {
  return "injected error";
}
static void counting_stream_release(struct ArrowArrayStream* stream) {
  CountingStream* state = stream->private_data;
  state->stream_releases++;
  ArrowArrayStreamRelease(&state->inner);
  ArrowArrayStreamMarkReleased(stream);
}
void* setup_counting_stream(struct ArrowArrayStream* inner, int fail_schema, int fail_next_at,
                            struct ArrowArrayStream* out) {
  CountingStream* state = calloc(1, sizeof(*state));
  ArrowArrayStreamMove(inner, &state->inner);
  state->fail_schema = fail_schema;
  state->fail_next_at = fail_next_at;
  out->get_schema = counting_get_schema;
  out->get_next = counting_get_next;
  out->get_last_error = counting_get_last_error;
  out->release = counting_stream_release;
  out->private_data = state;
  return state;
}
void get_counting_stream_counts(void* state, int* stream_releases, int* batches_out,
                                int* batch_releases) {
  CountingStream* s = state;
  *stream_releases = s->stream_releases;
  *batches_out = s->batches_out;
  *batch_releases = s->batch_releases;
}
