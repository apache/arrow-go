<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# arrgen

Zero-reflection Arrow encoders, generated from your Go structs.

`arrgen` is the code-generation counterpart to
[`arrow/array/arreflect`](../arrow/array/arreflect). `arreflect` reads a
struct's `arrow:"..."` tags with reflection on every value; `arrgen` reads the
same tags once, when you run it, and writes typed Go source that appends struct
fields straight into typed Arrow builders.

It lives in its own Go module: nothing in `github.com/apache/arrow-go/v18`
imports it, and the code it emits imports only `arrow`, `arrow/array` and
`arrow/memory` — never `arrgen` itself. Generating is a build-time step; your
binary does not grow a dependency for it.

## Quick start

Tag a struct as you would for `arreflect`, and add a `go:generate` line:

```go
package telemetry

import "time"

type Metric struct {
	Time   time.Time `arrow:"ts"`
	Host   string    `arrow:"host"`
	CPU    float64   `arrow:"cpu"`
	Value  *float64  `arrow:"value"` // nullable
	Secret string    `arrow:"-"`     // not a column
}

//go:generate go run github.com/apache/arrow-go/arrgen/cmd/arrgen -type Metric
```

```sh
go generate ./...   # writes metric_arrow.go next to the type
```

Check the result in, as you would `stringer` or `easyjson` output, and
regenerate when the struct changes. If you would rather not add `arrgen` to your
module's dependencies at all, pin it in the directive instead:

```go
//go:generate go run github.com/apache/arrow-go/arrgen/cmd/arrgen@v0.1.0 -type Metric
```

### What you get

```go
// One batch from a slice - the drop-in for arreflect.RecordFromSlice.
rec, err := telemetry.MetricRecordBatch(mem, metrics)

// Or stream rows in and cut batches where you want them.
a := telemetry.NewMetricAppender(mem)
defer a.Release()
a.Reserve(batchSize)
for row := range rows {
	a.Append(&row)
	if a.Len() == batchSize {
		rec := a.NewRecordBatch()
		... // hand it off
		rec.Release()
		a.Reserve(batchSize)
	}
}

telemetry.MetricSchema()   // the schema, built once at init
a.Err()                    // first append error; only dictionary columns can fail
```

`Append` reads `v` synchronously and never retains it, so a caller draining a
stream can reuse one row variable for every row.

## Flags

| Flag | Meaning |
| --- | --- |
| `-type` | Struct type name. Repeatable, or comma-separated. Required. |
| `-output` | Output file, relative to `-dir`. Defaults to `<first type>_arrow.go`. |
| `-dir` | Package directory. Defaults to `.`, which is where `go:generate` runs. |
| `-header` | File whose contents are copied above the generated-code marker, for a license header. |

## Supported fields

`bool`, `int8/16/32/64`, `int`, `uint8/16/32/64`, `uint`, `float32/64`,
`string`, `[]byte`, `time.Time`, `time.Duration`, `decimal.Decimal32`,
`decimal.Decimal64`, `decimal128.Num`, `decimal256.Num`, and a pointer to any of
those for a nullable column.

Tag options are `arreflect`'s: a leading column name, `-` to skip the field, the
temporal overrides `date32`, `date64`, `time32`, `time64` and `timestamp`,
`dict`, `view`, `large`, and `decimal(precision,scale)`.

Anything else — nested structs, slices other than `[]byte`, arrays, maps,
embedded fields, defined scalar types such as `type ID int64` — is a
**generate-time error naming the field**, never a silently dropped column. Those
structs still encode fine with `arreflect` at runtime.

The generator is equally strict about options that would do nothing: `date32` on
an `int64` field is an error here even though `arreflect` ignores it.

## Equivalence with arreflect

The point of matching `arreflect`'s tag dialect is that switching between the
two paths is a call-site change and nothing else. `internal/gentypes` holds a
fixture with one field for every supported column shape and asserts, on every
run, that the generated encoder and `arreflect` produce the same schema and the
same column data for the same input.

### Two columns where they differ, and why

`arreflect`'s struct-field path dispatches on `reflect.Kind` before it checks
the types it special-cases, so a Go struct that Arrow models as a scalar reaches
its nested-struct branch, which finds only unexported fields and returns an
empty `struct<>`. The value is then dropped. This affects:

| Field | `arreflect` infers | `arrgen` emits |
| --- | --- | --- |
| `time.Time` (untagged, or `,timestamp`) | `struct<>` | `timestamp[ns, tz=UTC]` |
| `decimal128.Num` / `decimal256.Num` (untagged) | `struct<>` | `decimal128` / `decimal256` |

A tag that names an explicit Arrow type — `,date32`, `,decimal(20,3)` — rescues
the field, because `arreflect` overrides the inferred type from the tag
afterwards. `arreflect`'s own tests only pass these types as the top-level
element of a slice, where a different branch handles them correctly.

`arrgen` emits what `arreflect.InferType[time.Time]` and its top-level
`FromSlice` path both agree on. `TestArreflectStructFieldGaps` pins the current
upstream behavior, so if `arrow/array/arreflect` is fixed, that test fails and
tells us to delete this section.

## Performance

`go test ./internal/gentypes/ -bench . -benchmem`, Go 1.25, linux/arm64. Batch
benchmarks encode 1024 rows per operation.

| Benchmark | arreflect | arrgen | |
| --- | --- | --- | --- |
| `MetricBatch` (4 columns) | 56.9 µs, 98 allocs | 20.3 µs, 52 allocs | **2.8x faster** |
| `FixedBatch` (5 fixed-width columns) | 59.0 µs, 97 allocs | 17.4 µs, 46 allocs | **3.4x faster** |
| `RowBatch` (46 columns) | 843 µs, 3430 allocs | 413 µs, 3126 allocs | **2.0x faster** |
| `StreamAppend` (per row) | 58.3 ns | 18.6 ns, **0 allocs** | **3.1x faster** |
| `Schema` | 5.6 µs, 132 allocs | 1.5 ns, **0 allocs** | |

Three things move the numbers:

- **Field access.** The generated code loads `v.CPU` and calls
  `Float64Builder.Append`. The reflection path resolves a field index, produces
  a `reflect.Value`, and dispatches on the builder's dynamic type, per value.
- **Builder lookup.** Typed builders are resolved once, in the constructor, so
  `Append` does no type assertions at all — not even the
  `b.Field(i).(*array.Float64Builder)` a hand-written encoder usually repeats.
- **Schema construction.** The schema is a package-level variable rather than a
  struct walk per call.

Allocation counts converge on wide fixtures because both paths pay for the same
Arrow buffers, and because Arrow's dictionary builders allocate per appended
value regardless of who calls them. The place the difference is unambiguous is
the streaming path: with room reserved, `Append` allocates nothing, which
`TestStreamingAppendIsAllocationFree` asserts.

The streaming path also removes a constraint: `arreflect` cannot encode anything
until the caller has materialized the whole `[]T`, while an appender can be fed
a row at a time and cut into batches wherever you like.

## Working on arrgen

The repository root carries a `go.work` so this module builds against the
arrow-go tree next to it rather than the released version its `go.mod` names:

```sh
go test ./arrgen/...                    # workspace: local arrow-go
cd arrgen && GOWORK=off go test ./...   # standalone: released arrow-go
```

Both are expected to pass. `TestCheckedInFilesAreUpToDate` regenerates every
committed output in this module and fails if it differs, so a struct change
without a `go generate` is caught at the point of the edit.

The golden file in `testdata` is the review surface for generator changes:
`go test ./arrgen/ -update` rewrites it, and the resulting diff is exactly what
users would see in their own regenerated code.

### Releasing

As a nested module, `arrgen` is versioned and tagged independently of the root
module: its tags are `arrgen/vX.Y.Z`, not `vX.Y.Z`. It cannot share the root's
`v18` line, because a module path without a `/vN` suffix is limited to v0 and
v1. Nothing in the release scripts tags it yet.
