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

It lives in its own Go module, so it is opt-in twice over: nothing in
`github.com/apache/arrow-go/v18` imports it, and the code it emits imports only
`arrow`, `arrow/array` and `arrow/memory`, never `arrgen` itself. Generating is
a build-time step; your binary does not grow a dependency for it.

## Quick start

Add `arrgen` to your module as a tool dependency, which needs `go 1.24` or
later in your `go.mod`:

```sh
go get -tool github.com/apache/arrow-go/arrgen/cmd/arrgen
```

Then tag a struct as you would for `arreflect` and add a `go:generate` line:

```go
package telemetry

import "time"

type Metric struct {
	Day    time.Time `arrow:"day,date32"`
	Host   string    `arrow:"host"`
	CPU    float64   `arrow:"cpu"`
	Value  *float64  `arrow:"value"` // nullable
	Secret string    `arrow:"-"`     // not a column
}

//go:generate go tool arrgen -type Metric
```

```sh
go generate ./...   # writes metric_arrow.go next to the type
```

Check the result in, as you would `stringer` or `easyjson` output, and
regenerate when the struct changes.

`go get -tool` records the generator in your `go.mod` and `go.sum` but not in
your build: nothing in your packages imports it, so it is not linked into your
binary. If you would rather not record it at all, name a version in the
directive instead, which resolves the generator per run without touching your
module files:

```go
//go:generate go run github.com/apache/arrow-go/arrgen/cmd/arrgen@arrgen/v0.1.0 -type Metric
```

Both forms need `arrgen` to be resolvable. As a nested module it is versioned
under its own `arrgen/vX.Y.Z` tags. Until the first of those is published,
neither form resolves from a released version, so point at a checkout instead:

```sh
go mod edit -replace github.com/apache/arrow-go/arrgen=../arrow-go/arrgen
```

The unversioned `go run github.com/apache/arrow-go/arrgen/cmd/arrgen` spelling
works only inside a module that already requires `arrgen`; anywhere else the go
tool refuses it with `no required module provides package`. The directives in
this module use that spelling because this module is `arrgen`.

### What you get

```go
// One batch from a slice. The drop-in for arreflect.RecordFromSlice.
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

Runnable versions of all three are the testable examples in
[`example_test.go`](example_test.go).

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
`decimal.Decimal64`, `decimal128.Num`, `decimal256.Num`, and one or more
pointers to any of those for a nullable column. A nil at any level is a null,
as it is in `arreflect`.

Tag options are `arreflect`'s: a leading column name, `-` to skip the field, the
temporal overrides `date32`, `date64`, `time32` and `time64`, `dict`, `view`,
`large`, and `decimal(precision,scale)`.

Two of those types need an explicit tag, because `arreflect` cannot infer the
untagged spelling as a struct field. See
[below](#the-two-types-that-need-a-tag):

| Field | Required tag |
| --- | --- |
| `time.Time` | one of `date32`, `date64`, `time32`, `time64` |
| `decimal128.Num`, `decimal256.Num` | `decimal(precision,scale)` |

Anything else such as nested structs, slices other than `[]byte`, arrays, maps,
embedded fields, defined scalar types such as `type ID int64` is a
generate-time error naming the field. Those structs still encode fine with `arreflect` at runtime.

The generator is equally strict about options that would do nothing: `date32` on
an `int64` field is an error here even though `arreflect` ignores it.

## Equivalence with arreflect

The point of matching `arreflect`'s tag dialect is that switching between the
two paths is a call-site change and nothing else. `internal/gentypes` holds a
fixture with one field for every supported column shape and asserts, on every
run, that the generated encoder and `arreflect` produce the same schema and the
same column data for the same input. Every column is compared, with no
exceptions.

### The two types that need a tag

`arreflect`'s `inferArrowType` switches on `reflect.Kind` before it reaches the
types it matches by identity, so a Go struct that Arrow models as a scalar
reaches `inferStructType` instead. That finds only unexported fields and yields
an empty `struct<>`, and the value is then dropped. Today that affects:

| Field | `arreflect` infers |
| --- | --- |
| `time.Time`, untagged or `,timestamp` | `struct<>` |
| `decimal128.Num` / `decimal256.Num`, untagged | `struct<>` |

A tag naming the Arrow type survives, such as `,date32` or `,decimal(20,3)`,
because `arreflect` applies tags after inference. `arreflect.FromSlice` also
handles them correctly at the top level, returning `timestamp[ns, tz=UTC]` and
`decimal(38, 0)`, because `buildArray` matches them by type rather than reaching
`inferStructType`.

`arreflect`'s own tests assert the intended mapping against
`inferPrimitiveArrowType`, which a struct field never reaches, so they pass
either way.

`arrgen` could emit the column Arrow means here, and an earlier revision did.
Generated code and `arreflect` would then disagree about the schema, so instead
these spellings are a generate-time error naming the field and the tag that
fixes it. One consequence: **`arrgen` cannot emit a `TIMESTAMP` column**, since
`,timestamp` is rejected along with the untagged spelling.

`TestArreflectCannotInferStructScalars` pins the upstream behavior. If
`arrow/array/arreflect` is fixed, that test fails, which is the signal to drop
these rejections and generate the columns.

## Performance

`go test ./internal/gentypes/ -bench . -benchmem`, Go 1.25, linux/arm64, 2 vCPU.
Batch benchmarks encode 1024 rows per operation.

| Benchmark | arreflect | arrgen | |
| --- | --- | --- | --- |
| `MetricBatch` (4 columns) | 69.6 µs, 97 allocs | 30.5 µs, 52 allocs | **2.3x faster** |
| `FixedBatch` (5 fixed-width columns) | 72.9 µs, 96 allocs | 26.7 µs, 46 allocs | **2.7x faster** |
| `RowBatch` (49 columns) | 870 µs, 3502 allocs | 483 µs, 3174 allocs | **1.8x faster** |
| `StreamAppend` (per row) | 73.2 ns | 27.5 ns, **0 allocs** | **2.7x faster** |
| `Schema` | 6.4 µs, 131 allocs | 1.8 ns, **0 allocs** | |

Three things move the numbers:

- **Field access.** The generated code loads `v.CPU` and calls
  `Float64Builder.Append`. The reflection path resolves a field index, produces
  a `reflect.Value`, and dispatches on the builder's dynamic type, per value.
- **Builder lookup.** Typed builders are resolved once, in the constructor, so
  `Append` does no type assertions at all, not even the
  `b.Field(i).(*array.Float64Builder)` a hand-written encoder usually repeats.
- **Schema construction.** The schema is a package-level variable rather than a
  struct walk per call.

Allocation counts converge on wide fixtures because both paths pay for the same
Arrow buffers, and because Arrow's dictionary builders allocate per appended
value regardless of who calls them. The place the difference is unambiguous is
the streaming path: with room reserved, `Append` allocates nothing, and
`TestStreamingAppendIsAllocationFree` asserts exactly that rather than leaving
it to a benchmark to imply.

The streaming path also removes a constraint rather than a cost: `arreflect`
cannot encode anything until the caller has materialized the whole `[]T`, while
an appender can be fed a row at a time and cut into batches wherever you like.

## Working on arrgen

This module's `go.mod` names a released `github.com/apache/arrow-go/v18`, so by
default it builds against that rather than the tree it sits in:

```sh
cd arrgen && go test ./...
```

To test it against the local arrow-go instead, put a workspace over the two
modules. This is what you want when changing `arrow/array/arreflect`, since the
equivalence tests are what catch a divergence:

```sh
go work init . ./arrgen   # from the repository root; go.work is gitignored
go test ./arrgen/...
```

`ci/scripts/build.sh` and `ci/scripts/test.sh` both descend into this module
explicitly, because `./...` in the root module stops at its `go.mod`. CI builds
it, vets it, runs its tests under the same `-race`/`-asan` args as the rest of
the repository, and runs `go generate ./...` followed by `git diff
--exit-code` so a committed output that drifted from its struct fails the build.

`TestCheckedInFilesAreUpToDate` checks the same property from inside the test
binary, which is what fails first when you edit a struct and forget to
regenerate.

The golden file in `testdata` is where generator changes get reviewed:
`go test ./arrgen/ -update` rewrites it, and the resulting diff is exactly what
users would see in their own regenerated code.

### Releasing

As a nested module, `arrgen` is versioned and tagged independently of the root
module: its tags are `arrgen/vX.Y.Z`, not `vX.Y.Z`. It cannot share the root's
`v18` line, because a module path without a `/vN` suffix is limited to v0 and
v1. Nothing in the release scripts tags it yet. That is a deliberate omission
for maintainers to decide on, and it is why the Quick Start cannot yet name a
version that resolves.
