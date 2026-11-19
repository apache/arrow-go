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

//go:build go1.18

package compute_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func newCastBufferTestInput(dt arrow.DataType, values []string, valid []bool) arrow.Array {
	b := array.NewBuilder(memory.DefaultAllocator, dt)
	defer b.Release()
	for i, v := range values {
		if !valid[i] {
			b.AppendNull()
			continue
		}
		switch bb := b.(type) {
		case *array.BinaryBuilder:
			bb.Append([]byte(v))
		case *array.StringBuilder:
			bb.Append(v)
		case *array.LargeStringBuilder:
			bb.Append(v)
		default:
			panic("unexpected binary-like builder")
		}
	}
	return b.NewArray()
}

func castBufferTestValue(a arrow.Array, i int) string {
	switch v := a.(type) {
	case *array.Binary:
		return string(v.Value(i))
	case *array.LargeBinary:
		return string(v.Value(i))
	case *array.String:
		return v.Value(i)
	case *array.LargeString:
		return v.Value(i)
	case *array.BinaryView:
		return string(v.Value(i))
	case *array.StringView:
		return v.Value(i)
	case *array.FixedSizeBinary:
		return string(v.Value(i))
	default:
		panic("unexpected binary-like array")
	}
}

func assertCastBufferContents(t *testing.T, expected, got arrow.Array) {
	t.Helper()
	if got.Len() != expected.Len() || got.NullN() != expected.NullN() {
		t.Fatalf("length/null count mismatch: got (%d, %d), want (%d, %d)",
			got.Len(), got.NullN(), expected.Len(), expected.NullN())
	}
	if v, ok := got.(interface{ ValidateFull() error }); ok {
		if err := v.ValidateFull(); err != nil {
			t.Fatalf("invalid cast output: %v", err)
		}
	}
	for i := 0; i < expected.Len(); i++ {
		if got.IsNull(i) != expected.IsNull(i) {
			t.Fatalf("null mismatch at %d", i)
		}
		if !expected.IsNull(i) && castBufferTestValue(got, i) != castBufferTestValue(expected, i) {
			t.Fatalf("value mismatch at %d: got %q want %q",
				i, castBufferTestValue(got, i), castBufferTestValue(expected, i))
		}
	}
}

func TestCastBinaryViewDirectBuffers(t *testing.T) {
	values := []string{"slice-prefix", "", "a", "123456789012", "1234567890123",
		strings.Repeat("hello", 16), "héllo 🌍", strings.Repeat("data", 32), "tail"}
	validity := []struct {
		name  string
		valid []bool
	}{
		{"all-valid", []bool{true, true, true, true, true, true, true, true, true}},
		{"mixed", []bool{true, false, true, true, false, true, false, true, true}},
		{"all-null", []bool{false, false, false, false, false, false, false, false, false}},
	}
	physicalTypes := []arrow.DataType{
		arrow.BinaryTypes.Binary, arrow.BinaryTypes.LargeBinary,
		arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString,
	}
	viewTypes := []arrow.DataType{arrow.BinaryTypes.BinaryView, arrow.BinaryTypes.StringView}

	for _, dt := range physicalTypes {
		for _, validityCase := range validity {
			t.Run(dt.String()+"/"+validityCase.name, func(t *testing.T) {
				input := newCastBufferTestInput(dt, values, validityCase.valid)
				defer input.Release()
				// Non-zero offsets exercise both sliced input data and bitmap alignment.
				sliced := array.NewSlice(input, 1, int64(input.Len()-1))
				defer sliced.Release()

				for _, viewType := range viewTypes {
					t.Run(viewType.String(), func(t *testing.T) {
						view, err := compute.CastArray(context.Background(), sliced, compute.SafeCastOptions(viewType))
						if err != nil {
							t.Fatal(err)
						}
						defer view.Release()
						assertCastBufferContents(t, sliced, view)
						if len(view.Data().Buffers()) > 3 {
							t.Fatalf("view uses %d buffers, expected at most 3", len(view.Data().Buffers()))
						}

						for _, targetType := range physicalTypes {
							t.Run(targetType.String(), func(t *testing.T) {
								got, err := compute.CastArray(context.Background(), view, compute.SafeCastOptions(targetType))
								if err != nil {
									t.Fatal(err)
								}
								defer got.Release()
								assertCastBufferContents(t, sliced, got)
								if arrow.TypeEqual(targetType, dt) && !array.Equal(sliced, got) {
									t.Fatal("round-trip changed the input array")
								}
							})
						}
					})
				}
			})
		}
	}
}

func TestCastFixedSizeBinaryToViewDirectBuffers(t *testing.T) {
	b := array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, &arrow.FixedSizeBinaryType{ByteWidth: 16})
	b.Append([]byte("abcdefghijklmnop"))
	b.AppendNull()
	b.Append([]byte("1234567890abcdef"))
	b.Append([]byte("16-bytes-of-data"))
	input := b.NewArray()
	b.Release()
	defer input.Release()

	for _, viewType := range []arrow.DataType{arrow.BinaryTypes.BinaryView, arrow.BinaryTypes.StringView} {
		view, err := compute.CastArray(context.Background(), input, compute.SafeCastOptions(viewType))
		if err != nil {
			t.Fatal(err)
		}
		assertCastBufferContents(t, input, view)
		view.Release()
	}
}

func BenchmarkBinaryToViewDirectBuffers(b *testing.B) {
	const count = 64 * 1024
	for _, payloadSize := range []int{8, 32, 256} {
		for _, nullEvery := range []int{0, 10} {
			name := strings.Repeat("x", payloadSize)
			values := make([]string, count)
			valid := make([]bool, count)
			for i := range values {
				values[i] = name
				valid[i] = nullEvery == 0 || i%nullEvery != 0
			}
			b.Run(fmt.Sprintf("bytes=%d/null-every=%d", payloadSize, nullEvery), func(b *testing.B) {
				input := newCastBufferTestInput(arrow.BinaryTypes.String, values, valid)
				defer input.Release()
				b.SetBytes(int64(count * payloadSize))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					out, err := compute.CastArray(context.Background(), input,
						compute.SafeCastOptions(arrow.BinaryTypes.StringView))
					if err != nil {
						b.Fatal(err)
					}
					out.Release()
				}
			})
		}
	}
}
