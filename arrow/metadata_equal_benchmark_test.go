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

package arrow

import (
	"fmt"
	"slices"
	"testing"
)

var metadataEqualBenchmarkResult bool

func metadataEqualityBenchmarkPair(size int, pattern string) (Metadata, Metadata) {
	keys, values := make([]string, size), make([]string, size)
	for i := range keys {
		keys[i] = fmt.Sprintf("key_%04d", i)
		values[i] = fmt.Sprintf("value_%04d", i)
	}
	left := NewMetadata(keys, values)
	right := left.clone()
	switch pattern {
	case "matching-reversed":
		slices.Reverse(left.keys)
		slices.Reverse(left.values)
		slices.Reverse(right.keys)
		slices.Reverse(right.values)
	case "different-value":
		right.values[size-1] = "different"
	case "reversed":
		slices.Reverse(right.keys)
		slices.Reverse(right.values)
	case "late-swap", "middle-swap":
		pos := size - 2
		if pattern == "middle-swap" {
			pos = size/2 - 1
		}
		right.keys[pos], right.keys[pos+1] = right.keys[pos+1], right.keys[pos]
		right.values[pos], right.values[pos+1] = right.values[pos+1], right.values[pos]
	}
	return left, right
}

func BenchmarkMetadataEqual(b *testing.B) {
	for _, size := range []int{0, 1, 8, 32, 128} {
		for _, pattern := range []string{"matching", "matching-reversed", "different-value", "reversed", "late-swap", "middle-swap"} {
			if size == 0 && pattern != "matching" || size == 1 && pattern != "matching" && pattern != "different-value" {
				continue
			}
			b.Run(fmt.Sprintf("size=%d/%s", size, pattern), func(b *testing.B) {
				left, right := metadataEqualityBenchmarkPair(size, pattern)
				if got := left.Equal(right); got != (pattern != "different-value") {
					b.Fatalf("unexpected equality: %v", got)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					metadataEqualBenchmarkResult = left.Equal(right)
				}
			})
		}
	}
}

func BenchmarkSchemaEqualFieldMetadata(b *testing.B) {
	const nfields = 16
	for _, size := range []int{0, 8, 32} {
		for _, pattern := range []string{"matching", "reversed"} {
			if size == 0 && pattern != "matching" {
				continue
			}
			b.Run(fmt.Sprintf("size=%d/%s", size, pattern), func(b *testing.B) {
				leftMetadata, rightMetadata := metadataEqualityBenchmarkPair(size, pattern)
				leftFields, rightFields := make([]Field, nfields), make([]Field, nfields)
				for i := range leftFields {
					name := fmt.Sprintf("field_%d", i)
					leftFields[i] = Field{Name: name, Type: PrimitiveTypes.Int64, Metadata: leftMetadata}
					rightFields[i] = Field{Name: name, Type: PrimitiveTypes.Int64, Metadata: rightMetadata}
				}
				left, right := NewSchema(leftFields, nil), NewSchema(rightFields, nil)
				if !left.Equal(right) {
					b.Fatal("schemas should be equal")
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					metadataEqualBenchmarkResult = left.Equal(right)
				}
			})
		}
	}
}
