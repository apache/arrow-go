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
	"strings"
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
	if strings.HasPrefix(pattern, "duplicate-") {
		for i := range left.keys {
			left.keys[i] = fmt.Sprintf("key_%04d", i/2)
			left.values[i] = fmt.Sprintf("value_%04d", i/2)
			right.keys[i], right.values[i] = left.keys[i], left.values[i]
		}
		pattern = strings.TrimPrefix(pattern, "duplicate-")
	}
	if pattern == "middle-swap-reversed" {
		slices.Reverse(left.keys)
		slices.Reverse(left.values)
		slices.Reverse(right.keys)
		slices.Reverse(right.values)
		pattern = "middle-swap"
	}
	switch pattern {
	case "matching-reversed":
		slices.Reverse(left.keys)
		slices.Reverse(left.values)
		slices.Reverse(right.keys)
		slices.Reverse(right.values)
	case "different-value":
		right.values[size-1] = "different"
	case "matching-distinct-strings":
		for i := range right.keys {
			right.keys[i] = strings.Clone(right.keys[i])
			right.values[i] = strings.Clone(right.values[i])
		}
	case "different-key":
		right.keys[size/2] = "different"
	case "reversed":
		slices.Reverse(right.keys)
		slices.Reverse(right.values)
	case "late-swap", "middle-swap", "penultimate-swap":
		pos := size - 2
		if pattern == "middle-swap" {
			pos = size/2 - 1
		} else if pattern == "penultimate-swap" {
			pos = size - 3
		}
		right.keys[pos], right.keys[pos+1] = right.keys[pos+1], right.keys[pos]
		right.values[pos], right.values[pos+1] = right.values[pos+1], right.values[pos]
	}
	return left, right
}

func BenchmarkMetadataEqual(b *testing.B) {
	for _, size := range []int{0, 1, 8, 11, 12, 13, 16, 17, 32, 33, 128, 129, 256, 257, 1024, 2047, 2048, 2049} {
		for _, pattern := range []string{"matching", "matching-reversed", "matching-distinct-strings", "different-value", "reversed", "late-swap", "middle-swap", "penultimate-swap", "middle-swap-reversed", "different-key"} {
			if size == 0 && pattern != "matching" || size == 1 && pattern != "matching" && pattern != "matching-distinct-strings" && pattern != "different-value" && pattern != "different-key" {
				continue
			}
			b.Run(fmt.Sprintf("size=%d/%s", size, pattern), func(b *testing.B) {
				left, right := metadataEqualityBenchmarkPair(size, pattern)
				if got := left.Equal(right); got != (pattern != "different-value" && pattern != "different-key") {
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

func BenchmarkMetadataEqualDuplicateKeys(b *testing.B) {
	for _, size := range []int{8, 12, 13, 32, 128, 129, 257, 1024, 2049} {
		for _, pattern := range []string{"matching", "reversed", "middle-swap"} {
			b.Run(fmt.Sprintf("size=%d/%s", size, pattern), func(b *testing.B) {
				left, right := metadataEqualityBenchmarkPair(size, "duplicate-"+pattern)
				if !left.Equal(right) {
					b.Fatal("metadata should be equal")
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

func BenchmarkMetadataEqualFreshGoroutine(b *testing.B) {
	for _, size := range []int{0, 1, 8, 12, 13, 32, 128, 129, 257, 2049} {
		for _, pattern := range []string{"matching", "middle-swap"} {
			if size < 2 && pattern != "matching" {
				continue
			}
			b.Run(fmt.Sprintf("size=%d/%s", size, pattern), func(b *testing.B) {
				left, right := metadataEqualityBenchmarkPair(size, pattern)
				if !left.Equal(right) {
					b.Fatal("metadata should be equal")
				}
				done := make(chan bool)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					go func() { done <- left.Equal(right) }()
					metadataEqualBenchmarkResult = <-done
				}
			})
		}
	}
}

func BenchmarkSchemaEqualFieldMetadata(b *testing.B) {
	const nfields = 16
	for _, size := range []int{0, 1, 8, 32} {
		for _, pattern := range []string{"matching", "reversed"} {
			if size < 2 && pattern != "matching" {
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
