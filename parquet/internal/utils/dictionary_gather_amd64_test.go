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

//go:build amd64 && !noasm && !appengine

package utils

import (
	"slices"
	"testing"
	"unsafe"

	"golang.org/x/sys/cpu"
)

func TestCopyDictionaryAVX2Bounds(t *testing.T) {
	t.Run("int32", testCopyDictionaryAVX2Bounds[int32])
	t.Run("int64", testCopyDictionaryAVX2Bounds[int64])
	t.Run("float32", testCopyDictionaryAVX2Bounds[float32])
	t.Run("float64", testCopyDictionaryAVX2Bounds[float64])
}

func testCopyDictionaryAVX2Bounds[T int32 | int64 | float32 | float64](t *testing.T) {
	dictionaryStorage := make([]T, 35)
	dictionary := dictionaryStorage[1:34]
	for i := range dictionary {
		dictionary[i] = T(i*17 - 100)
	}
	originalDictionary := slices.Clone(dictionaryStorage)
	threshold := dictionaryGather32MinValues
	if unsafe.Sizeof(T(0)) == 8 {
		threshold = dictionaryGather64MinValues
	}
	for _, length := range []int{0, 1, 15, 16, 17, 31, 32, 33, 64, 65, 1023, 1024, 1025} {
		for _, offset := range []int{0, 1, 7} {
			indexStorage := make([]IndexType, length+offset+1)
			indices := indexStorage[offset : offset+length]
			for i := range indices {
				indices[i] = IndexType((i*19 + length) % len(dictionary))
			}
			originalIndices := slices.Clone(indexStorage)
			output := make([]T, length+offset+2)
			for i := range output {
				output[i] = -77
			}
			expected := slices.Clone(output)
			wantDispatch := cpu.X86.HasAVX2 && length >= threshold
			if wantDispatch {
				copyDictionaryScalar(expected[offset:], dictionary, indices)
			}
			if got := CopyDictionary(output[offset:len(output)-1], dictionary, indices); got != wantDispatch {
				t.Fatalf("length=%d offset=%d: dispatch=%v, want %v", length, offset, got, wantDispatch)
			}
			if !slices.Equal(expected, output) {
				t.Fatalf("length=%d offset=%d: unexpected output or overwritten guard", length, offset)
			}
			if !slices.Equal(originalDictionary, dictionaryStorage) || !slices.Equal(originalIndices, indexStorage) {
				t.Fatal("dictionary copy modified its input")
			}
		}
	}
}

func TestCopyDictionaryDisabledAVX2(t *testing.T) {
	saved32, saved64 := dictionaryGather32, dictionaryGather64
	dictionaryGather32, dictionaryGather64 = nil, nil
	defer func() { dictionaryGather32, dictionaryGather64 = saved32, saved64 }()
	indices := make([]IndexType, 64)
	if CopyDictionary(make([]int32, len(indices)), []int32{1}, indices) {
		t.Fatal("32-bit gather ran while disabled")
	}
	if CopyDictionary(make([]int64, len(indices)), []int64{1}, indices) {
		t.Fatal("64-bit gather ran while disabled")
	}
}
