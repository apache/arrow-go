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

	"github.com/stretchr/testify/assert"
)

func TestMetadataEqual(t *testing.T) {
	for _, tc := range []struct {
		name        string
		left, right Metadata
		want        bool
	}{
		{"empty", Metadata{}, NewMetadata(nil, nil), true},
		{"empty-map", Metadata{}, MetadataFrom(map[string]string{}), true},
		{"single", NewMetadata([]string{"key"}, []string{"value"}), NewMetadata([]string{"key"}, []string{"value"}), true},
		{"single-copied-strings", NewMetadata([]string{"key"}, []string{"value"}), NewMetadata([]string{strings.Clone("key")}, []string{strings.Clone("value")}), true},
		{"single-long-key-difference", NewMetadata([]string{strings.Repeat("a", 4096) + "x"}, []string{"value"}), NewMetadata([]string{strings.Repeat("a", 4096) + "y"}, []string{"value"}), false},
		{"single-long-value-difference", NewMetadata([]string{"key"}, []string{strings.Repeat("a", 4096) + "x"}), NewMetadata([]string{"key"}, []string{strings.Repeat("a", 4096) + "y"}), false},
		{"different-lengths", Metadata{}, NewMetadata([]string{"key"}, []string{"value"}), false},
		{"different-key", NewMetadata([]string{"key"}, []string{"value"}), NewMetadata([]string{"other"}, []string{"value"}), false},
		{"different-value", NewMetadata([]string{"key"}, []string{"value"}), NewMetadata([]string{"key"}, []string{"other"}), false},
		{"empty-key-value", NewMetadata([]string{""}, []string{""}), NewMetadata([]string{""}, []string{""}), true},
		{"case-sensitive", NewMetadata([]string{"key"}, []string{"value"}), NewMetadata([]string{"Key"}, []string{"value"}), false},
		{"unicode", NewMetadata([]string{"é", "键"}, []string{"值", ""}), NewMetadata([]string{"é", "键"}, []string{"值", ""}), true},
		{"unicode-normalization", NewMetadata([]string{"é"}, []string{"value"}), NewMetadata([]string{"e\u0301"}, []string{"value"}), false},
		{"embedded-zero", NewMetadata([]string{"key\x00"}, []string{"value\x00"}), NewMetadata([]string{"key\x00"}, []string{"value\x00"}), true},
		{"matching-unsorted", NewMetadata([]string{"z", "a", "m"}, []string{"1", "2", "3"}), NewMetadata([]string{"z", "a", "m"}, []string{"1", "2", "3"}), true},
		{"reordered", NewMetadata([]string{"z", "a", "m"}, []string{"1", "2", "3"}), NewMetadata([]string{"a", "m", "z"}, []string{"2", "3", "1"}), true},
		{"reordered-different-value", NewMetadata([]string{"z", "a", "m"}, []string{"1", "2", "3"}), NewMetadata([]string{"a", "m", "z"}, []string{"2", "4", "1"}), false},
		{"duplicate-keys", NewMetadata([]string{"key", "key"}, []string{"1", "2"}), NewMetadata([]string{"key", "key"}, []string{"1", "2"}), true},
		{"duplicate-values-swapped", NewMetadata([]string{"key", "key"}, []string{"1", "2"}), NewMetadata([]string{"key", "key"}, []string{"2", "1"}), false},
		{"duplicate-pairs-reordered", NewMetadata([]string{"key", "key", "a"}, []string{"v", "v", "a"}), NewMetadata([]string{"a", "key", "key"}, []string{"a", "v", "v"}), true},
		{"duplicate-distinct-values-reordered", NewMetadata([]string{"b", "a", "b"}, []string{"first", "a", "second"}), NewMetadata([]string{"a", "b", "b"}, []string{"a", "first", "second"}), true},
		{"duplicate-distinct-values-swapped", NewMetadata([]string{"b", "a", "b"}, []string{"first", "a", "second"}), NewMetadata([]string{"a", "b", "b"}, []string{"a", "second", "first"}), false},
		{"duplicate-counts", NewMetadata([]string{"key", "key", "a"}, []string{"v", "v", "v"}), NewMetadata([]string{"key", "a", "a"}, []string{"v", "v", "v"}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leftBefore := Metadata{keys: slices.Clone(tc.left.keys), values: slices.Clone(tc.left.values)}
			rightBefore := Metadata{keys: slices.Clone(tc.right.keys), values: slices.Clone(tc.right.values)}
			assert.Equal(t, tc.want, tc.left.Equal(tc.right))
			assert.Equal(t, tc.want, tc.right.Equal(tc.left))
			assert.Equal(t, leftBefore, tc.left)
			assert.Equal(t, rightBefore, tc.right)
		})
	}
}

func TestMetadataEqualDuplicateKeys(t *testing.T) {
	for _, size := range []int{1, 2, 3, 12, 13, 15, 16, 17, 31, 32, 33, 64, 127, 128, 129, 255, 256, 257, 1024, 2047, 2048, 2049} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			keys, values := make([]string, size), make([]string, size)
			for i := range keys {
				keys[i] = fmt.Sprintf("key_%d", i%4)
				values[i] = fmt.Sprintf("value_%d", i)
			}
			left, right := NewMetadata(keys, values), NewMetadata(keys, values)
			assert.True(t, left.Equal(right))
			right.Values()[size/2] = "changed"
			assert.False(t, left.Equal(right))
			assert.False(t, right.Equal(left))
		})
	}
}

func TestMetadataEqualReordered(t *testing.T) {
	for _, size := range []int{2, 8, 11, 12, 13, 15, 16, 17, 31, 32, 33, 127, 128, 129, 256, 257, 1024, 2047, 2048, 2049} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			keys, values := make([]string, size), make([]string, size)
			for i := range keys {
				keys[i] = fmt.Sprintf("key_%04d", i)
				values[i] = fmt.Sprintf("value_%04d", i)
			}
			left := NewMetadata(keys, values)
			for _, pattern := range []string{"reversed", "middle-swap", "last-swap"} {
				t.Run(pattern, func(t *testing.T) {
					right := left.clone()
					if pattern == "reversed" {
						slices.Reverse(right.Keys())
						slices.Reverse(right.Values())
					} else {
						pos := size/2 - 1
						if pattern == "last-swap" {
							pos = size - 2
						}
						right.keys[pos], right.keys[pos+1] = right.keys[pos+1], right.keys[pos]
						right.values[pos], right.values[pos+1] = right.values[pos+1], right.values[pos]
					}
					leftBefore, rightBefore := left.clone(), right.clone()
					assert.True(t, left.Equal(right))
					assert.True(t, right.Equal(left))
					assert.Equal(t, leftBefore, left)
					assert.Equal(t, rightBefore, right)
					right.Values()[size/2] = "changed"
					assert.False(t, left.Equal(right))
					assert.False(t, right.Equal(left))
				})
			}
		})
	}
}

func TestMetadataEqualPermutations(t *testing.T) {
	keys := []string{"z", "", "a", "键"}
	values := []string{"value", "\x00", "", "值"}
	var permutations [][]int
	order := []int{0, 1, 2, 3}
	var permute func(int)
	permute = func(pos int) {
		if pos == len(order) {
			permutations = append(permutations, slices.Clone(order))
			return
		}
		for i := pos; i < len(order); i++ {
			order[pos], order[i] = order[i], order[pos]
			permute(pos + 1)
			order[pos], order[i] = order[i], order[pos]
		}
	}
	permute(0)
	makeMetadata := func(order []int) Metadata {
		k, v := make([]string, len(order)), make([]string, len(order))
		for i, idx := range order {
			k[i], v[i] = keys[idx], values[idx]
		}
		return NewMetadata(k, v)
	}
	for _, leftOrder := range permutations {
		for _, rightOrder := range permutations {
			t.Run(fmt.Sprintf("%v/%v", leftOrder, rightOrder), func(t *testing.T) {
				left, right := makeMetadata(leftOrder), makeMetadata(rightOrder)
				assert.True(t, left.Equal(right))
				assert.True(t, right.Equal(left))
			})
		}
	}
}

func TestMetadataEqualAfterMutation(t *testing.T) {
	left := NewMetadata([]string{"a", "b"}, []string{"1", "2"})
	right := left.clone()
	assert.True(t, left.Equal(right))
	right.Values()[0] = "changed"
	assert.False(t, left.Equal(right))
	right.Values()[0] = "1"
	right.Keys()[0] = "changed"
	assert.False(t, left.Equal(right))
	right.Keys()[0] = "a"
	assert.True(t, left.Equal(right))
	slices.Reverse(right.Keys())
	slices.Reverse(right.Values())
	assert.True(t, left.Equal(right))
}
