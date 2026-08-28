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

package array

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/internal/testing/tools"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestBuilder_Init(t *testing.T) {
	type exp struct{ size int }
	tests := []struct {
		name string
		cap  int

		exp exp
	}{
		{"07 bits", 07, exp{size: 1}},
		{"19 bits", 19, exp{size: 3}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ab := &builder{mem: memory.NewGoAllocator()}
			ab.init(test.cap)
			assert.Equal(t, test.cap, ab.Cap(), "invalid capacity")
			assert.Equal(t, test.exp.size, ab.nullBitmap.Len(), "invalid length")
		})
	}
}

func TestBuilder_UnsafeSetValid(t *testing.T) {
	ab := &builder{mem: memory.NewGoAllocator()}
	ab.init(32)
	ab.unsafeAppendBoolsToBitmap(tools.Bools(0, 0, 0, 0, 0), 5)
	assert.Equal(t, 5, ab.Len())
	assert.Equal(t, []byte{0, 0, 0, 0}, ab.nullBitmap.Bytes())

	ab.unsafeSetValid(17)
	assert.Equal(t, []byte{0xe0, 0xff, 0x3f, 0}, ab.nullBitmap.Bytes())
}

func TestBuilder_UnsafeAppendBoolsToBitmap(t *testing.T) {
	patterns := []struct {
		name  string
		valid func(int) bool
	}{
		{"all valid", func(int) bool { return true }},
		{"all null", func(int) bool { return false }},
		{"alternating", func(i int) bool { return i%2 == 0 }},
		{"one in three null", func(i int) bool { return i%3 != 0 }},
	}

	for _, pattern := range patterns {
		for offset := 0; offset < 8; offset++ {
			for length := 1; length <= 33; length++ {
				b := &builder{mem: memory.NewGoAllocator()}
				b.init(48)
				for i := range b.nullBitmap.Bytes() {
					b.nullBitmap.Bytes()[i] = byte(0x5a + i*31)
				}

				expectedBitmap := append([]byte(nil), b.nullBitmap.Bytes()...)
				valid := make([]bool, length)
				expectedNulls := offset - bitutil.CountSetBits(expectedBitmap, 0, offset)
				for i := range valid {
					valid[i] = pattern.valid(i)
					if valid[i] {
						bitutil.SetBit(expectedBitmap, offset+i)
					} else {
						bitutil.ClearBit(expectedBitmap, offset+i)
						expectedNulls++
					}
				}

				b.length = offset
				b.nulls = offset - bitutil.CountSetBits(b.nullBitmap.Bytes(), 0, offset)
				b.unsafeAppendBoolsToBitmap(valid, len(valid))

				assert.Equal(t, offset+length, b.Len(), "%s, offset=%d, length=%d", pattern.name, offset, length)
				assert.Equal(t, expectedNulls, b.NullN(), "%s, offset=%d, length=%d", pattern.name, offset, length)
				assert.Equal(t, expectedBitmap, b.nullBitmap.Bytes(), "%s, offset=%d, length=%d", pattern.name, offset, length)
				b.nullBitmap.Release()
			}
		}
	}
}

func TestPackBoolsByte(t *testing.T) {
	for want := 0; want < 1<<8; want++ {
		valid := make([]bool, 8)
		for i := range valid {
			valid[i] = want&(1<<i) != 0
		}
		assert.Equal(t, byte(want), packBoolsByte(valid), "want=%08b", want)
	}
}

func TestPackBoolsToBitmap(t *testing.T) {
	patterns := []struct {
		name  string
		value func(int) bool
	}{
		{"all false", func(int) bool { return false }},
		{"all true", func(int) bool { return true }},
		{"alternating", func(i int) bool { return i%2 == 0 }},
		{"one in three", func(i int) bool { return i%3 == 0 }},
	}

	for _, pattern := range patterns {
		for offset := 0; offset < 8; offset++ {
			for length := 0; length <= 33; length++ {
				got := make([]byte, 8)
				for i := range got {
					got[i] = byte(0x5a + i*31)
				}
				want := append([]byte(nil), got...)
				values := make([]bool, length)
				for i := range values {
					values[i] = pattern.value(i)
					bitutil.SetBitTo(want, offset+i, values[i])
				}

				packBoolsToBitmap(got, offset, values)
				assert.Equal(t, want, got, "%s, offset=%d, length=%d", pattern.name, offset, length)
			}
		}
	}
}

func TestBuilder_resize(t *testing.T) {
	b := &builder{mem: memory.NewGoAllocator()}
	n := 64

	b.init(n)
	assert.Equal(t, n, b.Cap())
	assert.Equal(t, 0, b.Len())

	b.UnsafeAppendBoolToBitmap(true)
	for i := 1; i < n; i++ {
		b.UnsafeAppendBoolToBitmap(false)
	}
	assert.Equal(t, n, b.Cap())
	assert.Equal(t, n, b.Len())
	assert.Equal(t, n-1, b.NullN())

	n = 5
	b.resize(n, b.init)
	assert.Equal(t, n, b.Len())
	assert.Equal(t, n-1, b.NullN())

	b.resize(32, b.init)
	assert.Equal(t, n, b.Len())
	assert.Equal(t, n-1, b.NullN())
}

func TestBuilder_IsNull(t *testing.T) {
	b := &builder{mem: memory.NewGoAllocator()}
	n := 32
	b.init(n)

	assert.True(t, b.IsNull(0))
	assert.True(t, b.IsNull(1))

	for i := 0; i < n; i++ {
		b.UnsafeAppendBoolToBitmap(i%2 == 0)
	}
	for i := 0; i < n; i++ {
		assert.Equal(t, i%2 != 0, b.IsNull(i))
	}
}

func TestBuilder_SetNull(t *testing.T) {
	b := &builder{mem: memory.NewGoAllocator()}
	n := 32
	b.init(n)

	for i := 0; i < n; i++ {
		// Set everything to true
		b.UnsafeAppendBoolToBitmap(true)
	}
	for i := 0; i < n; i++ {
		if i%2 == 0 { // Set all even numbers to null
			b.SetNull(i)
		}
	}
	assert.Equal(t, n/2, b.NullN())

	// idempotent SetNull
	b.SetNull(0)
	assert.Equal(t, n/2, b.NullN())

	for i := 0; i < n; i++ {
		if i%2 == 0 {
			assert.True(t, b.IsNull(i))
		} else {
			assert.False(t, b.IsNull(i))
		}
	}
}
