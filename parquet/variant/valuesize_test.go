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

package variant

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValueSizeLargeArray(t *testing.T) {
	// Build a large array with >255 elements to trigger is_large=true.
	// This is a regression test for the is_large bit position in valueSize().
	var b Builder
	start := b.Offset()
	offsets := make([]int, 0, 300)
	for i := 0; i < 300; i++ {
		offsets = append(offsets, b.NextElement(start))
		b.AppendNull()
	}
	require.NoError(t, b.FinishArray(start, offsets))

	raw := b.BuildWithoutMeta()

	// Verify the header has is_large set at the correct bit position
	typeInfo := (raw[0] >> basicTypeBits) & typeInfoMask
	isLarge := ((typeInfo >> 2) & 0x1) == 1
	assert.True(t, isLarge, "expected is_large=true for 300-element array")

	// Verify valueSize returns correct result
	got := valueSize(raw)

	// Compute expected size:
	// header(1) + numElements(4, is_large=true) + offsets((300+1)*offsetSize) + data(300*1)
	offsetSize := int((typeInfo & 0b11) + 1)
	expected := 1 + 4 + (300+1)*offsetSize + 300

	assert.Equal(t, expected, got, "valueSize should correctly handle large arrays")
}

func TestValueSizeLargeObject(t *testing.T) {
	// Verify that valueSize still works correctly for large objects (>255 fields).
	var b Builder
	start := b.Offset()
	fields := make([]FieldEntry, 0, 300)
	for i := 0; i < 300; i++ {
		fields = append(fields, b.NextField(start, string(rune('a'+i%26))+string(rune('0'+i/26))))
		b.AppendNull()
	}
	require.NoError(t, b.FinishObject(start, fields))

	raw := b.BuildWithoutMeta()

	// Verify the header has is_large set at the correct bit position for objects (bit 4).
	typeInfo := (raw[0] >> basicTypeBits) & typeInfoMask
	isLarge := ((typeInfo >> 4) & 0x1) == 1
	assert.True(t, isLarge, "expected is_large=true for 300-field object")

	got := valueSize(raw)
	assert.Equal(t, len(raw), got, "valueSize should return the full object size")
}
