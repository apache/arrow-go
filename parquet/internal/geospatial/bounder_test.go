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

package geospatial_test

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/internal/geospatial"
	"github.com/stretchr/testify/assert"
)

type byteOrder interface {
	binary.ByteOrder
	binary.AppendByteOrder
}

func wkb(order byteOrder, code uint32, parts ...any) []byte {
	var out []byte
	if order == byteOrder(binary.BigEndian) {
		out = append(out, 0)
	} else {
		out = append(out, 1)
	}
	out = order.AppendUint32(out, code)
	for _, p := range parts {
		switch v := p.(type) {
		case float64:
			out = order.AppendUint64(out, math.Float64bits(v))
		case uint32:
			out = order.AppendUint32(out, v)
		case []byte:
			out = append(out, v...)
		}
	}
	return out
}

func TestBounder(t *testing.T) {
	le, be := byteOrder(binary.LittleEndian), byteOrder(binary.BigEndian)

	b := geospatial.NewBounder()
	assert.True(t, b.Empty())

	b.Update(wkb(le, 1, -122.4, 37.8))
	b.Update(wkb(be, 2, uint32(2), 10.0, -5.0, 20.0, 50.0))
	assert.True(t, b.Valid())
	assert.Equal(t, []int32{1, 2}, b.Types())
	assert.Equal(t, -122.4, b.Min(geospatial.DimX))
	assert.Equal(t, 20.0, b.Max(geospatial.DimX))
	assert.Equal(t, -5.0, b.Min(geospatial.DimY))
	assert.Equal(t, 50.0, b.Max(geospatial.DimY))
	assert.False(t, b.HasDim(geospatial.DimZ))
	assert.False(t, b.HasDim(geospatial.DimM))

	// Z and M, polygon, and a nested collection
	b.Update(wkb(le, 1003, uint32(1), uint32(2), 0.0, 0.0, 7.0, 1.0, 1.0, -3.0))
	b.Update(wkb(le, 2001, uint32(1), 0.0, 0.0)) // invalid: truncated
	assert.False(t, b.Valid())

	b = geospatial.NewBounder()
	b.Update(wkb(le, 1003, uint32(1), uint32(2), 0.0, 0.0, 7.0, 1.0, 1.0, -3.0))
	b.Update(wkb(le, 2002, uint32(1), 0.0, 0.0, 4.0))
	b.Update(wkb(le, 7, uint32(1), wkb(le, 1, 100.0, 1.0)))
	assert.True(t, b.Valid())
	assert.Equal(t, []int32{7, 1003, 2002}, b.Types())
	assert.True(t, b.HasDim(geospatial.DimZ))
	assert.Equal(t, -3.0, b.Min(geospatial.DimZ))
	assert.Equal(t, 7.0, b.Max(geospatial.DimZ))
	assert.Equal(t, 4.0, b.Min(geospatial.DimM))
	assert.Equal(t, 100.0, b.Max(geospatial.DimX))

	// EWKB Z flag normalizes to the ISO code
	b = geospatial.NewBounder()
	b.Update(wkb(le, 1|0x80000000, 1.0, 2.0, 3.0))
	assert.Equal(t, []int32{1001}, b.Types())
}

func TestBounderEmptyPointAndInvalid(t *testing.T) {
	le := byteOrder(binary.LittleEndian)
	nan := math.NaN()

	b := geospatial.NewBounder()
	b.Update(wkb(le, 1, nan, nan))
	assert.True(t, b.Valid())
	assert.False(t, b.HasDim(geospatial.DimX))
	assert.Equal(t, []int32{1}, b.Types())

	for name, v := range map[string][]byte{
		"empty":         {},
		"bad order":     {2, 1, 0, 0, 0},
		"truncated":     wkb(le, 1, 1.0),
		"trailing":      append(wkb(le, 1, 1.0, 2.0), 0),
		"partial nan":   wkb(le, 1, 1.0, nan),
		"bad type":      wkb(le, 99, 1.0, 2.0),
		"huge count":    wkb(le, 2, uint32(math.MaxUint32)),
		"huge rings":    wkb(le, 3, uint32(math.MaxUint32)),
		"huge children": wkb(le, 7, uint32(math.MaxUint32)),
	} {
		b := geospatial.NewBounder()
		b.Update(v)
		assert.False(t, b.Valid(), name)
	}

	// invalid is sticky until Reset, and survives Merge
	b = geospatial.NewBounder()
	b.Update(nil)
	other := geospatial.NewBounder()
	other.Merge(b)
	assert.False(t, other.Valid())
	b.Reset()
	assert.True(t, b.Valid())
}
