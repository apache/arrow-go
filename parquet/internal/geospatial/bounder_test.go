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
	"fmt"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/internal/geospatial"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		default:
			// catch e.g. an untyped 0 that was meant to be 0.0
			panic(fmt.Sprintf("wkb: unsupported part %T", p))
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

const (
	ewkbZ    = 0x80000000
	ewkbM    = 0x40000000
	ewkbSRID = 0x20000000
)

// bounds is the expected extent of one dimension. A nil *bounds means the
// dimension must be absent.
type bounds struct{ min, max float64 }

func assertDim(t *testing.T, b *geospatial.Bounder, dim int, want *bounds, msg string) {
	t.Helper()
	if want == nil {
		assert.False(t, b.HasDim(dim), "%s: dim %d should be absent", msg, dim)
		return
	}
	if assert.True(t, b.HasDim(dim), "%s: dim %d should be present", msg, dim) {
		assert.Equal(t, *want, bounds{b.Min(dim), b.Max(dim)}, "%s: dim %d", msg, dim)
	}
}

func TestBounderGeometryTypes(t *testing.T) {
	for _, order := range []byteOrder{binary.LittleEndian, binary.BigEndian} {
		o := order
		tests := []struct {
			name  string
			wkb   []byte
			types []int32
			x, y  bounds
		}{
			{
				name: "Point", types: []int32{1},
				wkb: wkb(o, 1, 1.5, -2.5),
				x:   bounds{1.5, 1.5}, y: bounds{-2.5, -2.5},
			},
			{
				name: "LineString", types: []int32{2},
				wkb: wkb(o, 2, uint32(3), 0.0, 0.0, 10.0, -1.0, 5.0, 20.0),
				x:   bounds{0, 10}, y: bounds{-1, 20},
			},
			{
				// the hole is inside the shell, so only the shell sets the extent
				name: "Polygon with hole", types: []int32{3},
				wkb: wkb(o, 3, uint32(2),
					uint32(5), 0.0, 0.0, 10.0, 0.0, 10.0, 10.0, 0.0, 10.0, 0.0, 0.0,
					uint32(4), 2.0, 2.0, 3.0, 2.0, 3.0, 3.0, 2.0, 2.0),
				x: bounds{0, 10}, y: bounds{0, 10},
			},
			{
				name: "MultiPoint", types: []int32{4},
				wkb: wkb(o, 4, uint32(2), wkb(o, 1, -1.0, -1.0), wkb(o, 1, 1.0, 1.0)),
				x:   bounds{-1, 1}, y: bounds{-1, 1},
			},
			{
				name: "MultiLineString", types: []int32{5},
				wkb: wkb(o, 5, uint32(2),
					wkb(o, 2, uint32(2), 0.0, 0.0, 1.0, 1.0),
					wkb(o, 2, uint32(2), 5.0, 5.0, 6.0, -6.0)),
				x: bounds{0, 6}, y: bounds{-6, 5},
			},
			{
				name: "MultiPolygon", types: []int32{6},
				wkb: wkb(o, 6, uint32(2),
					wkb(o, 3, uint32(1), uint32(4), 0.0, 0.0, 4.0, 0.0, 4.0, 4.0, 0.0, 0.0),
					wkb(o, 3, uint32(1), uint32(4), -8.0, -8.0, -7.0, -8.0, -7.0, -7.0, -8.0, -8.0)),
				x: bounds{-8, 4}, y: bounds{-8, 4},
			},
			{
				name: "GeometryCollection", types: []int32{7},
				wkb: wkb(o, 7, uint32(2),
					wkb(o, 1, 9.0, 9.0),
					wkb(o, 2, uint32(2), -9.0, 0.0, 0.0, 0.0)),
				x: bounds{-9, 9}, y: bounds{0, 9},
			},
		}

		for _, tt := range tests {
			name := fmt.Sprintf("%s/%v", tt.name, order)
			b := geospatial.NewBounder()
			b.Update(tt.wkb)
			require.True(t, b.Valid(), name)
			assert.Equal(t, tt.types, b.Types(), name)
			assertDim(t, b, geospatial.DimX, &tt.x, name)
			assertDim(t, b, geospatial.DimY, &tt.y, name)
			assertDim(t, b, geospatial.DimZ, nil, name)
			assertDim(t, b, geospatial.DimM, nil, name)
		}
	}
}

func TestBounderDimensions(t *testing.T) {
	le, be := byteOrder(binary.LittleEndian), byteOrder(binary.BigEndian)

	tests := []struct {
		name  string
		wkb   []byte
		types []int32
		z, m  *bounds
	}{
		{
			name: "ISO Point Z", types: []int32{1001},
			wkb: wkb(le, 1001, 1.0, 2.0, 3.0),
			z:   &bounds{3, 3},
		},
		{
			// the third ordinate of an M geometry is M, not Z
			name: "ISO Point M", types: []int32{2001},
			wkb: wkb(le, 2001, 1.0, 2.0, 3.0),
			m:   &bounds{3, 3},
		},
		{
			name: "ISO Point ZM", types: []int32{3001},
			wkb: wkb(le, 3001, 1.0, 2.0, 3.0, 4.0),
			z:   &bounds{3, 3}, m: &bounds{4, 4},
		},
		{
			name: "ISO LineString ZM", types: []int32{3002},
			wkb: wkb(be, 3002, uint32(2), 0.0, 0.0, -1.0, 10.0, 1.0, 1.0, 1.0, 20.0),
			z:   &bounds{-1, 1}, m: &bounds{10, 20},
		},
		{
			name: "EWKB M flag", types: []int32{2001},
			wkb: wkb(le, 1|ewkbM, 1.0, 2.0, 5.0),
			m:   &bounds{5, 5},
		},
		{
			name: "EWKB Z and M flags", types: []int32{3001},
			wkb: wkb(le, 1|ewkbZ|ewkbM, 1.0, 2.0, 3.0, 4.0),
			z:   &bounds{3, 3}, m: &bounds{4, 4},
		},
		{
			name: "EWKB Z flag on LineString", types: []int32{1002},
			wkb: wkb(be, 2|ewkbZ, uint32(1), 1.0, 2.0, 3.0),
			z:   &bounds{3, 3},
		},
		{
			// the SRID is skipped and must not be read as a coordinate
			name: "EWKB SRID", types: []int32{1},
			wkb: wkb(le, 1|ewkbSRID, uint32(4326), 1.0, 2.0),
		},
		{
			name: "EWKB SRID and Z", types: []int32{1001},
			wkb: wkb(le, 1|ewkbZ|ewkbSRID, uint32(4326), 1.0, 2.0, 3.0),
			z:   &bounds{3, 3},
		},
		{
			// an EWKB flag on an ISO Z code adds nothing new
			name: "EWKB Z flag on ISO Z code", types: []int32{1001},
			wkb: wkb(le, 1001|ewkbZ, 1.0, 2.0, 3.0),
			z:   &bounds{3, 3},
		},
		{
			// only the top-level type is recorded, but nested Z
			// coordinates still count towards the Z bounds
			name: "Point Z inside 2D collection", types: []int32{7},
			wkb: wkb(le, 7, uint32(1), wkb(le, 1001, 1.0, 2.0, 3.0)),
			z:   &bounds{3, 3},
		},
	}

	for _, tt := range tests {
		b := geospatial.NewBounder()
		b.Update(tt.wkb)
		require.True(t, b.Valid(), tt.name)
		assert.Equal(t, tt.types, b.Types(), tt.name)
		assert.True(t, b.HasDim(geospatial.DimX), tt.name)
		assertDim(t, b, geospatial.DimZ, tt.z, tt.name)
		assertDim(t, b, geospatial.DimM, tt.m, tt.name)
	}
}

func TestBounderEmptyGeometries(t *testing.T) {
	le := byteOrder(binary.LittleEndian)
	nan := math.NaN()

	// each is valid and records its type, but has no extent
	for _, tt := range []struct {
		name  string
		wkb   []byte
		types []int32
	}{
		{"LineString EMPTY", wkb(le, 2, uint32(0)), []int32{2}},
		{"Polygon EMPTY", wkb(le, 3, uint32(0)), []int32{3}},
		{"Polygon with an empty ring", wkb(le, 3, uint32(1), uint32(0)), []int32{3}},
		{"MultiPoint EMPTY", wkb(le, 4, uint32(0)), []int32{4}},
		{"MultiPolygon EMPTY", wkb(le, 6, uint32(0)), []int32{6}},
		{"GeometryCollection EMPTY", wkb(le, 7, uint32(0)), []int32{7}},
		{"Point Z EMPTY", wkb(le, 1001, nan, nan, nan), []int32{1001}},
		{"Point ZM EMPTY", wkb(le, 3001, nan, nan, nan, nan), []int32{3001}},
		{"MultiPoint of POINT EMPTY", wkb(le, 4, uint32(1), wkb(le, 1, nan, nan)), []int32{4}},
	} {
		b := geospatial.NewBounder()
		b.Update(tt.wkb)
		require.True(t, b.Valid(), tt.name)
		assert.False(t, b.Empty(), tt.name)
		assert.Equal(t, tt.types, b.Types(), tt.name)
		for dim := range 4 {
			assertDim(t, b, dim, nil, tt.name)
		}
	}

	// an empty point next to a real one adds nothing to the extent
	b := geospatial.NewBounder()
	b.Update(wkb(le, 4, uint32(2), wkb(le, 1, nan, nan), wkb(le, 1, 3.0, 4.0)))
	require.True(t, b.Valid())
	assertDim(t, b, geospatial.DimX, &bounds{3, 3}, "mixed")
	assertDim(t, b, geospatial.DimY, &bounds{4, 4}, "mixed")
}

func TestBounderInvalidEdgeCases(t *testing.T) {
	le := byteOrder(binary.LittleEndian)
	nan := math.NaN()

	for name, v := range map[string][]byte{
		// NaN is only allowed as a whole empty point
		"NaN in LineString":      wkb(le, 2, uint32(1), nan, nan),
		"NaN in Polygon ring":    wkb(le, 3, uint32(1), uint32(1), 1.0, nan),
		"NaN only in Z":          wkb(le, 1001, 1.0, 2.0, nan),
		"NaN only in M":          wkb(le, 3001, 1.0, 2.0, 3.0, nan),
		"partial NaN child":      wkb(le, 4, uint32(1), wkb(le, 1, nan, 1.0)),
		"Point with extra coord": wkb(le, 1, 1.0, 2.0, 3.0),
		"Point Z missing Z":      wkb(le, 1001, 1.0, 2.0),

		// unsupported type codes
		"type 0 (Geometry)":           wkb(le, 0, 1.0, 2.0),
		"type 8":                      wkb(le, 8, uint32(0)),
		"type 15 (PolyhedralSurface)": wkb(le, 15, uint32(0)),
		"type 17 (TIN)":               wkb(le, 17, uint32(0)),
		"dimension offset 4000":       wkb(le, 4001, 1.0, 2.0),

		// headers and counts
		"only byte order":         {1},
		"truncated type":          {1, 1, 0},
		"truncated SRID":          append(wkb(le, 1|ewkbSRID), 0xE6, 0x10),
		"missing count":           wkb(le, 2),
		"huge ring point count":   wkb(le, 3, uint32(1), uint32(math.MaxUint32)),
		"huge MultiPoint count":   wkb(le, 4, uint32(math.MaxUint32)),
		"fewer points than count": wkb(le, 2, uint32(3), 0.0, 0.0, 1.0, 1.0),
		"fewer rings than count":  wkb(le, 3, uint32(2), uint32(1), 0.0, 0.0),

		// problems inside a child
		"truncated child":           wkb(le, 7, uint32(1), wkb(le, 1, 1.0)),
		"fewer children than count": wkb(le, 7, uint32(2), wkb(le, 1, 1.0, 1.0)),
		"bad child byte order":      wkb(le, 7, uint32(1), []byte{0xFF}, wkb(le, 1, 1.0, 1.0)[1:]),
		"bad child type":            wkb(le, 7, uint32(1), wkb(le, 99, 1.0, 1.0)),
		"trailing after children":   append(wkb(le, 7, uint32(1), wkb(le, 1, 1.0, 1.0)), 0),
	} {
		b := geospatial.NewBounder()
		b.Update(v)
		assert.False(t, b.Valid(), name)
	}
}

func TestBounderMixedByteOrders(t *testing.T) {
	le, be := byteOrder(binary.LittleEndian), byteOrder(binary.BigEndian)

	// each child declares its own byte order
	b := geospatial.NewBounder()
	b.Update(wkb(be, 4, uint32(2), wkb(le, 1, 1.0, -1.0), wkb(be, 1, 2.0, -2.0)))
	require.True(t, b.Valid())
	assert.Equal(t, []int32{4}, b.Types())
	assertDim(t, b, geospatial.DimX, &bounds{1, 2}, "x")
	assertDim(t, b, geospatial.DimY, &bounds{-2, -1}, "y")
}

func TestBounderEveryTruncationIsInvalid(t *testing.T) {
	le, be := byteOrder(binary.LittleEndian), byteOrder(binary.BigEndian)

	full := wkb(le, 7, uint32(3),
		wkb(be, 3, uint32(1), uint32(4), 0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 0.0),
		wkb(le, 1002, uint32(2), 0.0, 0.0, 0.0, 1.0, 1.0, 1.0),
		wkb(le, 1|ewkbSRID, uint32(4326), 5.0, 5.0))

	b := geospatial.NewBounder()
	b.Update(full)
	require.True(t, b.Valid())

	for n := range len(full) {
		b := geospatial.NewBounder()
		b.Update(full[:n])
		assert.False(t, b.Valid(), "prefix of %d/%d bytes", n, len(full))
	}
}

func TestBounderNestingDepth(t *testing.T) {
	le := byteOrder(binary.LittleEndian)

	// a point wrapped in n single-element collections
	nested := func(n int) []byte {
		g := wkb(le, 1, 1.0, 2.0)
		for range n {
			g = wkb(le, 7, uint32(1), g)
		}
		return g
	}

	b := geospatial.NewBounder()
	b.Update(nested(64))
	assert.True(t, b.Valid(), "64 levels of nesting is the limit")
	assertDim(t, b, geospatial.DimX, &bounds{1, 1}, "x")

	b = geospatial.NewBounder()
	b.Update(nested(65))
	assert.False(t, b.Valid(), "65 levels of nesting is too deep")
}

func TestBounderAccumulation(t *testing.T) {
	le := byteOrder(binary.LittleEndian)

	// types are deduplicated and sorted, bounds cover every value
	b := geospatial.NewBounder()
	b.Update(wkb(le, 1001, 5.0, 5.0, 5.0))
	b.Update(wkb(le, 1, -1.0, 0.0))
	b.Update(wkb(le, 7, uint32(0)))
	b.Update(wkb(le, 1, 10.0, 0.0))
	require.True(t, b.Valid())
	assert.Equal(t, []int32{1, 7, 1001}, b.Types())
	assertDim(t, b, geospatial.DimX, &bounds{-1, 10}, "x")
	assertDim(t, b, geospatial.DimZ, &bounds{5, 5}, "z")

	// once invalid, later values are ignored
	types := b.Types()
	b.Update([]byte("not wkb"))
	b.Update(wkb(le, 2, uint32(1), 100.0, 100.0))
	assert.False(t, b.Valid())
	assert.Equal(t, types, b.Types())
}

func TestBounderMerge(t *testing.T) {
	le := byteOrder(binary.LittleEndian)

	a := geospatial.NewBounder()
	a.Update(wkb(le, 1, 0.0, 0.0))
	a.Update(wkb(le, 1, 1.0, 1.0))

	z := geospatial.NewBounder()
	z.Update(wkb(le, 1001, -5.0, 10.0, 3.0))

	a.Merge(z)
	require.True(t, a.Valid())
	assert.Equal(t, []int32{1, 1001}, a.Types())
	assertDim(t, a, geospatial.DimX, &bounds{-5, 1}, "x")
	assertDim(t, a, geospatial.DimY, &bounds{0, 10}, "y")
	assertDim(t, a, geospatial.DimZ, &bounds{3, 3}, "z")
	assertDim(t, a, geospatial.DimM, nil, "m")

	// merging an empty Bounder changes nothing
	before := a.Types()
	a.Merge(geospatial.NewBounder())
	assert.Equal(t, before, a.Types())
	assertDim(t, a, geospatial.DimX, &bounds{-5, 1}, "x after empty merge")

	// merging into an empty Bounder copies everything
	empty := geospatial.NewBounder()
	empty.Merge(a)
	assert.Equal(t, a.Types(), empty.Types())
	for dim := range 4 {
		assert.Equal(t, a.HasDim(dim), empty.HasDim(dim))
		if a.HasDim(dim) {
			assert.Equal(t, a.Min(dim), empty.Min(dim))
			assert.Equal(t, a.Max(dim), empty.Max(dim))
		}
	}
}

func TestNewBounderFrom(t *testing.T) {
	inf := math.Inf(1)
	min := [4]float64{0, 1, inf, 7}
	max := [4]float64{10, 2, -inf, 7}

	b := geospatial.NewBounderFrom(min, max, []int32{3001, 1, 1})
	assert.True(t, b.Valid())
	assert.False(t, b.Empty())
	assert.Equal(t, []int32{1, 3001}, b.Types())
	assertDim(t, b, geospatial.DimX, &bounds{0, 10}, "x")
	assertDim(t, b, geospatial.DimY, &bounds{1, 2}, "y")
	assertDim(t, b, geospatial.DimZ, nil, "z (min > max)")
	assertDim(t, b, geospatial.DimM, &bounds{7, 7}, "m (min == max)")

	assert.True(t, geospatial.NewBounderFrom(min, max, nil).Empty())
}
