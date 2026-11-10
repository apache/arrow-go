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

package metadata

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wkbPoint encodes a little-endian ISO WKB point with the given type code
// (1 = XY, 1001 = XYZ, 2001 = XYM, 3001 = XYZM) and coordinates.
func wkbPoint(code uint32, coords ...float64) parquet.ByteArray {
	out := []byte{1}
	out = binary.LittleEndian.AppendUint32(out, code)
	for _, c := range coords {
		out = binary.LittleEndian.AppendUint64(out, math.Float64bits(c))
	}
	return out
}

func newGeoStats(values ...parquet.ByteArray) *GeoStatistics {
	g := NewGeoStatistics()
	g.Update(values)
	return g
}

// assertSameGeoStats checks that every accessor of got matches want.
func assertSameGeoStats(t *testing.T, want, got *GeoStatistics) {
	t.Helper()
	assert.Equal(t, want.Valid(), got.Valid(), "Valid")
	assert.Equal(t, want.IsEmpty(), got.IsEmpty(), "IsEmpty")
	assert.Equal(t, want.GeometryTypes(), got.GeometryTypes(), "GeometryTypes")
	require.Equal(t, want.HasBoundingBox(), got.HasBoundingBox(), "HasBoundingBox")
	if want.HasBoundingBox() {
		assert.Equal(t, []float64{want.XMin(), want.XMax(), want.YMin(), want.YMax()},
			[]float64{got.XMin(), got.XMax(), got.YMin(), got.YMax()}, "XY bounds")
	}
	require.Equal(t, want.HasZ(), got.HasZ(), "HasZ")
	if want.HasZ() {
		assert.Equal(t, []float64{want.ZMin(), want.ZMax()}, []float64{got.ZMin(), got.ZMax()}, "Z bounds")
	}
	require.Equal(t, want.HasM(), got.HasM(), "HasM")
	if want.HasM() {
		assert.Equal(t, []float64{want.MMin(), want.MMax()}, []float64{got.MMin(), got.MMax()}, "M bounds")
	}
}

func ptr(v float64) *float64 { return &v }

func TestGeoStatisticsThriftRoundTrip(t *testing.T) {
	tests := []struct {
		name       string
		values     []parquet.ByteArray
		types      []int32
		xy         [4]float64 // xmin, xmax, ymin, ymax
		zmin, zmax *float64
		mmin, mmax *float64
	}{
		{
			name:   "XY",
			values: []parquet.ByteArray{wkbPoint(1, -1, 10), wkbPoint(1, 3, -2)},
			types:  []int32{1},
			xy:     [4]float64{-1, 3, -2, 10},
		},
		{
			name:   "XYZ",
			values: []parquet.ByteArray{wkbPoint(1001, 1, 2, 30), wkbPoint(1001, 4, 5, -6)},
			types:  []int32{1001},
			xy:     [4]float64{1, 4, 2, 5},
			zmin:   ptr(-6), zmax: ptr(30),
		},
		{
			name:   "XYM",
			values: []parquet.ByteArray{wkbPoint(2001, 1, 2, 7), wkbPoint(2001, 0, 0, 9)},
			types:  []int32{2001},
			xy:     [4]float64{0, 1, 0, 2},
			mmin:   ptr(7), mmax: ptr(9),
		},
		{
			name:   "XYZM",
			values: []parquet.ByteArray{wkbPoint(3001, 1, 2, 3, 4), wkbPoint(3001, 5, 6, 7, 8)},
			types:  []int32{3001},
			xy:     [4]float64{1, 5, 2, 6},
			zmin:   ptr(3), zmax: ptr(7),
			mmin: ptr(4), mmax: ptr(8),
		},
		{
			// Z bounds come only from the Z point, the XY point still
			// contributes to the XY bounds.
			name:   "mixed XY and XYZ",
			values: []parquet.ByteArray{wkbPoint(1, 100, 100), wkbPoint(1001, 1, 2, 3)},
			types:  []int32{1, 1001},
			xy:     [4]float64{1, 100, 2, 100},
			zmin:   ptr(3), zmax: ptr(3),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := newGeoStats(tt.values...)

			th := g.toThrift()
			require.NotNil(t, th)
			assert.Equal(t, tt.types, th.GetGeospatialTypes())
			bb := th.GetBbox()
			require.NotNil(t, bb)
			assert.Equal(t, tt.xy, [4]float64{bb.GetXmin(), bb.GetXmax(), bb.GetYmin(), bb.GetYmax()})
			assert.Equal(t, tt.zmin, bb.Zmin)
			assert.Equal(t, tt.zmax, bb.Zmax)
			assert.Equal(t, tt.mmin, bb.Mmin)
			assert.Equal(t, tt.mmax, bb.Mmax)

			assertSameGeoStats(t, g, geoStatisticsFromThrift(th))
		})
	}
}

func TestGeoStatisticsOnlyEmptyPoints(t *testing.T) {
	// POINT EMPTY is encoded with NaN coordinates. It records a geometry type
	// but contributes no extent, so the thrift form has types and no bbox.
	g := newGeoStats(wkbPoint(1, math.NaN(), math.NaN()))
	assert.False(t, g.IsEmpty())
	assert.False(t, g.HasBoundingBox())

	th := g.toThrift()
	require.NotNil(t, th)
	assert.Equal(t, []int32{1}, th.GetGeospatialTypes())
	assert.Nil(t, th.GetBbox())

	back := geoStatisticsFromThrift(th)
	assertSameGeoStats(t, g, back)
}

func TestGeoStatisticsToThriftOmitted(t *testing.T) {
	t.Run("nothing written", func(t *testing.T) {
		g := NewGeoStatistics()
		assert.True(t, g.Valid())
		assert.True(t, g.IsEmpty())
		assert.Nil(t, g.toThrift())
	})

	t.Run("invalid WKB", func(t *testing.T) {
		g := newGeoStats(wkbPoint(1, 1, 2), parquet.ByteArray("not wkb"))
		assert.False(t, g.Valid())
		assert.Nil(t, g.toThrift())
	})

	t.Run("NaN in a non-empty geometry", func(t *testing.T) {
		g := newGeoStats(wkbPoint(1, 1, math.NaN()))
		assert.False(t, g.Valid())
		assert.Nil(t, g.toThrift())
	})

	t.Run("invalidated", func(t *testing.T) {
		g := newGeoStats(wkbPoint(1, 1, 2))
		g.Invalidate()
		assert.False(t, g.Valid())
		assert.Nil(t, g.toThrift())
	})
}

func TestGeoStatisticsReset(t *testing.T) {
	g := newGeoStats(wkbPoint(1, 1, 2), parquet.ByteArray("not wkb"))
	require.False(t, g.Valid())

	g.Reset()
	assert.True(t, g.Valid())
	assert.True(t, g.IsEmpty())
	assert.False(t, g.HasBoundingBox())

	g.UpdateWKB(wkbPoint(1, 5, 6))
	assertSameGeoStats(t, newGeoStats(wkbPoint(1, 5, 6)), g)
}

func TestGeoStatisticsMerge(t *testing.T) {
	a := newGeoStats(wkbPoint(1, 0, 0), wkbPoint(1, 1, 1))
	b := newGeoStats(wkbPoint(1001, -5, 10, 3))

	a.Merge(b)
	assertSameGeoStats(t, newGeoStats(
		wkbPoint(1, 0, 0), wkbPoint(1, 1, 1), wkbPoint(1001, -5, 10, 3)), a)

	// merging in invalid statistics makes the result invalid
	bad := newGeoStats(parquet.ByteArray("not wkb"))
	a.Merge(bad)
	assert.False(t, a.Valid())
	assert.Nil(t, a.toThrift())
}

func TestGeoStatisticsUpdateSpaced(t *testing.T) {
	// The null slots hold bytes that are not WKB, so the statistics would
	// become invalid if they were read.
	garbage := parquet.ByteArray("garbage")
	values := []parquet.ByteArray{wkbPoint(1, 1, 2), garbage, wkbPoint(1, 3, 4), garbage}
	want := newGeoStats(wkbPoint(1, 1, 2), wkbPoint(1, 3, 4))

	t.Run("no offset", func(t *testing.T) {
		g := NewGeoStatistics()
		g.UpdateSpaced(values, []byte{0b0101}, 0)
		assertSameGeoStats(t, want, g)
	})

	t.Run("with offset", func(t *testing.T) {
		// the same validity, starting at bit 3
		g := NewGeoStatistics()
		g.UpdateSpaced(values, []byte{0b0101 << 3}, 3)
		assertSameGeoStats(t, want, g)
	})

	t.Run("nil validity", func(t *testing.T) {
		g := NewGeoStatistics()
		g.UpdateSpaced(values[:1], nil, 0)
		assertSameGeoStats(t, newGeoStats(wkbPoint(1, 1, 2)), g)
	})
}

func convertOffsets[T ~int32 | ~int64](offsets []int) []T {
	out := make([]T, len(offsets))
	for i, o := range offsets {
		out[i] = T(o)
	}
	return out
}

// testArrowOffsets expects the second of three values to be null.
func testArrowOffsets[T ~int32 | ~int64](t *testing.T, data []byte, offsets []T, want *GeoStatistics) {
	t.Run("no offset", func(t *testing.T) {
		g := NewGeoStatistics()
		UpdateGeoStatisticsFromArrowOffsets(g, data, offsets, []byte{0b101}, 0)
		assertSameGeoStats(t, want, g)
	})
	t.Run("with offset", func(t *testing.T) {
		// the same validity, starting at bit 2
		g := NewGeoStatistics()
		UpdateGeoStatisticsFromArrowOffsets(g, data, offsets, []byte{0b101 << 2}, 2)
		assertSameGeoStats(t, want, g)
	})
}

func TestUpdateGeoStatisticsFromArrowOffsets(t *testing.T) {
	a, b := wkbPoint(1, 1, 2), wkbPoint(1, 3, 4)
	garbage := []byte("bad")
	var data []byte
	data = append(data, a...)
	data = append(data, garbage...)
	data = append(data, b...)
	offsets := []int{0, len(a), len(a) + len(garbage), len(data)}
	want := newGeoStats(a, b)

	t.Run("int32", func(t *testing.T) { testArrowOffsets(t, data, convertOffsets[int32](offsets), want) })
	t.Run("int64", func(t *testing.T) { testArrowOffsets(t, data, convertOffsets[int64](offsets), want) })

	t.Run("nil validity", func(t *testing.T) {
		g := NewGeoStatistics()
		UpdateGeoStatisticsFromArrowOffsets(g, a, []int32{0, int32(len(a))}, nil, 0)
		assertSameGeoStats(t, newGeoStats(a), g)
	})

	t.Run("fewer than two offsets", func(t *testing.T) {
		g := NewGeoStatistics()
		UpdateGeoStatisticsFromArrowOffsets(g, data, []int32{0}, nil, 0)
		UpdateGeoStatisticsFromArrowOffsets(g, data, []int32{}, nil, 0)
		assert.True(t, g.Valid())
		assert.True(t, g.IsEmpty())
	})
}
