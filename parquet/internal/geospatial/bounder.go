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

// Package geospatial computes the bounding box and geometry type set of
// WKB-encoded geometries for Parquet geospatial statistics. It has no
// dependency on any geometry library: it only walks WKB coordinates.
package geospatial

import (
	"encoding/binary"
	"math"
	"slices"
)

// Coordinate dimensions, in the order they are indexed in a Bounder.
const (
	DimX = iota
	DimY
	DimZ
	DimM
	numDims
)

const (
	ewkbZ    = 0x80000000
	ewkbM    = 0x40000000
	ewkbSRID = 0x20000000

	maxNesting = 64
)

// Bounder accumulates the per-dimension min/max and the set of ISO WKB geometry
// type codes of the geometries it is given. If any value is not valid WKB, or
// contains a NaN coordinate (other than an empty point), the Bounder becomes
// invalid and its results must not be written.
//
// The zero value is not usable, use NewBounder.
type Bounder struct {
	min, max [numDims]float64
	types    map[int32]struct{}
	invalid  bool
}

// NewBounder returns an empty, valid Bounder.
func NewBounder() *Bounder {
	b := &Bounder{}
	b.Reset()
	return b
}

// NewBounderFrom builds a Bounder from previously computed results, such as
// those read back from a file footer. A dimension is considered present
// when min <= max.
func NewBounderFrom(min, max [4]float64, types []int32) *Bounder {
	b := &Bounder{min: min, max: max, types: make(map[int32]struct{}, len(types))}
	for _, t := range types {
		b.types[t] = struct{}{}
	}
	return b
}

// Reset clears all accumulated state and makes the Bounder valid again.
func (b *Bounder) Reset() {
	for i := range b.min {
		b.min[i], b.max[i] = math.Inf(1), math.Inf(-1)
	}
	b.types = make(map[int32]struct{})
	b.invalid = false
}

// Invalidate marks the results as unusable, for example when values were
// written that cannot be inspected.
func (b *Bounder) Invalidate() { b.invalid = true }

// Valid reports whether every value seen so far was valid WKB.
func (b *Bounder) Valid() bool { return !b.invalid }

// HasDim reports whether at least one coordinate was seen in dimension dim.
func (b *Bounder) HasDim(dim int) bool { return b.min[dim] <= b.max[dim] }

// Min returns the minimum for dim, only meaningful if HasDim(dim).
func (b *Bounder) Min(dim int) float64 { return b.min[dim] }

// Max returns the maximum for dim, only meaningful if HasDim(dim).
func (b *Bounder) Max(dim int) float64 { return b.max[dim] }

// Types returns the sorted ISO WKB geometry type codes seen
// (e.g. 1 = Point, 1001 = Point Z, 2003 = LineString M, 3001 ...).
func (b *Bounder) Types() []int32 {
	out := make([]int32, 0, len(b.types))
	for t := range b.types {
		out = append(out, t)
	}
	slices.Sort(out)
	return out
}

// Empty reports whether no geometry has been seen at all.
func (b *Bounder) Empty() bool { return len(b.types) == 0 }

// Merge folds other into b.
func (b *Bounder) Merge(other *Bounder) {
	if other.invalid {
		b.invalid = true
	}
	for i := range b.min {
		b.min[i] = math.Min(b.min[i], other.min[i])
		b.max[i] = math.Max(b.max[i], other.max[i])
	}
	for t := range other.types {
		b.types[t] = struct{}{}
	}
}

// Update adds a single WKB-encoded geometry.
func (b *Bounder) Update(wkb []byte) {
	if b.invalid {
		return
	}

	// a failed parse may have partially updated the bounds, but the
	// invalid flag means they are never used.
	r := reader{buf: wkb}
	code, ok := b.readGeometry(&r, 0)
	if !ok || len(r.buf) != 0 {
		b.invalid = true
		return
	}
	b.types[code] = struct{}{}
}

type reader struct {
	buf []byte
}

func (r *reader) u8() (byte, bool) {
	if len(r.buf) < 1 {
		return 0, false
	}
	v := r.buf[0]
	r.buf = r.buf[1:]
	return v, true
}

func (r *reader) u32(order binary.ByteOrder) (uint32, bool) {
	if len(r.buf) < 4 {
		return 0, false
	}
	v := order.Uint32(r.buf)
	r.buf = r.buf[4:]
	return v, true
}

func (b *Bounder) readGeometry(r *reader, depth int) (isoCode int32, ok bool) {
	if depth > maxNesting {
		return 0, false
	}

	bo, ok := r.u8()
	if !ok {
		return 0, false
	}
	var order binary.ByteOrder
	switch bo {
	case 0:
		order = binary.BigEndian
	case 1:
		order = binary.LittleEndian
	default:
		return 0, false
	}

	raw, ok := r.u32(order)
	if !ok {
		return 0, false
	}

	// accept both ISO (1000/2000/3000 offsets) and EWKB (high bit flags)
	hasZ, hasM := raw&ewkbZ != 0, raw&ewkbM != 0
	if raw&ewkbSRID != 0 {
		if _, ok := r.u32(order); !ok {
			return 0, false
		}
	}
	code := raw &^ (ewkbZ | ewkbM | ewkbSRID)
	switch code / 1000 {
	case 0:
	case 1:
		hasZ = true
	case 2:
		hasM = true
	case 3:
		hasZ, hasM = true, true
	default:
		return 0, false
	}
	base := code % 1000

	var dims [numDims]int
	n := 0
	dims[n], n = DimX, n+1
	dims[n], n = DimY, n+1
	if hasZ {
		dims[n], n = DimZ, n+1
	}
	if hasM {
		dims[n], n = DimM, n+1
	}
	ndims := dims[:n]

	isoCode = int32(base)
	if hasZ {
		isoCode += 1000
	}
	if hasM {
		isoCode += 2000
	}

	switch base {
	case 1: // Point
		return isoCode, b.readCoords(r, order, ndims, 1, true)
	case 2: // LineString
		cnt, ok := r.u32(order)
		return isoCode, ok && b.readCoords(r, order, ndims, uint64(cnt), false)
	case 3: // Polygon
		rings, ok := r.u32(order)
		if !ok || uint64(rings)*4 > uint64(len(r.buf)) {
			return 0, false
		}
		for i := uint32(0); i < rings; i++ {
			cnt, ok := r.u32(order)
			if !ok || !b.readCoords(r, order, ndims, uint64(cnt), false) {
				return 0, false
			}
		}
		return isoCode, true
	case 4, 5, 6, 7: // Multi* and GeometryCollection
		cnt, ok := r.u32(order)
		// every child is at least 5 bytes
		if !ok || uint64(cnt)*5 > uint64(len(r.buf)) {
			return 0, false
		}
		for range cnt {
			if _, ok := b.readGeometry(r, depth+1); !ok {
				return 0, false
			}
		}
		return isoCode, true
	}
	return 0, false
}

func (b *Bounder) readCoords(r *reader, order binary.ByteOrder, dims []int, count uint64, allowEmpty bool) bool {
	size := uint64(len(dims)) * 8
	if count*size > uint64(len(r.buf)) {
		return false
	}

	var coord [numDims]float64
	for i := uint64(0); i < count; i++ {
		nan := 0
		for j := range dims {
			coord[j] = math.Float64frombits(order.Uint64(r.buf))
			r.buf = r.buf[8:]
			if math.IsNaN(coord[j]) {
				nan++
			}
		}

		switch {
		case nan == 0:
			for j, d := range dims {
				b.min[d] = math.Min(b.min[d], coord[j])
				b.max[d] = math.Max(b.max[d], coord[j])
			}
		case nan == len(dims) && allowEmpty:
			// POINT EMPTY is encoded as all-NaN, it contributes no extent
		default:
			return false
		}
	}
	return true
}
