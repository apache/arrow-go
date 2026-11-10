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
	"math"

	"github.com/apache/arrow-go/v18/internal/bitutils"
	"github.com/apache/arrow-go/v18/parquet"
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/geospatial"
)

// GeoStatistics holds the geospatial statistics of a GEOMETRY column chunk:
// the bounding box of all coordinates and the set of geometry types (as ISO
// WKB codes, e.g. 1 = Point, 1002 = LineString Z) that were written.
//
// If any value was not valid WKB, or had a NaN coordinate, the statistics are
// invalid and are never written to the file.
type GeoStatistics struct {
	b *geospatial.Bounder
}

// NewGeoStatistics returns empty, valid geospatial statistics.
func NewGeoStatistics() *GeoStatistics {
	return &GeoStatistics{b: geospatial.NewBounder()}
}

// Reset clears the statistics.
func (g *GeoStatistics) Reset() { g.b.Reset() }

// Invalidate marks the statistics as unusable, e.g. when values were written
// without being inspected.
func (g *GeoStatistics) Invalidate() { g.b.Invalidate() }

// Merge folds other into g.
func (g *GeoStatistics) Merge(other *GeoStatistics) { g.b.Merge(other.b) }

// Valid reports whether the statistics can be trusted.
func (g *GeoStatistics) Valid() bool { return g.b.Valid() }

// IsEmpty reports whether no non-null geometry was seen.
func (g *GeoStatistics) IsEmpty() bool { return g.b.Empty() }

// HasBoundingBox reports whether any coordinate was seen. It is false for,
// e.g., a chunk consisting only of empty points.
func (g *GeoStatistics) HasBoundingBox() bool { return g.b.HasDim(geospatial.DimX) }

// XMin, XMax, YMin and YMax are the bounds of the X and Y coordinates, only
// meaningful if HasBoundingBox is true.
func (g *GeoStatistics) XMin() float64 { return g.b.Min(geospatial.DimX) }
func (g *GeoStatistics) XMax() float64 { return g.b.Max(geospatial.DimX) }
func (g *GeoStatistics) YMin() float64 { return g.b.Min(geospatial.DimY) }
func (g *GeoStatistics) YMax() float64 { return g.b.Max(geospatial.DimY) }

// HasZ and HasM report whether any Z or M coordinates were seen.
func (g *GeoStatistics) HasZ() bool { return g.b.HasDim(geospatial.DimZ) }
func (g *GeoStatistics) HasM() bool { return g.b.HasDim(geospatial.DimM) }

// ZMin, ZMax, MMin and MMax are only meaningful if HasZ/HasM is true.
func (g *GeoStatistics) ZMin() float64 { return g.b.Min(geospatial.DimZ) }
func (g *GeoStatistics) ZMax() float64 { return g.b.Max(geospatial.DimZ) }
func (g *GeoStatistics) MMin() float64 { return g.b.Min(geospatial.DimM) }
func (g *GeoStatistics) MMax() float64 { return g.b.Max(geospatial.DimM) }

// GeometryTypes returns the sorted ISO WKB geometry type codes that were seen.
func (g *GeoStatistics) GeometryTypes() []int32 { return g.b.Types() }

// UpdateWKB adds a single non-null WKB value.
func (g *GeoStatistics) UpdateWKB(wkb []byte) { g.b.Update(wkb) }

// Update adds a slice of non-null WKB values.
func (g *GeoStatistics) Update(values []parquet.ByteArray) {
	for _, v := range values {
		g.b.Update(v)
	}
}

// UpdateSpaced adds values where validBits marks which entries are non-null.
func (g *GeoStatistics) UpdateSpaced(values []parquet.ByteArray, validBits []byte, validBitsOffset int64) {
	if validBits == nil {
		g.Update(values)
		return
	}
	bitutils.VisitSetBitRunsNoErr(validBits, validBitsOffset, int64(len(values)), func(pos, length int64) {
		g.Update(values[pos : pos+length])
	})
}

// UpdateGeoStatisticsFromArrowOffsets adds the values described by an Arrow value buffer
// and offsets (32 or 64-bit). validBits may be nil if there are no nulls.
func UpdateGeoStatisticsFromArrowOffsets[T ~int32 | ~int64](g *GeoStatistics, values []byte, offsets []T, validBits []byte, validBitsOffset int64) {
	if len(offsets) < 2 {
		return
	}
	n := int64(len(offsets) - 1)
	visit := func(pos, length int64) {
		for i := pos; i < pos+length; i++ {
			g.b.Update(values[offsets[i]:offsets[i+1]])
		}
	}
	if validBits == nil {
		visit(0, n)
		return
	}
	bitutils.VisitSetBitRunsNoErr(validBits, validBitsOffset, n, visit)
}

// toThrift returns the thrift form, or nil if the statistics are invalid or
// nothing was written.
func (g *GeoStatistics) toThrift() *format.GeospatialStatistics {
	if !g.Valid() || g.IsEmpty() {
		return nil
	}

	ret := format.NewGeospatialStatistics()
	ret.GeospatialTypes = g.GeometryTypes()
	if !g.HasBoundingBox() {
		return ret
	}

	bbox := format.NewBoundingBox()
	bbox.Xmin, bbox.Xmax = g.XMin(), g.XMax()
	bbox.Ymin, bbox.Ymax = g.YMin(), g.YMax()
	if g.HasZ() {
		zmin, zmax := g.ZMin(), g.ZMax()
		bbox.Zmin, bbox.Zmax = &zmin, &zmax
	}
	if g.HasM() {
		mmin, mmax := g.MMin(), g.MMax()
		bbox.Mmin, bbox.Mmax = &mmin, &mmax
	}
	ret.Bbox = bbox
	return ret
}

func geoStatisticsFromThrift(t *format.GeospatialStatistics) *GeoStatistics {
	var min, max [4]float64
	for i := range min {
		min[i], max[i] = math.Inf(1), math.Inf(-1)
	}
	if bb := t.GetBbox(); bb != nil {
		min[geospatial.DimX], max[geospatial.DimX] = bb.GetXmin(), bb.GetXmax()
		min[geospatial.DimY], max[geospatial.DimY] = bb.GetYmin(), bb.GetYmax()
		if bb.IsSetZmin() && bb.IsSetZmax() {
			min[geospatial.DimZ], max[geospatial.DimZ] = bb.GetZmin(), bb.GetZmax()
		}
		if bb.IsSetMmin() && bb.IsSetMmax() {
			min[geospatial.DimM], max[geospatial.DimM] = bb.GetMmin(), bb.GetMmax()
		}
	}
	return &GeoStatistics{b: geospatial.NewBounderFrom(min, max, t.GetGeospatialTypes())}
}
