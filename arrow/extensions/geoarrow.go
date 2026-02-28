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

package extensions

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// CoordinateDimension represents the dimensionality of coordinates
type CoordinateDimension int

const (
	DimensionXY CoordinateDimension = iota
	DimensionXYZ
	DimensionXYM
	DimensionXYZM
)

// String returns the string representation of the coordinate dimension
func (d CoordinateDimension) String() string {
	switch d {
	case DimensionXY:
		return "xy"
	case DimensionXYZ:
		return "xyz"
	case DimensionXYM:
		return "xym"
	case DimensionXYZM:
		return "xyzm"
	default:
		return "unknown"
	}
}

// Size returns the number of coordinate values per point
func (d CoordinateDimension) Size() int {
	switch d {
	case DimensionXY:
		return 2
	case DimensionXYZ, DimensionXYM:
		return 3
	case DimensionXYZM:
		return 4
	default:
		return 2
	}
}

// GeometryEncoding represents the encoding method for geometry data
type GeometryEncoding int

const (
	EncodingGeoArrow GeometryEncoding = iota
)

// String returns the string representation of the encoding
func (e GeometryEncoding) String() string {
	switch e {
	case EncodingGeoArrow:
		return "geoarrow"
	default:
		return "unknown"
	}
}

// EdgeType represents the edge interpretation for geometry data
type EdgeType string

const (
	EdgePlanar    EdgeType = "planar"
	EdgeSpherical EdgeType = "spherical"
)

// String returns the string representation of the edge type
func (e EdgeType) String() string {
	return string(e)
}

// CoordType represents the coordinate layout type
type CoordType string

const (
	CoordSeparate    CoordType = "separate"
	CoordInterleaved CoordType = "interleaved"
)

// String returns the string representation of the coordinate type
func (c CoordType) String() string {
	return string(c)
}

// GeometryMetadata contains metadata for GeoArrow geometry types
type GeometryMetadata struct {
	// Encoding specifies the geometry encoding format
	Encoding GeometryEncoding `json:"encoding,omitempty"`

	// CRS contains PROJJSON coordinate reference system information
	CRS json.RawMessage `json:"crs,omitempty"`

	// Edges specifies the edge interpretation for the geometry
	Edges EdgeType `json:"edges,omitempty"`

	// CoordType specifies the coordinate layout (separate vs interleaved)
	CoordType CoordType `json:"coord_type,omitempty"`
}

// NewGeometryMetadata creates a new GeometryMetadata with default values
func NewGeometryMetadata() *GeometryMetadata {
	return &GeometryMetadata{
		Encoding:  EncodingGeoArrow,
		Edges:     EdgePlanar,
		CoordType: CoordSeparate,
	}
}

// Serialize serializes the metadata to a JSON string
func (gm *GeometryMetadata) Serialize() (string, error) {
	if gm == nil {
		return "", nil
	}
	data, err := json.Marshal(gm)
	if err != nil {
		return "", fmt.Errorf("failed to serialize geometry metadata: %w", err)
	}
	return string(data), nil
}

// DeserializeGeometryMetadata deserializes geometry metadata from a JSON string
func DeserializeGeometryMetadata(data string) (*GeometryMetadata, error) {
	if data == "" {
		return NewGeometryMetadata(), nil
	}

	var gm GeometryMetadata
	if err := json.Unmarshal([]byte(data), &gm); err != nil {
		return nil, fmt.Errorf("failed to deserialize geometry metadata: %w", err)
	}

	return &gm, nil
}

// createCoordinateType creates an Arrow data type for coordinates based on dimension
func createCoordinateType(dim CoordinateDimension) arrow.DataType {
	var fieldNames []string

	switch dim {
	case DimensionXY:
		fieldNames = []string{"x", "y"}
	case DimensionXYZ:
		fieldNames = []string{"x", "y", "z"}
	case DimensionXYM:
		fieldNames = []string{"x", "y", "m"}
	case DimensionXYZM:
		fieldNames = []string{"x", "y", "z", "m"}
	default:
		fieldNames = []string{"x", "y"}
	}

	fields := make([]arrow.Field, len(fieldNames))
	for i, name := range fieldNames {
		fields[i] = arrow.Field{
			Name:     name,
			Type:     arrow.PrimitiveTypes.Float64,
			Nullable: false,
		}
	}

	return arrow.StructOf(fields...)
}
