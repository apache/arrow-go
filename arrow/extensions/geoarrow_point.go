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
	"math"
	"reflect"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Point represents a point geometry with coordinates
type Point struct {
	X, Y, Z, M float64
	Dimension  CoordinateDimension
}

// NewPoint creates a new 2D point
func NewPoint(x, y float64) Point {
	return Point{X: x, Y: y, Dimension: DimensionXY}
}

// NewPointZ creates a new 3D point with Z coordinate
func NewPointZ(x, y, z float64) Point {
	return Point{X: x, Y: y, Z: z, Dimension: DimensionXYZ}
}

// NewPointM creates a new 2D point with M coordinate
func NewPointM(x, y, m float64) Point {
	return Point{X: x, Y: y, M: m, Dimension: DimensionXYM}
}

// NewPointZM creates a new 3D point with Z and M coordinates
func NewPointZM(x, y, z, m float64) Point {
	return Point{X: x, Y: y, Z: z, M: m, Dimension: DimensionXYZM}
}

// NewEmptyPoint creates a new empty point
func NewEmptyPoint() Point {
	return Point{X: math.NaN(), Y: math.NaN(), Z: math.NaN(), M: math.NaN(), Dimension: DimensionXY}
}

// IsEmpty returns true if this is an empty point (all coordinates are NaN or zero with no dimension)
func (p Point) IsEmpty() bool {
	return math.IsNaN(p.X) && math.IsNaN(p.Y) && math.IsNaN(p.Z) && math.IsNaN(p.M)
}

// String returns a string representation of the point
func (p Point) String() string {
	if p.IsEmpty() {
		return "POINT EMPTY"
	}

	switch p.Dimension {
	case DimensionXY:
		return fmt.Sprintf("POINT(%.6f %.6f)", p.X, p.Y)
	case DimensionXYZ:
		return fmt.Sprintf("POINT Z(%.6f %.6f %.6f)", p.X, p.Y, p.Z)
	case DimensionXYM:
		return fmt.Sprintf("POINT M(%.6f %.6f %.6f)", p.X, p.Y, p.M)
	case DimensionXYZM:
		return fmt.Sprintf("POINT ZM(%.6f %.6f %.6f %.6f)", p.X, p.Y, p.Z, p.M)
	default:
		return fmt.Sprintf("POINT(%.6f %.6f)", p.X, p.Y)
	}
}

// PointType is the extension type for Point geometries
type PointType struct {
	arrow.ExtensionBase
	metadata  *GeometryMetadata
	dimension CoordinateDimension
}

// NewPointType creates a new Point extension type for 2D points
func NewPointType() *PointType {
	return NewPointTypeWithDimension(DimensionXY)
}

// NewPointTypeWithDimension creates a new Point extension type with specified dimension
func NewPointTypeWithDimension(dim CoordinateDimension) *PointType {
	metadata := NewGeometryMetadata()
	coordType := createCoordinateType(dim)

	return &PointType{
		ExtensionBase: arrow.ExtensionBase{Storage: coordType},
		metadata:      metadata,
		dimension:     dim,
	}
}

// NewPointTypeWithMetadata creates a new Point extension type with custom metadata
func NewPointTypeWithMetadata(dim CoordinateDimension, metadata *GeometryMetadata) *PointType {
	if metadata == nil {
		metadata = NewGeometryMetadata()
	}
	coordType := createCoordinateType(dim)

	return &PointType{
		ExtensionBase: arrow.ExtensionBase{Storage: coordType},
		metadata:      metadata,
		dimension:     dim,
	}
}

// ArrayType returns the array type for Point arrays
func (*PointType) ArrayType() reflect.Type {
	return reflect.TypeOf(PointArray{})
}

// ExtensionName returns the name of the extension
func (*PointType) ExtensionName() string {
	return "geoarrow.point"
}

// String returns a string representation of the type
func (p *PointType) String() string {
	return fmt.Sprintf("extension<%s[%s]>", p.ExtensionName(), p.dimension.String())
}

// ExtensionEquals checks if two extension types are equal
func (p *PointType) ExtensionEquals(other arrow.ExtensionType) bool {
	if p.ExtensionName() != other.ExtensionName() {
		return false
	}
	if otherPoint, ok := other.(*PointType); ok {
		return p.dimension == otherPoint.dimension
	}
	return arrow.TypeEqual(p.Storage, other.StorageType())
}

// Serialize serializes the extension type metadata
func (p *PointType) Serialize() string {
	if p.metadata == nil {
		return ""
	}
	serialized, _ := p.metadata.Serialize()
	return serialized
}

// Deserialize deserializes the extension type metadata
func (*PointType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	metadata, err := DeserializeGeometryMetadata(data)
	if err != nil {
		return nil, err
	}

	// Determine dimension from storage type
	dim := DimensionXY
	if structType, ok := storageType.(*arrow.StructType); ok {
		numFields := structType.NumFields()
		switch numFields {
		case 2:
			dim = DimensionXY
		case 3:
			// Check if it's XYZ or XYM by looking at field names
			if structType.Field(2).Name == "z" {
				dim = DimensionXYZ
			} else {
				dim = DimensionXYM
			}
		case 4:
			dim = DimensionXYZM
		default:
			dim = DimensionXY
		}
	}

	return &PointType{
		ExtensionBase: arrow.ExtensionBase{Storage: storageType},
		metadata:      metadata,
		dimension:     dim,
	}, nil
}

// NewBuilder creates a new array builder for this type
func (p *PointType) NewBuilder(mem memory.Allocator) array.Builder {
	return NewPointBuilder(mem, p)
}

// Metadata returns the geometry metadata
func (p *PointType) Metadata() *GeometryMetadata {
	return p.metadata
}

// Dimension returns the coordinate dimension
func (p *PointType) Dimension() CoordinateDimension {
	return p.dimension
}

// PointArray represents an array of Point geometries
type PointArray struct {
	array.ExtensionArrayBase
}

// String returns a string representation of the array
func (p *PointArray) String() string {
	o := new(strings.Builder)
	o.WriteString("PointArray[")
	for i := 0; i < p.Len(); i++ {
		if i > 0 {
			o.WriteString(" ")
		}
		if p.IsNull(i) {
			o.WriteString(array.NullValueStr)
		} else {
			point := p.Value(i)
			o.WriteString(point.String())
		}
	}
	o.WriteString("]")
	return o.String()
}

// Value returns the Point at the given index
func (p *PointArray) Value(i int) Point {
	pointType := p.ExtensionType().(*PointType)
	structArray := p.Storage().(*array.Struct)

	point := Point{Dimension: pointType.dimension}

	if p.IsNull(i) {
		return point
	}

	// Get X coordinate
	xArray := structArray.Field(0).(*array.Float64)
	point.X = xArray.Value(i)

	// Get Y coordinate
	yArray := structArray.Field(1).(*array.Float64)
	point.Y = yArray.Value(i)

	// Get Z coordinate if present
	if pointType.dimension == DimensionXYZ || pointType.dimension == DimensionXYZM {
		zArray := structArray.Field(2).(*array.Float64)
		point.Z = zArray.Value(i)
	}

	// Get M coordinate if present
	switch pointType.dimension {
	case DimensionXYM:
		mArray := structArray.Field(2).(*array.Float64)
		point.M = mArray.Value(i)
	case DimensionXYZM:
		mArray := structArray.Field(3).(*array.Float64)
		point.M = mArray.Value(i)
	}

	return point
}

// Values returns all Point values as a slice
func (p *PointArray) Values() []Point {
	values := make([]Point, p.Len())
	for i := range values {
		values[i] = p.Value(i)
	}
	return values
}

// ValueStr returns a string representation of the value at index i
func (p *PointArray) ValueStr(i int) string {
	if p.IsNull(i) {
		return array.NullValueStr
	}
	return p.Value(i).String()
}

// GetOneForMarshal returns the value at index i for JSON marshaling
func (p *PointArray) GetOneForMarshal(i int) any {
	if p.IsNull(i) {
		return nil
	}
	point := p.Value(i)
	switch point.Dimension {
	case DimensionXY:
		return []float64{point.X, point.Y}
	case DimensionXYZ:
		return []float64{point.X, point.Y, point.Z}
	case DimensionXYM:
		return []float64{point.X, point.Y, point.M}
	case DimensionXYZM:
		return []float64{point.X, point.Y, point.Z, point.M}
	default:
		// Should never happen but defensive programming
		panic(fmt.Sprintf("unknown coordinate dimension: %v", point.Dimension))
	}
}

// MarshalJSON implements json.Marshaler
func (p *PointArray) MarshalJSON() ([]byte, error) {
	vals := make([]any, p.Len())
	for i := range vals {
		vals[i] = p.GetOneForMarshal(i)
	}
	return json.Marshal(vals)
}

// PointBuilder is an array builder for Point geometries
type PointBuilder struct {
	*array.ExtensionBuilder
}

// NewPointBuilder creates a new Point array builder
func NewPointBuilder(mem memory.Allocator, dtype *PointType) *PointBuilder {
	return &PointBuilder{
		ExtensionBuilder: array.NewExtensionBuilder(mem, dtype),
	}
}

// Append appends a Point to the array
func (b *PointBuilder) Append(point Point) {
	pointType := b.Type().(*PointType)
	structBuilder := b.Builder.(*array.StructBuilder)
	structBuilder.Append(true)

	// X coordinate
	xBuilder := structBuilder.FieldBuilder(0).(*array.Float64Builder)
	xBuilder.Append(point.X)

	// Y coordinate
	yBuilder := structBuilder.FieldBuilder(1).(*array.Float64Builder)
	yBuilder.Append(point.Y)

	// Z coordinate if present
	if pointType.dimension == DimensionXYZ || pointType.dimension == DimensionXYZM {
		zBuilder := structBuilder.FieldBuilder(2).(*array.Float64Builder)
		zBuilder.Append(point.Z)
	}

	// M coordinate if present
	switch pointType.dimension {
	case DimensionXYM:
		mBuilder := structBuilder.FieldBuilder(2).(*array.Float64Builder)
		mBuilder.Append(point.M)
	case DimensionXYZM:
		mBuilder := structBuilder.FieldBuilder(3).(*array.Float64Builder)
		mBuilder.Append(point.M)
	}
}

// AppendXY appends a 2D point to the array
func (b *PointBuilder) AppendXY(x, y float64) {
	b.Append(NewPoint(x, y))
}

// AppendXYZ appends a 3D point to the array
func (b *PointBuilder) AppendXYZ(x, y, z float64) {
	b.Append(NewPointZ(x, y, z))
}

// AppendXYM appends a 2D point with M coordinate to the array
func (b *PointBuilder) AppendXYM(x, y, m float64) {
	b.Append(NewPointM(x, y, m))
}

// AppendXYZM appends a 3D point with M coordinate to the array
func (b *PointBuilder) AppendXYZM(x, y, z, m float64) {
	b.Append(NewPointZM(x, y, z, m))
}

// AppendNull appends a null value to the array
func (b *PointBuilder) AppendNull() {
	b.ExtensionBuilder.Builder.(*array.StructBuilder).AppendNull()
}

// AppendValues appends multiple Point values to the array
func (b *PointBuilder) AppendValues(v []Point, valid []bool) {
	if len(v) != len(valid) && len(valid) != 0 {
		panic("len(v) != len(valid) && len(valid) != 0")
	}

	for i, point := range v {
		if len(valid) > 0 && !valid[i] {
			b.AppendNull()
		} else {
			b.Append(point)
		}
	}
}

// NewArray creates a new array from the builder
func (b *PointBuilder) NewArray() arrow.Array {
	storage := b.Builder.(*array.StructBuilder).NewArray()
	defer storage.Release()
	return array.NewExtensionArrayWithStorage(b.Type().(*PointType), storage)
}
