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

package extensions_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPointTypeBasics(t *testing.T) {
	// Test extensions.NewPointType (default XY)
	pointType := extensions.NewPointType()
	assert.Equal(t, "geoarrow.point", pointType.ExtensionName())
	assert.Equal(t, extensions.DimensionXY, pointType.Dimension())

	// Test extensions.NewPointTypeWithDimension for different dimensions
	pointTypeXYZ := extensions.NewPointTypeWithDimension(extensions.DimensionXYZ)
	pointTypeXYM := extensions.NewPointTypeWithDimension(extensions.DimensionXYM)
	pointTypeXYZM := extensions.NewPointTypeWithDimension(extensions.DimensionXYZM)

	assert.Equal(t, "geoarrow.point", pointType.ExtensionName())
	assert.Equal(t, "geoarrow.point", pointTypeXYZ.ExtensionName())
	assert.Equal(t, "geoarrow.point", pointTypeXYM.ExtensionName())
	assert.Equal(t, "geoarrow.point", pointTypeXYZM.ExtensionName())

	assert.True(t, pointType.ExtensionEquals(pointType))
	assert.True(t, pointTypeXYZ.ExtensionEquals(pointTypeXYZ))
	assert.True(t, pointTypeXYM.ExtensionEquals(pointTypeXYM))
	assert.True(t, pointTypeXYZM.ExtensionEquals(pointTypeXYZM))

	// Different dimensions should not be equal
	assert.False(t, pointType.ExtensionEquals(pointTypeXYZ))
	assert.False(t, pointTypeXYZ.ExtensionEquals(pointTypeXYM))
	assert.False(t, pointTypeXYM.ExtensionEquals(pointTypeXYZM))

	// Test string representations
	assert.Equal(t, "extension<geoarrow.point[xy]>", pointType.String())
	assert.Equal(t, "extension<geoarrow.point[xyz]>", pointTypeXYZ.String())
	assert.Equal(t, "extension<geoarrow.point[xym]>", pointTypeXYM.String())
	assert.Equal(t, "extension<geoarrow.point[xyzm]>", pointTypeXYZM.String())
}

func TestGeometryMetadata(t *testing.T) {
	t.Run("default_metadata", func(t *testing.T) {
		metadata := extensions.NewGeometryMetadata()
		assert.Equal(t, extensions.EncodingGeoArrow, metadata.Encoding)
		assert.Equal(t, extensions.EdgePlanar, metadata.Edges)
		assert.Equal(t, extensions.CoordSeparate, metadata.CoordType)
		assert.Empty(t, metadata.CRS)
	})

	t.Run("edge_types", func(t *testing.T) {
		assert.Equal(t, "planar", extensions.EdgePlanar.String())
		assert.Equal(t, "spherical", extensions.EdgeSpherical.String())
	})

	t.Run("coord_types", func(t *testing.T) {
		assert.Equal(t, "separate", extensions.CoordSeparate.String())
		assert.Equal(t, "interleaved", extensions.CoordInterleaved.String())
	})

	t.Run("custom_metadata", func(t *testing.T) {
		metadata := extensions.NewGeometryMetadata()
		metadata.Edges = extensions.EdgeSpherical
		metadata.CoordType = extensions.CoordInterleaved
		metadata.CRS = []byte(`{"type": "name", "properties": {"name": "EPSG:4326"}}`)

		// Test serialization
		serialized, err := metadata.Serialize()
		require.NoError(t, err)
		assert.Contains(t, serialized, "spherical")
		assert.Contains(t, serialized, "interleaved")
		assert.Contains(t, serialized, "EPSG:4326")

		// Test deserialization
		deserialized, err := extensions.DeserializeGeometryMetadata(serialized)
		require.NoError(t, err)
		assert.Equal(t, extensions.EdgeSpherical, deserialized.Edges)
		assert.Equal(t, extensions.CoordInterleaved, deserialized.CoordType)
		assert.JSONEq(t, string(metadata.CRS), string(deserialized.CRS))
	})

	t.Run("point_type_with_custom_metadata", func(t *testing.T) {
		metadata := extensions.NewGeometryMetadata()
		metadata.Edges = extensions.EdgeSpherical
		metadata.CoordType = extensions.CoordInterleaved

		pointType := extensions.NewPointTypeWithMetadata(extensions.DimensionXYZ, metadata)
		assert.Equal(t, extensions.DimensionXYZ, pointType.Dimension())

		retrievedMetadata := pointType.Metadata()
		assert.Equal(t, extensions.EdgeSpherical, retrievedMetadata.Edges)
		assert.Equal(t, extensions.CoordInterleaved, retrievedMetadata.CoordType)
	})
}

func TestPointExtensionRegistration(t *testing.T) {
	// Test that Point type is registered with Arrow
	extType := arrow.GetExtensionType("geoarrow.point")
	require.NotNil(t, extType, "Point extension type should be registered")
	assert.Equal(t, "geoarrow.point", extType.ExtensionName())
}

func TestPointBuilderExtensive(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("basic_building", func(t *testing.T) {
		builder := extensions.NewPointBuilder(mem, extensions.NewPointType())
		defer builder.Release()

		// Test all append methods
		builder.AppendXY(1.0, 2.0)
		builder.AppendNull()
		builder.Append(extensions.NewPoint(3.0, 4.0))

		// Test AppendValues
		points := []extensions.Point{extensions.NewPoint(5.0, 6.0), extensions.NewPoint(7.0, 8.0)}
		valid := []bool{true, true}
		builder.AppendValues(points, valid)

		arr := builder.NewArray()
		defer arr.Release()

		pointArray := arr.(*extensions.PointArray)
		assert.Equal(t, 5, pointArray.Len())
		assert.Equal(t, 1, pointArray.NullN())

		// Test Values() method
		allPoints := pointArray.Values()
		assert.Equal(t, 5, len(allPoints))
		assert.Equal(t, 1.0, allPoints[0].X)
		assert.Equal(t, 2.0, allPoints[0].Y)

		// Test null handling
		assert.True(t, pointArray.IsNull(1))
		assert.False(t, pointArray.IsNull(0))
	})

	t.Run("multidimensional_building", func(t *testing.T) {
		// Test XYZ
		builderXYZ := extensions.NewPointBuilder(mem, extensions.NewPointTypeWithDimension(extensions.DimensionXYZ))
		defer builderXYZ.Release()
		builderXYZ.AppendXYZ(1.0, 2.0, 3.0)
		builderXYZ.Append(extensions.NewPointZ(4.0, 5.0, 6.0))

		arrXYZ := builderXYZ.NewArray()
		defer arrXYZ.Release()
		pointArrayXYZ := arrXYZ.(*extensions.PointArray)

		point := pointArrayXYZ.Value(0)
		assert.Equal(t, extensions.DimensionXYZ, point.Dimension)
		assert.Equal(t, 3.0, point.Z)

		// Test XYM
		builderXYM := extensions.NewPointBuilder(mem, extensions.NewPointTypeWithDimension(extensions.DimensionXYM))
		defer builderXYM.Release()
		builderXYM.AppendXYM(1.0, 2.0, 100.0)
		builderXYM.Append(extensions.NewPointM(4.0, 5.0, 200.0))

		arrXYM := builderXYM.NewArray()
		defer arrXYM.Release()
		pointArrayXYM := arrXYM.(*extensions.PointArray)

		pointM := pointArrayXYM.Value(0)
		assert.Equal(t, extensions.DimensionXYM, pointM.Dimension)
		assert.Equal(t, 100.0, pointM.M)

		// Test XYZM
		builderXYZM := extensions.NewPointBuilder(mem, extensions.NewPointTypeWithDimension(extensions.DimensionXYZM))
		defer builderXYZM.Release()
		builderXYZM.AppendXYZM(1.0, 2.0, 3.0, 100.0)

		arrXYZM := builderXYZM.NewArray()
		defer arrXYZM.Release()
		pointArrayXYZM := arrXYZM.(*extensions.PointArray)

		pointZM := pointArrayXYZM.Value(0)
		assert.Equal(t, extensions.DimensionXYZM, pointZM.Dimension)
		assert.Equal(t, 3.0, pointZM.Z)
		assert.Equal(t, 100.0, pointZM.M)
	})
}

func TestPointTypeCreateFromArray(t *testing.T) {
	pointType := extensions.NewPointType()

	// Use the storage type from the point type
	structType := pointType.StorageType().(*arrow.StructType)
	structBuilder := array.NewStructBuilder(memory.DefaultAllocator, structType)
	defer structBuilder.Release()

	// Add some points manually to struct
	structBuilder.Append(true)
	structBuilder.FieldBuilder(0).(*array.Float64Builder).Append(1.5)
	structBuilder.FieldBuilder(1).(*array.Float64Builder).Append(2.5)

	structBuilder.AppendNull()

	structBuilder.Append(true)
	structBuilder.FieldBuilder(0).(*array.Float64Builder).Append(3.5)
	structBuilder.FieldBuilder(1).(*array.Float64Builder).Append(4.5)

	storage := structBuilder.NewArray()
	defer storage.Release()

	arr := array.NewExtensionArrayWithStorage(pointType, storage)
	defer arr.Release()

	assert.Equal(t, 3, arr.Len())
	assert.Equal(t, 1, arr.NullN())

	pointArray, ok := arr.(*extensions.PointArray)
	require.True(t, ok)

	point0 := pointArray.Value(0)
	assert.Equal(t, 1.5, point0.X)
	assert.Equal(t, 2.5, point0.Y)

	// Check null
	assert.True(t, pointArray.IsNull(1))

	point2 := pointArray.Value(2)
	assert.Equal(t, 3.5, point2.X)
	assert.Equal(t, 4.5, point2.Y)
}

func TestPointRecordBuilder(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "point", Type: extensions.NewPointType()},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	pointBuilder := builder.Field(0).(*extensions.PointBuilder)
	pointBuilder.AppendXY(1.0, 2.0)
	pointBuilder.AppendNull()
	pointBuilder.AppendXY(3.0, 4.0)

	record := builder.NewRecordBatch()
	defer record.Release()

	// Test JSON marshaling of record
	b, err := record.MarshalJSON()
	require.NoError(t, err)

	// Should contain coordinate arrays
	jsonStr := string(b)
	assert.Contains(t, jsonStr, "[1,2]")
	assert.Contains(t, jsonStr, "null")
	assert.Contains(t, jsonStr, "[3,4]")
}

func TestMarshalPointArray(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Test different dimensions
	testCases := []struct {
		name      string
		pointType *extensions.PointType
		points    []extensions.Point
		expected  string
	}{
		{
			name:      "XY",
			pointType: extensions.NewPointType(),
			points:    []extensions.Point{extensions.NewPoint(1.5, 2.5), extensions.NewPoint(3.0, 4.0)},
			expected:  "[[1.5,2.5],[3,4]]",
		},
		{
			name:      "XYZ",
			pointType: extensions.NewPointTypeWithDimension(extensions.DimensionXYZ),
			points:    []extensions.Point{extensions.NewPointZ(1.0, 2.0, 3.0), extensions.NewPointZ(4.0, 5.0, 6.0)},
			expected:  "[[1,2,3],[4,5,6]]",
		},
		{
			name:      "XYM",
			pointType: extensions.NewPointTypeWithDimension(extensions.DimensionXYM),
			points:    []extensions.Point{extensions.NewPointM(1.0, 2.0, 100.0), extensions.NewPointM(4.0, 5.0, 200.0)},
			expected:  "[[1,2,100],[4,5,200]]",
		},
		{
			name:      "XYZM",
			pointType: extensions.NewPointTypeWithDimension(extensions.DimensionXYZM),
			points:    []extensions.Point{extensions.NewPointZM(1.0, 2.0, 3.0, 100.0)},
			expected:  "[[1,2,3,100]]",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := extensions.NewPointBuilder(mem, tc.pointType)
			defer builder.Release()

			for _, point := range tc.points {
				builder.Append(point)
			}

			arr := builder.NewArray()
			defer arr.Release()

			pointArray := arr.(*extensions.PointArray)
			b, err := pointArray.MarshalJSON()
			require.NoError(t, err)

			require.JSONEq(t, tc.expected, string(b))
		})
	}
}

func TestPointConstructors(t *testing.T) {
	// Test coordinate dimension enum first
	t.Run("coordinate_dimension", func(t *testing.T) {
		assert.Equal(t, "xy", extensions.DimensionXY.String())
		assert.Equal(t, "xyz", extensions.DimensionXYZ.String())
		assert.Equal(t, "xym", extensions.DimensionXYM.String())
		assert.Equal(t, "xyzm", extensions.DimensionXYZM.String())

		assert.Equal(t, 2, extensions.DimensionXY.Size())
		assert.Equal(t, 3, extensions.DimensionXYZ.Size())
		assert.Equal(t, 3, extensions.DimensionXYM.Size())
		assert.Equal(t, 4, extensions.DimensionXYZM.Size())
	})

	// Test all Point constructor functions
	t.Run("extensions.NewPoint", func(t *testing.T) {
		point := extensions.NewPoint(1.5, 2.5)
		assert.Equal(t, 1.5, point.X)
		assert.Equal(t, 2.5, point.Y)
		assert.Equal(t, 0.0, point.Z)
		assert.Equal(t, 0.0, point.M)
		assert.Equal(t, extensions.DimensionXY, point.Dimension)
		assert.False(t, point.IsEmpty())
		assert.Equal(t, "POINT(1.500000 2.500000)", point.String())
	})

	t.Run("extensions.NewPointZ", func(t *testing.T) {
		point := extensions.NewPointZ(1.5, 2.5, 3.5)
		assert.Equal(t, 1.5, point.X)
		assert.Equal(t, 2.5, point.Y)
		assert.Equal(t, 3.5, point.Z)
		assert.Equal(t, 0.0, point.M)
		assert.Equal(t, extensions.DimensionXYZ, point.Dimension)
		assert.False(t, point.IsEmpty())
		assert.Equal(t, "POINT Z(1.500000 2.500000 3.500000)", point.String())
	})

	t.Run("extensions.NewPointM", func(t *testing.T) {
		point := extensions.NewPointM(1.5, 2.5, 100.0)
		assert.Equal(t, 1.5, point.X)
		assert.Equal(t, 2.5, point.Y)
		assert.Equal(t, 0.0, point.Z)
		assert.Equal(t, 100.0, point.M)
		assert.Equal(t, extensions.DimensionXYM, point.Dimension)
		assert.False(t, point.IsEmpty())
		assert.Equal(t, "POINT M(1.500000 2.500000 100.000000)", point.String())
	})

	t.Run("extensions.NewPointZM", func(t *testing.T) {
		point := extensions.NewPointZM(1.5, 2.5, 3.5, 100.0)
		assert.Equal(t, 1.5, point.X)
		assert.Equal(t, 2.5, point.Y)
		assert.Equal(t, 3.5, point.Z)
		assert.Equal(t, 100.0, point.M)
		assert.Equal(t, extensions.DimensionXYZM, point.Dimension)
		assert.False(t, point.IsEmpty())
		assert.Equal(t, "POINT ZM(1.500000 2.500000 3.500000 100.000000)", point.String())
	})

	t.Run("NewEmptyPoint", func(t *testing.T) {
		point := extensions.NewEmptyPoint()
		assert.True(t, point.IsEmpty())
		assert.Equal(t, "POINT EMPTY", point.String())
		assert.Equal(t, extensions.DimensionXY, point.Dimension)
	})
}

func TestPointSerialization(t *testing.T) {
	t.Run("type_roundtrip", func(t *testing.T) {
		originalType := extensions.NewPointTypeWithDimension(extensions.DimensionXYM)
		serialized := originalType.Serialize()

		deserializedInterface, err := originalType.Deserialize(originalType.StorageType(), serialized)
		require.NoError(t, err)

		deserializedType, ok := deserializedInterface.(*extensions.PointType)
		require.True(t, ok)

		assert.True(t, originalType.ExtensionEquals(deserializedType))
		assert.Equal(t, originalType.Dimension(), deserializedType.Dimension())
	})

	t.Run("json_marshaling", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		pointType := extensions.NewPointType()
		builder := extensions.NewPointBuilder(mem, pointType)

		builder.AppendXY(1.5, 2.5)
		builder.AppendNull()

		arr := builder.NewArray()
		defer arr.Release()

		pointArray := arr.(*extensions.PointArray)

		// Test GetOneForMarshal
		jsonVal0 := pointArray.GetOneForMarshal(0)
		coords0, ok := jsonVal0.([]float64)
		require.True(t, ok)
		assert.Equal(t, []float64{1.5, 2.5}, coords0)

		jsonVal1 := pointArray.GetOneForMarshal(1)
		assert.Nil(t, jsonVal1) // null value
	})
}
