package geoarrow_test

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	_ "github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/extensions/geoarrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/internal/json"
	"github.com/stretchr/testify/require"
)

func xyStorage() arrow.DataType {
	return arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	)
}

func xyzStorage() arrow.DataType {
	return arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "z", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	)
}

func xymStorage() arrow.DataType {
	return arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "m", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	)
}

func xyzmStorage() arrow.DataType {
	return arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "z", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "m", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	)
}

func TestPointTypeBasics(t *testing.T) {
	typ := geoarrow.NewPointType()

	require.Equal(t, "geoarrow.point", typ.ExtensionName())
	require.True(t, typ.ExtensionEquals(typ))
	require.Equal(t, arrow.STRUCT, typ.StorageType().ID())

	// Default is XY (2 fields)
	st := typ.StorageType().(*arrow.StructType)
	require.Equal(t, 2, st.NumFields())
	require.Equal(t, "x", st.Field(0).Name)
	require.Equal(t, "y", st.Field(1).Name)

	// Equal types
	typ2 := geoarrow.NewPointType()
	require.True(t, typ.ExtensionEquals(typ2))

	// Different storage (XYZ) should still be equal (same extension + metadata)
	typXYZ := geoarrow.NewPointType(geoarrow.PointWithStorage(xyzStorage()))
	require.True(t, typ.ExtensionEquals(typXYZ))
}

func TestPointTypeWithMetadata(t *testing.T) {
	meta := geoarrow.Metadata{Edges: geoarrow.EdgeSpherical}
	typ := geoarrow.NewPointType(geoarrow.PointWithMetadata(meta))

	require.Equal(t, geoarrow.EdgeSpherical, typ.Metadata().Edges)

	// Different metadata should not be equal
	typDefault := geoarrow.NewPointType()
	require.False(t, typ.ExtensionEquals(typDefault))
}

func TestPointSerializationRoundTrip(t *testing.T) {
	typ := geoarrow.NewPointType(geoarrow.PointWithMetadata(geoarrow.Metadata{
		Edges: geoarrow.EdgeSpherical,
	}))

	serialized := typ.Serialize()
	deserialized, err := typ.Deserialize(typ.StorageType(), serialized)
	require.NoError(t, err)
	require.True(t, typ.ExtensionEquals(deserialized))
}

func TestPointValueConstructors(t *testing.T) {
	t.Run("XY", func(t *testing.T) {
		v := geoarrow.NewPointValue(1.5, 2.5)
		require.Equal(t, 1.5, v.X())
		require.Equal(t, 2.5, v.Y())
		require.Equal(t, geoarrow.XY, v.Dimension())
		require.Equal(t, geoarrow.PointID, v.GeometryType())
		require.False(t, v.IsEmpty())
		require.Equal(t, "POINT(1.500000 2.500000)", v.String())
	})

	t.Run("XYZ", func(t *testing.T) {
		v := geoarrow.NewPointValueZ(1.0, 2.0, 3.0)
		require.Equal(t, 3.0, v.Z())
		require.Equal(t, geoarrow.XYZ, v.Dimension())
		require.Equal(t, geoarrow.PointZID, v.GeometryType())
		require.Equal(t, "POINT Z(1.000000 2.000000 3.000000)", v.String())
	})

	t.Run("XYM", func(t *testing.T) {
		v := geoarrow.NewPointValueM(1.0, 2.0, 100.0)
		require.Equal(t, 100.0, v.M())
		require.Equal(t, geoarrow.XYM, v.Dimension())
		require.Equal(t, geoarrow.PointMID, v.GeometryType())
		require.Equal(t, "POINT M(1.000000 2.000000 100.000000)", v.String())
	})

	t.Run("XYZM", func(t *testing.T) {
		v := geoarrow.NewPointValueZM(1.0, 2.0, 3.0, 100.0)
		require.Equal(t, 3.0, v.Z())
		require.Equal(t, 100.0, v.M())
		require.Equal(t, geoarrow.XYZM, v.Dimension())
		require.Equal(t, geoarrow.PointZMID, v.GeometryType())
		require.Equal(t, "POINT ZM(1.000000 2.000000 3.000000 100.000000)", v.String())
	})

	t.Run("Empty", func(t *testing.T) {
		v := geoarrow.PointValue{}
		require.True(t, v.IsEmpty())
	})
}

func TestPointBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPointType()
	builder := typ.NewBuilder(mem).(*geoarrow.PointBuilder)
	defer builder.Release()

	builder.Append(geoarrow.NewPointValue(1.0, 2.0))
	builder.AppendNull()
	builder.Append(geoarrow.NewPointValue(3.0, 4.0))

	arr := builder.NewArray()
	defer arr.Release()

	require.Equal(t, 3, arr.Len())
	require.Equal(t, 1, arr.NullN())

	pointArr := arr.(*geoarrow.PointArray)
	v0 := pointArr.Value(0)
	require.Equal(t, 1.0, v0.X())
	require.Equal(t, 2.0, v0.Y())

	require.True(t, arr.IsNull(1))

	v2 := pointArr.Value(2)
	require.Equal(t, 3.0, v2.X())
	require.Equal(t, 4.0, v2.Y())
}

func TestPointBuilderXYZ(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPointType(geoarrow.PointWithStorage(xyzStorage()))
	builder := typ.NewBuilder(mem).(*geoarrow.PointBuilder)
	defer builder.Release()

	builder.Append(geoarrow.NewPointValueZ(1.0, 2.0, 3.0))
	builder.Append(geoarrow.NewPointValueZ(4.0, 5.0, 6.0))

	arr := builder.NewArray()
	defer arr.Release()

	pointArr := arr.(*geoarrow.PointArray)
	v := pointArr.Value(0)
	require.Equal(t, geoarrow.XYZ, v.Dimension())
	require.Equal(t, 1.0, v.X())
	require.Equal(t, 2.0, v.Y())
	require.Equal(t, 3.0, v.Z())
}

func TestPointBuilderXYM(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPointType(geoarrow.PointWithStorage(xymStorage()))
	builder := typ.NewBuilder(mem).(*geoarrow.PointBuilder)
	defer builder.Release()

	builder.Append(geoarrow.NewPointValueM(1.0, 2.0, 100.0))

	arr := builder.NewArray()
	defer arr.Release()

	pointArr := arr.(*geoarrow.PointArray)
	v := pointArr.Value(0)
	require.Equal(t, geoarrow.XYM, v.Dimension())
	require.Equal(t, 100.0, v.M())
}

func TestPointBuilderXYZM(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPointType(geoarrow.PointWithStorage(xyzmStorage()))
	builder := typ.NewBuilder(mem).(*geoarrow.PointBuilder)
	defer builder.Release()

	builder.Append(geoarrow.NewPointValueZM(1.0, 2.0, 3.0, 100.0))

	arr := builder.NewArray()
	defer arr.Release()

	pointArr := arr.(*geoarrow.PointArray)
	v := pointArr.Value(0)
	require.Equal(t, geoarrow.XYZM, v.Dimension())
	require.Equal(t, 3.0, v.Z())
	require.Equal(t, 100.0, v.M())
}

func TestPointAppendValues(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPointType()
	builder := typ.NewBuilder(mem).(*geoarrow.PointBuilder)
	defer builder.Release()

	values := []geoarrow.PointValue{
		geoarrow.NewPointValue(1.0, 2.0),
		geoarrow.NewPointValue(3.0, 4.0),
		geoarrow.NewPointValue(5.0, 6.0),
	}
	valid := []bool{true, false, true}
	builder.AppendValues(values, valid)

	arr := builder.NewArray()
	defer arr.Release()

	require.Equal(t, 3, arr.Len())
	require.Equal(t, 1, arr.NullN())
	require.True(t, arr.IsNull(1))

	pointArr := arr.(*geoarrow.PointArray)
	require.Equal(t, 1.0, pointArr.Value(0).X())
	require.Equal(t, 5.0, pointArr.Value(2).X())
}

func TestPointCreateFromStorage(t *testing.T) {
	typ := geoarrow.NewPointType()
	structType := typ.StorageType().(*arrow.StructType)

	structBuilder := array.NewStructBuilder(memory.DefaultAllocator, structType)
	defer structBuilder.Release()

	structBuilder.Append(true)
	structBuilder.FieldBuilder(0).(*array.Float64Builder).Append(1.5)
	structBuilder.FieldBuilder(1).(*array.Float64Builder).Append(2.5)

	structBuilder.AppendNull()

	structBuilder.Append(true)
	structBuilder.FieldBuilder(0).(*array.Float64Builder).Append(3.5)
	structBuilder.FieldBuilder(1).(*array.Float64Builder).Append(4.5)

	storage := structBuilder.NewArray()
	defer storage.Release()

	arr := array.NewExtensionArrayWithStorage(typ, storage)
	defer arr.Release()

	require.Equal(t, 3, arr.Len())
	require.Equal(t, 1, arr.NullN())

	pointArr, ok := arr.(*geoarrow.PointArray)
	require.True(t, ok)

	require.Equal(t, 1.5, pointArr.Value(0).X())
	require.Equal(t, 2.5, pointArr.Value(0).Y())
	require.True(t, arr.IsNull(1))
	require.Equal(t, 3.5, pointArr.Value(2).X())
}

func TestPointStringRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPointType()
	builder := typ.NewBuilder(mem).(*geoarrow.PointBuilder)
	defer builder.Release()

	builder.Append(geoarrow.NewPointValue(1.0, 2.0))
	builder.AppendNull()
	builder.Append(geoarrow.NewPointValue(3.0, 4.0))

	arr := builder.NewArray()
	defer arr.Release()

	// Rebuild from ValueStr
	builder2 := typ.NewBuilder(mem).(*geoarrow.PointBuilder)
	defer builder2.Release()

	for i := 0; i < arr.Len(); i++ {
		require.NoError(t, builder2.AppendValueFromString(arr.ValueStr(i)))
	}

	arr2 := builder2.NewArray()
	defer arr2.Release()

	require.True(t, array.Equal(arr, arr2))
}

func TestPointJSONRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPointType()
	builder := typ.NewBuilder(mem).(*geoarrow.PointBuilder)
	defer builder.Release()

	builder.Append(geoarrow.NewPointValue(1.5, 2.5))
	builder.AppendNull()
	builder.Append(geoarrow.NewPointValue(3.0, 4.0))

	arr := builder.NewArray()
	defer arr.Release()

	// Marshal
	jsonData, err := json.Marshal(arr)
	require.NoError(t, err)

	// Unmarshal via FromJSON
	arr2, _, err := array.FromJSON(mem, typ, bytes.NewReader(jsonData))
	require.NoError(t, err)
	defer arr2.Release()

	require.True(t, array.Equal(arr, arr2))
}

func TestPointIPCRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPointType()
	builder := typ.NewBuilder(mem).(*geoarrow.PointBuilder)

	builder.Append(geoarrow.NewPointValue(1.0, 2.0))
	builder.AppendNull()
	builder.Append(geoarrow.NewPointValue(3.0, 4.0))

	arr := builder.NewArray()
	defer arr.Release()
	builder.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "point", Type: typ, Nullable: true}}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{arr}, -1)
	defer batch.Release()

	var buf bytes.Buffer
	wr := ipc.NewWriter(&buf, ipc.WithSchema(batch.Schema()))
	require.NoError(t, wr.Write(batch))
	require.NoError(t, wr.Close())

	rdr, err := ipc.NewReader(&buf)
	require.NoError(t, err)
	written, err := rdr.Read()
	require.NoError(t, err)
	written.Retain()
	defer written.Release()
	rdr.Release()

	require.True(t, batch.Schema().Equal(written.Schema()))
	require.True(t, array.RecordEqual(batch, written))
}

func TestPointRecordBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPointType()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "point", Type: typ},
	}, nil)
	recBuilder := array.NewRecordBuilder(mem, schema)
	defer recBuilder.Release()

	pointBuilder := recBuilder.Field(0).(*geoarrow.PointBuilder)
	pointBuilder.Append(geoarrow.NewPointValue(1.0, 2.0))
	pointBuilder.AppendNull()
	pointBuilder.Append(geoarrow.NewPointValue(3.0, 4.0))

	record := recBuilder.NewRecordBatch()
	defer record.Release()

	require.Equal(t, int64(3), record.NumRows())

	// Marshal to JSON
	b, err := record.MarshalJSON()
	require.NoError(t, err)
	require.Contains(t, string(b), "null")
}

func TestPointRegistration(t *testing.T) {
	extType := arrow.GetExtensionType("geoarrow.point")
	require.NotNil(t, extType)
	require.Equal(t, "geoarrow.point", extType.ExtensionName())
}

func TestPointValuesMethod(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPointType()
	builder := typ.NewBuilder(mem).(*geoarrow.PointBuilder)
	defer builder.Release()

	builder.Append(geoarrow.NewPointValue(1.0, 2.0))
	builder.Append(geoarrow.NewPointValue(3.0, 4.0))

	arr := builder.NewArray()
	defer arr.Release()

	pointArr := arr.(*geoarrow.PointArray)
	values := pointArr.Values()
	require.Equal(t, 2, len(values))
	require.Equal(t, 1.0, values[0].X())
	require.Equal(t, 4.0, values[1].Y())
}

func TestPointMarshalJSON(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tests := []struct {
		name     string
		storage  arrow.DataType
		points   []geoarrow.PointValue
		expected string
	}{
		{
			name:     "XY",
			storage:  xyStorage(),
			points:   []geoarrow.PointValue{geoarrow.NewPointValue(1.5, 2.5), geoarrow.NewPointValue(3.0, 4.0)},
			expected: "[[1.5,2.5],[3,4]]",
		},
		{
			name:     "XYZ",
			storage:  xyzStorage(),
			points:   []geoarrow.PointValue{geoarrow.NewPointValueZ(1.0, 2.0, 3.0)},
			expected: "[[1,2,3]]",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			typ := geoarrow.NewPointType(geoarrow.PointWithStorage(tc.storage))
			builder := typ.NewBuilder(mem).(*geoarrow.PointBuilder)
			defer builder.Release()

			for _, p := range tc.points {
				builder.Append(p)
			}

			arr := builder.NewArray()
			defer arr.Release()

			pointArr := arr.(*geoarrow.PointArray)
			b, err := pointArr.MarshalJSON()
			require.NoError(t, err)
			require.JSONEq(t, tc.expected, string(b))
		})
	}
}
