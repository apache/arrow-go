package geoarrow_test

import (
	"bytes"
	"encoding/hex"
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

// ISO WKB for POINT(1 2) — little-endian, type=1 (Point), x=1.0, y=2.0
var testWKBPoint, _ = hex.DecodeString("0101000000000000000000f03f0000000000000040")

// ISO WKB for POINT Z(1 2 3) — little-endian, type=1001 (PointZ)
var testWKBPointZ, _ = hex.DecodeString("01e9030000000000000000f03f00000000000000400000000000000840")

func TestWKBTypeBasics(t *testing.T) {
	typ := geoarrow.NewWKBType()

	require.Equal(t, "geoarrow.wkb", typ.ExtensionName())
	require.True(t, typ.ExtensionEquals(typ))
	require.True(t, arrow.TypeEqual(typ.StorageType(), &arrow.LargeBinaryType{}))

	// Different storage types should not be equal
	typBinary := geoarrow.NewWKBType(geoarrow.WKBWithBinaryStorage())
	require.False(t, typ.ExtensionEquals(typBinary))

	// Same storage type should be equal
	typ2 := geoarrow.NewWKBType()
	require.True(t, typ.ExtensionEquals(typ2))
}

func TestWKBSerializationRoundTrip(t *testing.T) {
	typ := geoarrow.NewWKBType(geoarrow.WKBWithMetadata(geoarrow.Metadata{
		Edges: geoarrow.EdgeSpherical,
	}))

	serialized := typ.Serialize()
	deserialized, err := typ.Deserialize(typ.StorageType(), serialized)
	require.NoError(t, err)
	require.True(t, typ.ExtensionEquals(deserialized))
}

func TestWKBDeserializeStorageTypes(t *testing.T) {
	typ := geoarrow.NewWKBType()
	serialized := typ.Serialize()

	// Binary storage
	dt, err := typ.Deserialize(&arrow.BinaryType{}, serialized)
	require.NoError(t, err)
	require.Equal(t, arrow.BINARY, dt.StorageType().ID())

	// LargeBinary storage
	dt, err = typ.Deserialize(&arrow.LargeBinaryType{}, serialized)
	require.NoError(t, err)
	require.Equal(t, arrow.LARGE_BINARY, dt.StorageType().ID())

	// Unsupported storage
	_, err = typ.Deserialize(arrow.PrimitiveTypes.Int32, serialized)
	require.Error(t, err)
}

func TestWKBBytesValue(t *testing.T) {
	wkb := geoarrow.WKBBytes(testWKBPoint)
	require.Equal(t, geoarrow.XY, wkb.Dimension())
	require.Equal(t, geoarrow.PointID, wkb.GeometryType())
	require.False(t, wkb.IsEmpty())

	wkbZ := geoarrow.WKBBytes(testWKBPointZ)
	require.Equal(t, geoarrow.XYZ, wkbZ.Dimension())
	require.Equal(t, geoarrow.PointZID, wkbZ.GeometryType())

	// Empty WKB
	var empty geoarrow.WKBBytes
	require.True(t, empty.IsEmpty())
}

func TestWKBBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewWKBType()
	builder := typ.NewBuilder(mem).(*geoarrow.WKBBuilder)
	defer builder.Release()

	builder.Append(geoarrow.WKBBytes(testWKBPoint))
	builder.AppendNull()
	builder.Append(geoarrow.WKBBytes(testWKBPointZ))

	arr := builder.NewArray()
	defer arr.Release()

	require.Equal(t, 3, arr.Len())
	require.Equal(t, 1, arr.NullN())

	wkbArr := arr.(*geoarrow.WKBArray)
	require.Equal(t, geoarrow.WKBBytes(testWKBPoint), wkbArr.Value(0))
	require.True(t, wkbArr.Value(1).IsEmpty())
	require.Equal(t, geoarrow.WKBBytes(testWKBPointZ), wkbArr.Value(2))
}

func TestWKBBuilderBinaryStorage(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewWKBType(geoarrow.WKBWithBinaryStorage())
	builder := typ.NewBuilder(mem).(*geoarrow.WKBBuilder)
	defer builder.Release()

	builder.Append(geoarrow.WKBBytes(testWKBPoint))
	arr := builder.NewArray()
	defer arr.Release()

	wkbArr := arr.(*geoarrow.WKBArray)
	require.Equal(t, geoarrow.WKBBytes(testWKBPoint), wkbArr.Value(0))
}

func TestWKBAppendValues(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewWKBType()
	builder := typ.NewBuilder(mem).(*geoarrow.WKBBuilder)
	defer builder.Release()

	values := []geoarrow.WKBBytes{
		geoarrow.WKBBytes(testWKBPoint),
		geoarrow.WKBBytes(testWKBPointZ),
		geoarrow.WKBBytes(testWKBPoint),
	}
	valid := []bool{true, false, true}
	builder.AppendValues(values, valid)

	arr := builder.NewArray()
	defer arr.Release()

	require.Equal(t, 3, arr.Len())
	require.Equal(t, 1, arr.NullN())

	wkbArr := arr.(*geoarrow.WKBArray)
	require.Equal(t, geoarrow.WKBBytes(testWKBPoint), wkbArr.Value(0))
	require.True(t, arr.IsNull(1))
	require.Equal(t, geoarrow.WKBBytes(testWKBPoint), wkbArr.Value(2))
}

func TestWKBCreateFromStorage(t *testing.T) {
	typ := geoarrow.NewWKBType()

	bldr := array.NewBinaryBuilder(memory.DefaultAllocator, &arrow.LargeBinaryType{})
	defer bldr.Release()

	bldr.Append(testWKBPoint)
	bldr.AppendNull()
	bldr.Append(testWKBPointZ)

	storage := bldr.NewArray()
	defer storage.Release()

	arr := array.NewExtensionArrayWithStorage(typ, storage)
	defer arr.Release()

	require.Equal(t, 3, arr.Len())
	require.Equal(t, 1, arr.NullN())

	wkbArr, ok := arr.(*geoarrow.WKBArray)
	require.True(t, ok)

	require.Equal(t, geoarrow.WKBBytes(testWKBPoint), wkbArr.Value(0))
	require.True(t, wkbArr.Value(1).IsEmpty())
	require.Equal(t, geoarrow.WKBBytes(testWKBPointZ), wkbArr.Value(2))
}

func TestWKBStringRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewWKBType()
	builder := typ.NewBuilder(mem).(*geoarrow.WKBBuilder)
	defer builder.Release()

	builder.Append(geoarrow.WKBBytes(testWKBPoint))
	builder.AppendNull()
	builder.Append(geoarrow.WKBBytes(testWKBPointZ))

	arr := builder.NewArray()
	defer arr.Release()

	// Rebuild from ValueStr
	builder2 := typ.NewBuilder(mem).(*geoarrow.WKBBuilder)
	defer builder2.Release()

	for i := 0; i < arr.Len(); i++ {
		require.NoError(t, builder2.AppendValueFromString(arr.ValueStr(i)))
	}

	arr2 := builder2.NewArray()
	defer arr2.Release()

	require.True(t, array.Equal(arr, arr2))
}

func TestWKBJSONRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewWKBType()
	builder := typ.NewBuilder(mem).(*geoarrow.WKBBuilder)
	defer builder.Release()

	builder.Append(geoarrow.WKBBytes(testWKBPoint))
	builder.AppendNull()
	builder.Append(geoarrow.WKBBytes(testWKBPointZ))

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

func TestWKBIPCRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewWKBType()
	builder := typ.NewBuilder(mem).(*geoarrow.WKBBuilder)

	builder.Append(geoarrow.WKBBytes(testWKBPoint))
	builder.AppendNull()
	builder.Append(geoarrow.WKBBytes(testWKBPointZ))

	arr := builder.NewArray()
	defer arr.Release()
	builder.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "geom", Type: typ, Nullable: true}}, nil)
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

func TestWKBRecordBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewWKBType()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "geom", Type: typ},
	}, nil)
	recBuilder := array.NewRecordBuilder(mem, schema)
	defer recBuilder.Release()

	wkbBuilder := recBuilder.Field(0).(*geoarrow.WKBBuilder)
	wkbBuilder.Append(geoarrow.WKBBytes(testWKBPoint))
	wkbBuilder.AppendNull()

	record := recBuilder.NewRecordBatch()
	defer record.Release()

	require.Equal(t, int64(2), record.NumRows())
}

func TestWKBRegistration(t *testing.T) {
	extType := arrow.GetExtensionType("geoarrow.wkb")
	require.NotNil(t, extType)
	require.Equal(t, "geoarrow.wkb", extType.ExtensionName())
}

func TestWKBValuesMethod(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewWKBType()
	builder := typ.NewBuilder(mem).(*geoarrow.WKBBuilder)
	defer builder.Release()

	builder.Append(geoarrow.WKBBytes(testWKBPoint))
	builder.Append(geoarrow.WKBBytes(testWKBPointZ))

	arr := builder.NewArray()
	defer arr.Release()

	wkbArr := arr.(*geoarrow.WKBArray)
	values := wkbArr.Values()
	require.Equal(t, 2, len(values))
	require.Equal(t, geoarrow.WKBBytes(testWKBPoint), values[0])
	require.Equal(t, geoarrow.WKBBytes(testWKBPointZ), values[1])
}

func TestWKBMarshalJSON(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewWKBType()
	builder := typ.NewBuilder(mem).(*geoarrow.WKBBuilder)
	defer builder.Release()

	builder.Append(geoarrow.WKBBytes(testWKBPoint))
	builder.AppendNull()

	arr := builder.NewArray()
	defer arr.Release()

	wkbArr := arr.(*geoarrow.WKBArray)
	b, err := wkbArr.MarshalJSON()
	require.NoError(t, err)

	expectedHex := hex.EncodeToString(testWKBPoint)
	require.Contains(t, string(b), expectedHex)
	require.Contains(t, string(b), "null")
}
