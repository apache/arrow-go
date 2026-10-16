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

package ipc

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/internal/dictutils"
	"github.com/apache/arrow-go/v18/arrow/internal/flatbuf"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/internal/types"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRWSchema(t *testing.T) {
	meta := arrow.NewMetadata([]string{"k1", "k2", "k3"}, []string{"v1", "v2", "v3"})

	mType := arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)
	mType.SetItemNullable(false)
	for _, tc := range []struct {
		schema *arrow.Schema
		memo   dictutils.Memo
	}{
		{
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "f1", Type: arrow.PrimitiveTypes.Int64},
				{Name: "f2", Type: arrow.PrimitiveTypes.Uint16},
				{Name: "f3", Type: arrow.PrimitiveTypes.Float64},
				{Name: "f4", Type: mType},
			}, &meta),
			memo: dictutils.Memo{},
		},
	} {
		t.Run("", func(t *testing.T) {
			b := flatbuffers.NewBuilder(0)

			tc.memo.Mapper.ImportSchema(tc.schema)
			offset := schemaToFB(b, tc.schema, &tc.memo.Mapper)
			b.Finish(offset)

			buf := b.FinishedBytes()

			fb := flatbuf.GetRootAsSchema(buf, 0)
			got, err := schemaFromFB(fb, &tc.memo)
			if err != nil {
				t.Fatal(err)
			}

			if !got.Equal(tc.schema) {
				t.Fatalf("r/w schema failed:\ngot = %#v\nwant= %#v\n", got, tc.schema)
			}

			{
				got := got.Metadata()
				want := tc.schema.Metadata()
				if got.Len() != want.Len() {
					t.Fatalf("invalid metadata len: got=%d, want=%d", got.Len(), want.Len())
				}
				if got, want := got.Keys(), want.Keys(); !reflect.DeepEqual(got, want) {
					t.Fatalf("invalid metadata keys:\ngot =%v\nwant=%v\n", got, want)
				}
				if got, want := got.Values(), want.Values(); !reflect.DeepEqual(got, want) {
					t.Fatalf("invalid metadata values:\ngot =%v\nwant=%v\n", got, want)
				}
			}
		})
	}
}

func TestMapFieldMetadataRoundTrip(t *testing.T) {
	keyMeta := arrow.NewMetadata([]string{"my.key.id"}, []string{"100"})
	itemMeta := arrow.NewMetadata([]string{"my.item.id"}, []string{"200"})

	mapType := arrow.MapOfFields(
		arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Metadata: keyMeta},
		arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: true, Metadata: itemMeta},
	)
	mapType.KeysSorted = true
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "m", Type: mapType, Nullable: true},
	}, nil)

	var buf bytes.Buffer
	w := NewWriter(&buf, WithSchema(schema), WithAllocator(memory.DefaultAllocator))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), WithAllocator(memory.DefaultAllocator))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()

	roundTripped := r.Schema()
	if !roundTripped.Equal(schema) {
		t.Fatalf("schema mismatch after IPC round-trip:\ngot:  %s\nwant: %s", roundTripped, schema)
	}

	rtMap := roundTripped.Field(0).Type.(*arrow.MapType)
	if got, ok := rtMap.KeyField().Metadata.GetValue("my.key.id"); !ok || got != "100" {
		t.Fatalf("key field metadata lost after IPC round-trip: got keys=%v", rtMap.KeyField().Metadata.Keys())
	}
	if got, ok := rtMap.ItemField().Metadata.GetValue("my.item.id"); !ok || got != "200" {
		t.Fatalf("item field metadata lost after IPC round-trip: got keys=%v", rtMap.ItemField().Metadata.Keys())
	}
	assert.True(t, rtMap.ItemField().Nullable)
	assert.True(t, rtMap.KeysSorted)
}

func TestRWFooter(t *testing.T) {
	for _, tc := range []struct {
		schema *arrow.Schema
		dicts  []dataBlock
		recs   []dataBlock
	}{
		{
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "f1", Type: arrow.PrimitiveTypes.Int64},
				{Name: "f2", Type: arrow.PrimitiveTypes.Uint16},
				{Name: "f3", Type: arrow.PrimitiveTypes.Float64},
			}, nil),
			dicts: []dataBlock{
				fileBlock{offset: 1, meta: 2, body: 3},
				fileBlock{offset: 4, meta: 5, body: 6},
				fileBlock{offset: 7, meta: 8, body: 9},
			},
			recs: []dataBlock{
				fileBlock{offset: 0, meta: 10, body: 30},
				fileBlock{offset: 10, meta: 30, body: 60},
				fileBlock{offset: 20, meta: 30, body: 40},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			o := new(bytes.Buffer)

			err := writeFileFooter(tc.schema, tc.dicts, tc.recs, o)
			if err != nil {
				t.Fatal(err)
			}

			footer := flatbuf.GetRootAsFooter(o.Bytes(), 0)

			if got, want := MetadataVersion(footer.Version()), currentMetadataVersion; got != want {
				t.Errorf("invalid metadata version: got=%[1]d %#[1]x, want=%[2]d %#[2]x", int16(got), int16(want))
			}

			schema, err := schemaFromFB(footer.Schema(nil), nil)
			if err != nil {
				t.Fatal(err)
			}

			if !schema.Equal(tc.schema) {
				t.Fatalf("schema r/w error:\ngot= %v\nwant=%v", schema, tc.schema)
			}

			if got, want := footer.DictionariesLength(), len(tc.dicts); got != want {
				t.Fatalf("dicts len differ: got=%d, want=%d", got, want)
			}

			for i, dict := range tc.dicts {
				var blk flatbuf.Block
				if !footer.Dictionaries(&blk, i) {
					t.Fatalf("could not get dictionary %d", i)
				}
				got := fileBlock{offset: blk.Offset(), meta: blk.MetaDataLength(), body: blk.BodyLength()}
				want := dict
				if got != want {
					t.Errorf("dict[%d] differ:\ngot= %v\nwant=%v", i, got, want)
				}
			}

			if got, want := footer.RecordBatchesLength(), len(tc.recs); got != want {
				t.Fatalf("recs len differ: got=%d, want=%d", got, want)
			}

			for i, rec := range tc.recs {
				var blk flatbuf.Block
				if !footer.RecordBatches(&blk, i) {
					t.Fatalf("could not get record %d", i)
				}
				got := fileBlock{offset: blk.Offset(), meta: blk.MetaDataLength(), body: blk.BodyLength()}
				want := rec
				if got != want {
					t.Errorf("record[%d] differ:\ngot= %v\nwant=%v", i, got, want)
				}
			}
		})
	}
}

func exampleUUID(mem memory.Allocator) arrow.Array {
	extType := extensions.NewUUIDType()
	bldr := array.NewExtensionBuilder(mem, extType)
	defer bldr.Release()

	bldr.Builder.(*array.FixedSizeBinaryBuilder).AppendValues(
		[][]byte{nil, []byte("abcdefghijklmno0"), []byte("abcdefghijklmno1"), []byte("abcdefghijklmno2")},
		[]bool{false, true, true, true})

	return bldr.NewArray()
}

func TestUnrecognizedExtensionType(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	extArr := exampleUUID(pool)
	defer extArr.Release()

	batch := array.NewRecordBatch(
		arrow.NewSchema([]arrow.Field{
			{Name: "f0", Type: extArr.DataType(), Nullable: true}}, nil),
		[]arrow.Array{extArr}, 4)
	defer batch.Release()

	storageArr := extArr.(array.ExtensionArray).Storage()

	var buf bytes.Buffer
	wr := NewWriter(&buf, WithAllocator(pool), WithSchema(batch.Schema()))
	assert.NoError(t, wr.Write(batch))
	wr.Close()

	// unregister the uuid type before we read back the buffer so it is
	// unrecognized when reading back the record batch.
	assert.NoError(t, arrow.UnregisterExtensionType("arrow.uuid"))
	// re-register once the test is complete
	defer arrow.RegisterExtensionType(extensions.NewUUIDType())
	rdr, err := NewReader(&buf, WithAllocator(pool))
	defer rdr.Release()

	assert.NoError(t, err)
	assert.True(t, rdr.Next())

	rec := rdr.Record()
	assert.NotNil(t, rec)

	// create a record batch with the same data, but the field should contain the
	// extension metadata and be of the storage type instead of being the extension type.
	extMetadata := arrow.NewMetadata([]string{ExtensionTypeKeyName, ExtensionMetadataKeyName}, []string{"uuid", "uuid-serialized"})
	batchNoExt := array.NewRecordBatch(
		arrow.NewSchema([]arrow.Field{
			{Name: "f0", Type: storageArr.DataType(), Nullable: true, Metadata: extMetadata},
		}, nil), []arrow.Array{storageArr}, 4)
	defer batchNoExt.Release()

	assert.Truef(t, array.RecordEqual(rec, batchNoExt), "expected: %s\ngot: %s\n", batchNoExt, rec)
}

// registerExtensionType registers extType for the duration of the test if no
// extension type of that name is registered yet.
func registerExtensionType(t *testing.T, extType arrow.ExtensionType) {
	t.Helper()
	if arrow.GetExtensionType(extType.ExtensionName()) != nil {
		return
	}
	if err := arrow.RegisterExtensionType(extType); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { arrow.UnregisterExtensionType(extType.ExtensionName()) })
}

// fieldFBRoundTrip serializes field to a flatbuffers Field and returns that
// Field along with the arrow.Field read back from it.
func fieldFBRoundTrip(t *testing.T, field arrow.Field) (*flatbuf.Field, arrow.Field) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{field}, nil)
	var memo dictutils.Memo
	memo.Mapper.ImportSchema(schema)

	b := flatbuffers.NewBuilder(0)
	b.Finish(schemaToFB(b, schema, &memo.Mapper))
	fbSchema := flatbuf.GetRootAsSchema(b.FinishedBytes(), 0)

	var fbField flatbuf.Field
	if !fbSchema.Fields(&fbField, 0) {
		t.Fatal("could not read field from flatbuffer")
	}

	readMemo := dictutils.NewMemo()
	defer readMemo.Clear()
	got, err := schemaFromFB(fbSchema, &readMemo)
	if err != nil {
		t.Fatal(err)
	}
	return &fbField, got.Field(0)
}

func fbMetadata(t *testing.T, md customMetadataer) map[string]string {
	t.Helper()
	meta, err := metadataFromFB(md)
	if err != nil {
		t.Fatal(err)
	}
	return meta.ToMap()
}

func TestDictionaryOfExtensionMetadata(t *testing.T) {
	registerExtensionType(t, types.NewParametric1Type(1))
	registerExtensionType(t, types.NewDictExtensionType())

	uuidType := extensions.NewUUIDType()
	p1Type := types.NewParametric1Type(6)
	dictExtType := types.NewDictExtensionType()
	opaqueOfDictOfUUID := extensions.NewOpaqueType(
		&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: uuidType},
		"type", "vendor")
	dictOfNested := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: extensions.NewOpaqueType(arrow.StructOf(arrow.Field{
			Name: "d", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: uuidType},
		}), "type", "vendor"),
	}

	tests := []struct {
		name      string
		typ       arrow.DataType
		fieldMeta map[string]string
		dictMeta  map[string]string
	}{
		{
			name:     "dictionary of uuid",
			typ:      &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: uuidType},
			dictMeta: map[string]string{ExtensionTypeKeyName: "arrow.uuid", ExtensionMetadataKeyName: ""},
		},
		{
			name:     "dictionary of extension type with metadata",
			typ:      &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: p1Type, Ordered: true},
			dictMeta: map[string]string{ExtensionTypeKeyName: "parametric-type-1", ExtensionMetadataKeyName: p1Type.Serialize()},
		},
		{
			name:      "extension type with dictionary storage",
			typ:       dictExtType,
			fieldMeta: map[string]string{ExtensionTypeKeyName: "dict-extension", ExtensionMetadataKeyName: "dict-extension-serialized"},
		},
		{
			name:      "extension type with dictionary of extension type storage",
			typ:       opaqueOfDictOfUUID,
			fieldMeta: map[string]string{ExtensionTypeKeyName: "arrow.opaque", ExtensionMetadataKeyName: opaqueOfDictOfUUID.Serialize()},
			dictMeta:  map[string]string{ExtensionTypeKeyName: "arrow.uuid", ExtensionMetadataKeyName: ""},
		},
		{
			name:     "dictionary of extension type with nested dictionary storage",
			typ:      dictOfNested,
			dictMeta: map[string]string{ExtensionTypeKeyName: "arrow.opaque", ExtensionMetadataKeyName: dictOfNested.ValueType.(arrow.ExtensionType).Serialize()},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			field := arrow.Field{Name: "f", Type: tc.typ, Nullable: true,
				Metadata: arrow.NewMetadata([]string{"k"}, []string{"v"})}
			fbField, got := fieldFBRoundTrip(t, field)

			wantFieldMeta := map[string]string{"k": "v"}
			for k, v := range tc.fieldMeta {
				wantFieldMeta[k] = v
			}
			assert.Equal(t, wantFieldMeta, fbMetadata(t, fbField))

			encoding := fbField.Dictionary(nil)
			if assert.NotNil(t, encoding) {
				if tc.dictMeta == nil {
					assert.Zero(t, encoding.CustomMetadataLength())
				} else {
					assert.Equal(t, tc.dictMeta, fbMetadata(t, encoding))
				}
			}

			assert.Truef(t, got.Equal(field), "got=%s\nwant=%s", got, field)
		})
	}
}

func TestDictionaryOfExtensionRoundTrip(t *testing.T) {
	registerExtensionType(t, types.NewParametric1Type(1))
	registerExtensionType(t, types.NewDictExtensionType())

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	uuidValues := exampleUUID(pool)
	defer uuidValues.Release()

	p1Storage, _, err := array.FromJSON(pool, arrow.PrimitiveTypes.Int32, strings.NewReader(`[1, 2, null, 4]`))
	if err != nil {
		t.Fatal(err)
	}
	defer p1Storage.Release()
	p1Values := array.NewExtensionArrayWithStorage(types.NewParametric1Type(6), p1Storage)
	defer p1Values.Release()

	dictExt, _, err := array.FromJSON(pool, types.NewDictExtensionType(), strings.NewReader(`["a", "b", null, "a", "c"]`))
	if err != nil {
		t.Fatal(err)
	}
	defer dictExt.Release()

	indices, _, err := array.FromJSON(pool, arrow.PrimitiveTypes.Int32, strings.NewReader(`[3, 0, 1, null, 3]`))
	if err != nil {
		t.Fatal(err)
	}
	defer indices.Release()

	dictOfUUID := array.NewDictionaryArray(
		&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: uuidValues.DataType()}, indices, uuidValues)
	defer dictOfUUID.Release()
	dictOfP1 := array.NewDictionaryArray(
		&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: p1Values.DataType()}, indices, p1Values)
	defer dictOfP1.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "uuid", Type: dictOfUUID.DataType(), Nullable: true},
		{Name: "p1", Type: dictOfP1.DataType(), Nullable: true},
		{Name: "dict_ext", Type: dictExt.DataType(), Nullable: true},
	}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{dictOfUUID, dictOfP1, dictExt}, 5)
	defer batch.Release()

	for _, format := range []string{"stream", "file"} {
		t.Run(format, func(t *testing.T) {
			var (
				buf  bytes.Buffer
				recs []arrow.RecordBatch
			)
			switch format {
			case "stream":
				w := NewWriter(&buf, WithAllocator(pool), WithSchema(schema))
				assert.NoError(t, w.Write(batch))
				assert.NoError(t, w.Close())

				r, err := NewReader(&buf, WithAllocator(pool))
				if err != nil {
					t.Fatal(err)
				}
				defer r.Release()
				assert.Truef(t, r.Schema().Equal(schema), "got=%s\nwant=%s", r.Schema(), schema)
				for r.Next() {
					rec := r.RecordBatch()
					rec.Retain()
					recs = append(recs, rec)
				}
				assert.NoError(t, r.Err())
			case "file":
				w, err := NewFileWriter(&buf, WithAllocator(pool), WithSchema(schema))
				if err != nil {
					t.Fatal(err)
				}
				assert.NoError(t, w.Write(batch))
				assert.NoError(t, w.Close())

				r, err := NewFileReader(bytes.NewReader(buf.Bytes()), WithAllocator(pool))
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				assert.Truef(t, r.Schema().Equal(schema), "got=%s\nwant=%s", r.Schema(), schema)
				for i := 0; i < r.NumRecords(); i++ {
					rec, err := r.RecordBatch(i)
					if err != nil {
						t.Fatal(err)
					}
					rec.Retain()
					recs = append(recs, rec)
				}
			}

			if assert.Len(t, recs, 1) {
				assert.Truef(t, array.RecordEqual(batch, recs[0]), "got=%s\nwant=%s", recs[0], batch)
				assert.IsType(t, extensions.NewUUIDType(),
					recs[0].Column(0).(*array.Dictionary).Dictionary().DataType())
			}
			for _, rec := range recs {
				rec.Release()
			}
		})
	}
}

func TestDictionaryOfUnregisteredExtension(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	// an extension type whose name is never registered
	extType := types.NewParametric2Type(424242)
	require.Nil(t, arrow.GetExtensionType(extType.ExtensionName()))

	storage, _, err := array.FromJSON(pool, arrow.PrimitiveTypes.Int32, strings.NewReader(`[1, null, 3, 4]`))
	require.NoError(t, err)
	defer storage.Release()
	values := array.NewExtensionArrayWithStorage(extType, storage)
	defer values.Release()
	indices, _, err := array.FromJSON(pool, arrow.PrimitiveTypes.Int32, strings.NewReader(`[1, 0, null, 3]`))
	require.NoError(t, err)
	defer indices.Release()
	dictArr := array.NewDictionaryArray(
		&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: extType}, indices, values)
	defer dictArr.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "f0", Type: dictArr.DataType(), Nullable: true}}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{dictArr}, 4)
	defer batch.Release()

	var buf bytes.Buffer
	wr := NewWriter(&buf, WithAllocator(pool), WithSchema(schema))
	require.NoError(t, wr.Write(batch))
	require.NoError(t, wr.Close())

	rdr, err := NewReader(&buf, WithAllocator(pool))
	require.NoError(t, err)
	defer rdr.Release()

	// the dictionary keeps its storage value type and the field gets no
	// extension metadata
	storageDictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.PrimitiveTypes.Int32}
	want := arrow.NewSchema([]arrow.Field{{Name: "f0", Type: storageDictType, Nullable: true}}, nil)
	assert.Truef(t, rdr.Schema().Equal(want), "got=%s\nwant=%s", rdr.Schema(), want)
	assert.Zero(t, rdr.Schema().Field(0).Metadata.Len())

	require.True(t, rdr.Next())
	storageDict := array.NewDictionaryArray(storageDictType, indices, storage)
	defer storageDict.Release()
	assert.Truef(t, array.Equal(storageDict, rdr.RecordBatch().Column(0)),
		"got=%s\nwant=%s", rdr.RecordBatch().Column(0), storageDict)
}

// dictFieldSchemaFB builds a flatbuffers Schema with a single dictionary
// encoded field of the given value type, whose DictionaryEncoding has the
// given custom metadata.
func dictFieldSchemaFB(t *testing.T, valueType arrow.DataType, dictMeta arrow.Metadata) *flatbuf.Schema {
	t.Helper()
	b := flatbuffers.NewBuilder(0)

	fv := fieldVisitor{b: b, meta: make(map[string]string)}
	fv.visit(arrow.Field{Name: "f", Type: valueType})
	nameFB := b.CreateString("f")
	flatbuf.FieldStartChildrenVector(b, 0)
	kidsFB := b.EndVector(0)
	indexTypeFB := intToFB(b, 8, true)
	dictMetaFB := metadataToFB(b, dictMeta, flatbuf.DictionaryEncodingStartCustomMetadataVector)
	flatbuf.DictionaryEncodingStart(b)
	flatbuf.DictionaryEncodingAddId(b, 0)
	flatbuf.DictionaryEncodingAddIndexType(b, indexTypeFB)
	flatbuf.DictionaryEncodingAddCustomMetadata(b, dictMetaFB)
	dictFB := flatbuf.DictionaryEncodingEnd(b)

	flatbuf.FieldStart(b)
	flatbuf.FieldAddName(b, nameFB)
	flatbuf.FieldAddNullable(b, true)
	flatbuf.FieldAddTypeType(b, fv.dtype)
	flatbuf.FieldAddType(b, fv.offset)
	flatbuf.FieldAddDictionary(b, dictFB)
	flatbuf.FieldAddChildren(b, kidsFB)
	fieldFB := flatbuf.FieldEnd(b)

	flatbuf.SchemaStartFieldsVector(b, 1)
	b.PrependUOffsetT(fieldFB)
	fieldsFB := b.EndVector(1)
	flatbuf.SchemaStart(b)
	flatbuf.SchemaAddFields(b, fieldsFB)
	b.Finish(flatbuf.SchemaEnd(b))
	return flatbuf.GetRootAsSchema(b.FinishedBytes(), 0)
}

func TestDictionaryEncodingCustomMetadataOnRead(t *testing.T) {
	uuidStorage := &arrow.FixedSizeBinaryType{ByteWidth: 16}
	tests := []struct {
		name      string
		valueType arrow.DataType
		dictMeta  map[string]string
		want      arrow.DataType
		wantErr   string
	}{
		{
			name:      "non-extension keys are dropped",
			valueType: arrow.PrimitiveTypes.Int32,
			dictMeta:  map[string]string{"k": "v"},
			want:      arrow.PrimitiveTypes.Int32,
		},
		{
			name:      "unregistered extension name is dropped",
			valueType: arrow.PrimitiveTypes.Int32,
			dictMeta: map[string]string{ExtensionTypeKeyName: "arrow-go.test.never-registered",
				ExtensionMetadataKeyName: "data", "k": "v"},
			want: arrow.PrimitiveTypes.Int32,
		},
		{
			name:      "registered extension with other keys",
			valueType: uuidStorage,
			dictMeta:  map[string]string{ExtensionTypeKeyName: "arrow.uuid", "k": "v"},
			want:      extensions.NewUUIDType(),
		},
		{
			name:      "registered extension over the wrong storage type",
			valueType: arrow.PrimitiveTypes.Int32,
			dictMeta:  map[string]string{ExtensionTypeKeyName: "arrow.uuid"},
			wantErr:   "invalid storage type for UUIDType: int32",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			memo := dictutils.NewMemo()
			defer memo.Clear()
			got, err := schemaFromFB(dictFieldSchemaFB(t, tc.valueType, arrow.MetadataFrom(tc.dictMeta)), &memo)
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)

			want := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: tc.want}
			assert.Truef(t, arrow.TypeEqual(want, got.Field(0).Type), "got=%s\nwant=%s", got.Field(0).Type, want)
			assert.Zero(t, got.Field(0).Metadata.Len())
		})
	}
}

func TestExtensionTypeFromMetadataDoesNotMutateInput(t *testing.T) {
	keys := []string{"a", ExtensionTypeKeyName, "b"}
	vals := []string{"1", "arrow.uuid", "2"}
	md := arrow.NewMetadata(keys, vals)
	orig := md

	dt, err := extensionTypeFromMetadata(&arrow.FixedSizeBinaryType{ByteWidth: 16}, &md)
	require.NoError(t, err)
	assert.Equal(t, "arrow.uuid", dt.(arrow.ExtensionType).ExtensionName())
	assert.Equal(t, []string{"a", "b"}, md.Keys())
	assert.Equal(t, []string{"1", "2"}, md.Values())
	assert.Equal(t, keys, orig.Keys())
	assert.Equal(t, vals, orig.Values())
}

func TestDictionaryOfNestedDictionaryExtensionRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	// dictionary<values=opaque<struct<d: dictionary<values=uuid>>>>
	uuidValues := exampleUUID(pool)
	defer uuidValues.Release()
	innerIdx, _, err := array.FromJSON(pool, arrow.PrimitiveTypes.Int8, strings.NewReader(`[3, 1, null, 2, 1]`))
	require.NoError(t, err)
	defer innerIdx.Release()
	inner := array.NewDictionaryArray(
		&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: uuidValues.DataType()}, innerIdx, uuidValues)
	defer inner.Release()

	structArr, err := array.NewStructArrayWithFields([]arrow.Array{inner},
		[]arrow.Field{{Name: "d", Type: inner.DataType(), Nullable: true}})
	require.NoError(t, err)
	defer structArr.Release()
	opaqueType := extensions.NewOpaqueType(structArr.DataType(), "type", "vendor")
	opaqueValues := array.NewExtensionArrayWithStorage(opaqueType, structArr)
	defer opaqueValues.Release()

	outerIdx, _, err := array.FromJSON(pool, arrow.PrimitiveTypes.Int32, strings.NewReader(`[4, 0, null, 2, 2, 1]`))
	require.NoError(t, err)
	defer outerIdx.Release()
	outer := array.NewDictionaryArray(
		&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: opaqueType}, outerIdx, opaqueValues)
	defer outer.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "f", Type: outer.DataType(), Nullable: true}}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{outer}, int64(outer.Len()))
	defer batch.Release()

	var buf bytes.Buffer
	w := NewWriter(&buf, WithAllocator(pool), WithSchema(schema))
	require.NoError(t, w.Write(batch))
	require.NoError(t, w.Close())

	r, err := NewReader(&buf, WithAllocator(pool))
	require.NoError(t, err)
	defer r.Release()
	assert.Truef(t, r.Schema().Equal(schema), "got=%s\nwant=%s", r.Schema(), schema)
	require.True(t, r.Next())
	assert.Truef(t, array.RecordEqual(batch, r.RecordBatch()), "got=%s\nwant=%s", r.RecordBatch(), batch)
	assert.False(t, r.Next())
	assert.NoError(t, r.Err())
}

func TestDictionaryOfExtensionDeltas(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	uuidType := extensions.NewUUIDType()
	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: uuidType}
	schema := arrow.NewSchema([]arrow.Field{{Name: "f", Type: dictType, Nullable: true}}, nil)

	makeBatch := func(values, indices string) arrow.RecordBatch {
		dict, _, err := array.FromJSON(pool, uuidType, strings.NewReader(values))
		require.NoError(t, err)
		defer dict.Release()
		idx, _, err := array.FromJSON(pool, arrow.PrimitiveTypes.Int32, strings.NewReader(indices))
		require.NoError(t, err)
		defer idx.Release()
		arr := array.NewDictionaryArray(dictType, idx, dict)
		defer arr.Release()
		return array.NewRecordBatch(schema, []arrow.Array{arr}, int64(arr.Len()))
	}

	const (
		uuid1 = `"00000000-0000-0000-0000-000000000001"`
		uuid2 = `"00000000-0000-0000-0000-000000000002"`
		uuid3 = `"00000000-0000-0000-0000-000000000003"`
	)
	batches := []arrow.RecordBatch{
		makeBatch(`[`+uuid1+`, `+uuid2+`]`, `[0, 1, null, 0]`),
		makeBatch(`[`+uuid1+`, `+uuid2+`, `+uuid3+`]`, `[2, 0, 1, 2]`),
	}
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()

	var buf bytes.Buffer
	w := NewWriter(&buf, WithAllocator(pool), WithSchema(schema), WithDictionaryDeltas(true))
	for _, b := range batches {
		require.NoError(t, w.Write(b))
	}
	require.NoError(t, w.Close())

	// the second dictionary is written as a delta
	var deltas []bool
	mr := NewMessageReader(bytes.NewReader(buf.Bytes()), WithAllocator(pool))
	for {
		msg, err := mr.Message()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		if msg.Type() == MessageDictionaryBatch {
			var md flatbuf.DictionaryBatch
			initFB(&md, msg.msg.Header)
			deltas = append(deltas, md.IsDelta())
		}
	}
	mr.Release()
	assert.Equal(t, []bool{false, true}, deltas)

	r, err := NewReader(bytes.NewReader(buf.Bytes()), WithAllocator(pool))
	require.NoError(t, err)
	defer r.Release()
	assert.Truef(t, r.Schema().Equal(schema), "got=%s\nwant=%s", r.Schema(), schema)
	for i, want := range batches {
		require.True(t, r.Next(), "batch %d", i)
		got := r.RecordBatch()
		assert.Truef(t, array.RecordEqual(want, got), "batch %d: got=%s\nwant=%s", i, got, want)
		assert.IsType(t, uuidType, got.Column(0).(*array.Dictionary).Dictionary().DataType())
	}
	assert.False(t, r.Next())
	assert.NoError(t, r.Err())
}

func TestDictionaryLegacyFieldExtensionMetadata(t *testing.T) {
	// Writers that predate DictionaryEncoding.custom_metadata put the
	// extension keys of a dictionary's value type in Field.custom_metadata,
	// which reads as an extension type with dictionary storage.
	storageDictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: &arrow.FixedSizeBinaryType{ByteWidth: 16}}

	t.Run("registered", func(t *testing.T) {
		field := arrow.Field{Name: "f", Type: storageDictType, Nullable: true,
			Metadata: arrow.NewMetadata([]string{ExtensionMetadataKeyName, ExtensionTypeKeyName}, []string{"", "arrow.uuid"})}
		schema := arrow.NewSchema([]arrow.Field{field}, nil)
		var memo dictutils.Memo
		memo.Mapper.ImportSchema(schema)
		b := flatbuffers.NewBuilder(0)
		b.Finish(schemaToFB(b, schema, &memo.Mapper))

		readMemo := dictutils.NewMemo()
		_, err := schemaFromFB(flatbuf.GetRootAsSchema(b.FinishedBytes(), 0), &readMemo)
		assert.ErrorContains(t, err, "invalid storage type for UUIDType: dictionary")
	})

	t.Run("unregistered", func(t *testing.T) {
		field := arrow.Field{Name: "f", Type: storageDictType, Nullable: true,
			Metadata: arrow.NewMetadata([]string{ExtensionTypeKeyName}, []string{"unregistered"})}
		fbField, got := fieldFBRoundTrip(t, field)
		assert.Zero(t, fbField.Dictionary(nil).CustomMetadataLength())
		assert.Truef(t, got.Equal(field), "got=%s\nwant=%s", got, field)
	})
}
