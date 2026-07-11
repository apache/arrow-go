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

package array_test

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/internal/arrdata"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/internal/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var typemap = map[arrow.DataType]reflect.Type{
	arrow.PrimitiveTypes.Int8:   reflect.TypeOf(int8(0)),
	arrow.PrimitiveTypes.Uint8:  reflect.TypeOf(uint8(0)),
	arrow.PrimitiveTypes.Int16:  reflect.TypeOf(int16(0)),
	arrow.PrimitiveTypes.Uint16: reflect.TypeOf(uint16(0)),
	arrow.PrimitiveTypes.Int32:  reflect.TypeOf(int32(0)),
	arrow.PrimitiveTypes.Uint32: reflect.TypeOf(uint32(0)),
	arrow.PrimitiveTypes.Int64:  reflect.TypeOf(int64(0)),
	arrow.PrimitiveTypes.Uint64: reflect.TypeOf(uint64(0)),
}

func TestIntegerArrsJSON(t *testing.T) {
	const N = 10
	types := []arrow.DataType{
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Uint64,
	}

	for _, tt := range types {
		t.Run(fmt.Sprint(tt), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			jsontest := make([]int, N)
			vals := reflect.MakeSlice(reflect.SliceOf(typemap[tt]), N, N)
			for i := 0; i < N; i++ {
				vals.Index(i).Set(reflect.ValueOf(i).Convert(typemap[tt]))
				jsontest[i] = i
			}

			data, _ := json.Marshal(jsontest)
			arr, _, err := array.FromJSON(mem, tt, bytes.NewReader(data))
			assert.NoError(t, err)
			defer arr.Release()

			assert.EqualValues(t, N, arr.Len())
			assert.Zero(t, arr.NullN())

			output, err := json.Marshal(arr)
			assert.NoError(t, err)
			assert.JSONEq(t, string(data), string(output))
		})
		t.Run(fmt.Sprint(tt)+" errors", func(t *testing.T) {
			_, _, err := array.FromJSON(memory.DefaultAllocator, tt, strings.NewReader(""))
			assert.Error(t, err)

			_, _, err = array.FromJSON(memory.DefaultAllocator, tt, strings.NewReader("["))
			assert.ErrorIs(t, err, io.ErrUnexpectedEOF)

			_, _, err = array.FromJSON(memory.DefaultAllocator, tt, strings.NewReader("0"))
			assert.Error(t, err)

			_, _, err = array.FromJSON(memory.DefaultAllocator, tt, strings.NewReader("{}"))
			assert.Error(t, err)

			_, _, err = array.FromJSON(memory.DefaultAllocator, tt, strings.NewReader("[[0]]"))
			assert.EqualError(t, err, "json: cannot unmarshal [ into Go value of type "+tt.Name())
		})
	}
}

type fromJSONSeekReader struct {
	*bytes.Reader
	seekFn     func(offset int64, whence int) (int64, error)
	seekCalled bool
}

func (r *fromJSONSeekReader) Seek(offset int64, whence int) (int64, error) {
	r.seekCalled = true
	if r.seekFn != nil {
		return r.seekFn(offset, whence)
	}
	return r.Reader.Seek(offset, whence)
}

func TestFromJSONStartOffsetSeekValidation(t *testing.T) {
	seekErr := errors.New("seek failed")
	tests := []struct {
		name            string
		reader          *fromJSONSeekReader
		offset          int64
		wantErr         error
		wantErrContains string
		wantSeek        bool
	}{
		{
			name:     "seek error",
			offset:   1,
			wantErr:  seekErr,
			wantSeek: true,
			reader: &fromJSONSeekReader{
				Reader: bytes.NewReader([]byte("[1]")),
				seekFn: func(int64, int) (int64, error) { return 0, seekErr },
			},
		},
		{
			name:            "wrong position",
			offset:          1,
			wantErrContains: "got 2, want 1",
			wantSeek:        true,
			reader: &fromJSONSeekReader{
				Reader: bytes.NewReader([]byte("[1]")),
				seekFn: func(offset int64, _ int) (int64, error) { return offset + 1, nil },
			},
		},
		{
			name:            "negative offset",
			offset:          -1,
			wantErrContains: "non-negative",
			reader:          &fromJSONSeekReader{Reader: bytes.NewReader([]byte("[1]"))},
		},
		{
			name:     "successful seek",
			offset:   4,
			wantSeek: true,
			reader:   &fromJSONSeekReader{Reader: bytes.NewReader([]byte("skip[1, 2]"))},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arr, _, err := array.FromJSON(memory.DefaultAllocator, arrow.PrimitiveTypes.Int32, tt.reader,
				array.WithStartOffset(tt.offset))
			if tt.name == "successful seek" {
				require.NoError(t, err)
				defer arr.Release()
				assert.Equal(t, 2, arr.Len())
				assert.Equal(t, int32(1), arr.(*array.Int32).Value(0))
				assert.Equal(t, int32(2), arr.(*array.Int32).Value(1))
				assert.True(t, tt.reader.seekCalled)
				return
			}

			require.Error(t, err)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else if tt.wantErrContains != "" {
				assert.ErrorContains(t, err, tt.wantErrContains)
			}
			assert.Equal(t, tt.wantSeek, tt.reader.seekCalled)
		})
	}
}

func TestStringsJSON(t *testing.T) {
	tests := []struct {
		jsonstring string
		values     []string
		valids     []bool
	}{
		{"[]", []string{}, []bool{}},
		{`["", "foo"]`, []string{"", "foo"}, nil},
		{`["", null]`, []string{"", ""}, []bool{true, false}},
		// NUL character in string
		{`["", "some\u0000char"]`, []string{"", "some\x00char"}, nil},
		// utf8 sequence in string
		{"[\"\xc3\xa9\"]", []string{"\xc3\xa9"}, nil},
		// bytes < 0x20 can be represented as JSON unicode escapes
		{`["\u0000\u001f"]`, []string{"\x00\x1f"}, nil},
	}

	for _, tt := range tests {
		t.Run("json "+tt.jsonstring, func(t *testing.T) {
			bldr := array.NewStringBuilder(memory.DefaultAllocator)
			defer bldr.Release()

			bldr.AppendValues(tt.values, tt.valids)
			expected := bldr.NewStringArray()
			defer expected.Release()

			arr, _, err := array.FromJSON(memory.DefaultAllocator, arrow.BinaryTypes.String, strings.NewReader(tt.jsonstring))
			assert.NoError(t, err)
			defer arr.Release()

			assert.Truef(t, array.Equal(expected, arr), "expected: %s\ngot: %s\n", expected, arr)

			data, err := json.Marshal(arr)
			assert.NoError(t, err)
			assert.JSONEq(t, tt.jsonstring, string(data))
		})
	}

	for _, tt := range tests {
		t.Run("large json "+tt.jsonstring, func(t *testing.T) {
			bldr := array.NewLargeStringBuilder(memory.DefaultAllocator)
			defer bldr.Release()

			bldr.AppendValues(tt.values, tt.valids)
			expected := bldr.NewLargeStringArray()
			defer expected.Release()

			arr, _, err := array.FromJSON(memory.DefaultAllocator, arrow.BinaryTypes.LargeString, strings.NewReader(tt.jsonstring))
			assert.NoError(t, err)
			defer arr.Release()

			assert.Truef(t, array.Equal(expected, arr), "expected: %s\ngot: %s\n", expected, arr)

			data, err := json.Marshal(arr)
			assert.NoError(t, err)
			assert.JSONEq(t, tt.jsonstring, string(data))
		})
	}

	t.Run("errors", func(t *testing.T) {
		_, _, err := array.FromJSON(memory.DefaultAllocator, arrow.BinaryTypes.String, strings.NewReader("[0]"))
		assert.Error(t, err)

		_, _, err = array.FromJSON(memory.DefaultAllocator, arrow.BinaryTypes.String, strings.NewReader("[[]]"))
		assert.Error(t, err)
	})
}

func TestStructArrayFromJSON(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	jsonStr := `[{"hello": 3.5, "world": true, "yo": "foo"},{"hello": 3.25, "world": false, "yo": "bar"}]`

	arr, _, err := array.FromJSON(mem, arrow.StructOf(
		arrow.Field{Name: "hello", Type: arrow.PrimitiveTypes.Float64},
		arrow.Field{Name: "world", Type: arrow.FixedWidthTypes.Boolean},
		arrow.Field{Name: "yo", Type: arrow.BinaryTypes.String},
	), strings.NewReader(jsonStr))
	assert.NoError(t, err)
	defer arr.Release()

	output, err := json.Marshal(arr)
	assert.NoError(t, err)
	assert.JSONEq(t, jsonStr, string(output))
}

func TestArrayFromJSONMulti(t *testing.T) {
	arr, _, err := array.FromJSON(memory.DefaultAllocator, arrow.StructOf(
		arrow.Field{Name: "hello", Type: arrow.PrimitiveTypes.Float64},
		arrow.Field{Name: "world", Type: arrow.FixedWidthTypes.Boolean},
		arrow.Field{Name: "yo", Type: arrow.BinaryTypes.String},
	), strings.NewReader("{\"hello\": 3.5, \"world\": true, \"yo\": \"foo\"}\n{\"hello\": 3.25, \"world\": false, \"yo\": \"bar\"}\n"),
		array.WithMultipleDocs())
	assert.NoError(t, err)
	defer arr.Release()

	assert.EqualValues(t, 2, arr.Len())
	assert.Zero(t, arr.NullN())
}

func TestNestedJSONArrs(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	jsonStr := `[{"hello": 1.5, "world": [1, 2, 3, 4], "yo": [{"foo": "2005-05-06", "bar": "15:02:04.123"},{"foo": "1956-01-02", "bar": "02:10:00"}]}]`

	arr, _, err := array.FromJSON(mem, arrow.StructOf(
		arrow.Field{Name: "hello", Type: arrow.PrimitiveTypes.Float64},
		arrow.Field{Name: "world", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32)},
		arrow.Field{Name: "yo", Type: arrow.FixedSizeListOf(2, arrow.StructOf(
			arrow.Field{Name: "foo", Type: arrow.FixedWidthTypes.Date32},
			arrow.Field{Name: "bar", Type: arrow.FixedWidthTypes.Time32ms},
		))},
	), strings.NewReader(jsonStr))
	assert.NoError(t, err)
	defer arr.Release()

	v, err := json.Marshal(arr)
	assert.NoError(t, err)
	assert.JSONEq(t, jsonStr, string(v))
}

func TestGetNullsFromJSON(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	jsonStr := `[
		{"yo": "thing", "arr": null, "nuf": {"ps": "今日は"}},
		{"yo": null, "nuf": {"ps": null}, "arr": []},
		{ "nuf": null, "yo": "今日は", "arr": [1,2,3]}
	]`

	rec, _, err := array.RecordFromJSON(mem, arrow.NewSchema([]arrow.Field{
		{Name: "yo", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "arr", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
		{Name: "nuf", Type: arrow.StructOf(arrow.Field{Name: "ps", Type: arrow.BinaryTypes.String, Nullable: true}), Nullable: true},
	}, nil), strings.NewReader(jsonStr))
	assert.NoError(t, err)
	defer rec.Release()

	assert.EqualValues(t, 3, rec.NumCols())
	assert.EqualValues(t, 3, rec.NumRows())

	data, err := json.Marshal(rec)
	assert.NoError(t, err)
	assert.JSONEq(t, jsonStr, string(data))
}

func TestDurationsJSON(t *testing.T) {
	tests := []struct {
		unit    arrow.TimeUnit
		jsonstr string
		values  []arrow.Duration
	}{
		{arrow.Second, `["1s", "2s", "3s", "4s", "5s"]`, []arrow.Duration{1, 2, 3, 4, 5}},
		{arrow.Millisecond, `["1ms", "2ms", "3ms", "4ms", "5ms"]`, []arrow.Duration{1, 2, 3, 4, 5}},
		{arrow.Microsecond, `["1us", "2us", "3us", "4us", "5us"]`, []arrow.Duration{1, 2, 3, 4, 5}},
		{arrow.Nanosecond, `["1ns", "2ns", "3ns", "4ns", "5ns"]`, []arrow.Duration{1, 2, 3, 4, 5}},
	}
	for _, tt := range tests {
		dtype := &arrow.DurationType{Unit: tt.unit}
		bldr := array.NewDurationBuilder(memory.DefaultAllocator, dtype)
		defer bldr.Release()

		bldr.AppendValues(tt.values, nil)
		expected := bldr.NewArray()
		defer expected.Release()

		arr, _, err := array.FromJSON(memory.DefaultAllocator, dtype, strings.NewReader(tt.jsonstr))
		assert.NoError(t, err)
		defer arr.Release()

		assert.Truef(t, array.Equal(expected, arr), "expected: %s\ngot: %s\n", expected, arr)
	}
}

func TestTimestampsJSON(t *testing.T) {
	tests := []struct {
		unit    arrow.TimeUnit
		jsonstr string
		values  []arrow.Timestamp
	}{
		{arrow.Second, `["1970-01-01", "2000-02-29", "3989-07-14", "1900-02-28"]`, []arrow.Timestamp{0, 951782400, 63730281600, -2203977600}},
		{arrow.Nanosecond, `["1970-01-01", "2000-02-29", "1900-02-28"]`, []arrow.Timestamp{0, 951782400000000000, -2203977600000000000}},
	}

	for _, tt := range tests {
		dtype := &arrow.TimestampType{Unit: tt.unit}
		bldr := array.NewTimestampBuilder(memory.DefaultAllocator, dtype)
		defer bldr.Release()

		bldr.AppendValues(tt.values, nil)
		expected := bldr.NewArray()
		defer expected.Release()

		arr, _, err := array.FromJSON(memory.DefaultAllocator, dtype, strings.NewReader(tt.jsonstr))
		assert.NoError(t, err)
		defer arr.Release()

		assert.Truef(t, array.Equal(expected, arr), "expected: %s\ngot: %s\n", expected, arr)
	}
}

func TestDateJSON(t *testing.T) {
	t.Run("date32", func(t *testing.T) {
		bldr := array.NewDate32Builder(memory.DefaultAllocator)
		defer bldr.Release()

		jsonstr := `["1970-01-06", null, "1970-02-12", 0]`
		jsonExp := `["1970-01-06", null, "1970-02-12", "1970-01-01"]`

		bldr.AppendValues([]arrow.Date32{5, 0, 42, 0}, []bool{true, false, true, true})
		expected := bldr.NewArray()
		defer expected.Release()

		arr, _, err := array.FromJSON(memory.DefaultAllocator, arrow.FixedWidthTypes.Date32, strings.NewReader(jsonstr))
		assert.NoError(t, err)
		defer arr.Release()

		assert.Truef(t, array.Equal(expected, arr), "expected: %s\ngot: %s\n", expected, arr)

		data, err := json.Marshal(arr)
		assert.NoError(t, err)
		assert.JSONEq(t, jsonExp, string(data))
	})
	t.Run("date64", func(t *testing.T) {
		bldr := array.NewDate64Builder(memory.DefaultAllocator)
		defer bldr.Release()

		jsonstr := `["1970-01-02", null, "2286-11-20", 86400000]`
		jsonExp := `["1970-01-02", null, "2286-11-20", "1970-01-02"]`

		bldr.AppendValues([]arrow.Date64{86400000, 0, 9999936000000, 86400000}, []bool{true, false, true, true})
		expected := bldr.NewArray()
		defer expected.Release()

		arr, _, err := array.FromJSON(memory.DefaultAllocator, arrow.FixedWidthTypes.Date64, strings.NewReader(jsonstr))
		assert.NoError(t, err)
		defer arr.Release()

		assert.Truef(t, array.Equal(expected, arr), "expected: %s\ngot: %s\n", expected, arr)

		data, err := json.Marshal(arr)
		assert.NoError(t, err)
		assert.JSONEq(t, jsonExp, string(data))
	})
}

func TestTimeJSON(t *testing.T) {
	tententen := 60*(60*(10)+10) + 10
	tests := []struct {
		dt       arrow.DataType
		jsonstr  string
		jsonexp  string
		valueadd int
	}{
		{arrow.FixedWidthTypes.Time32s, `[null, "10:10:10", 36610]`, `[null, "10:10:10", "10:10:10"]`, 123},
		{arrow.FixedWidthTypes.Time32ms, `[null, "10:10:10.123", 36610123]`, `[null, "10:10:10.123", "10:10:10.123"]`, 456},
		{arrow.FixedWidthTypes.Time64us, `[null, "10:10:10.123456", 36610123456]`, `[null, "10:10:10.123456", "10:10:10.123456"]`, 789},
		{arrow.FixedWidthTypes.Time64ns, `[null, "10:10:10.123456789", 36610123456789]`, `[null, "10:10:10.123456789", "10:10:10.123456789"]`, 0},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprint(tt.dt), func(t *testing.T) {
			defer func() {
				tententen = 1000*tententen + tt.valueadd
			}()

			bldr := array.NewBuilder(memory.DefaultAllocator, tt.dt)
			defer bldr.Release()

			switch tt.dt.ID() {
			case arrow.TIME32:
				bldr.(*array.Time32Builder).AppendValues([]arrow.Time32{0, arrow.Time32(tententen), arrow.Time32(tententen)}, []bool{false, true, true})
			case arrow.TIME64:
				bldr.(*array.Time64Builder).AppendValues([]arrow.Time64{0, arrow.Time64(tententen), arrow.Time64(tententen)}, []bool{false, true, true})
			}

			expected := bldr.NewArray()
			defer expected.Release()

			arr, _, err := array.FromJSON(memory.DefaultAllocator, tt.dt, strings.NewReader(tt.jsonstr))
			assert.NoError(t, err)
			defer arr.Release()

			assert.Truef(t, array.Equal(expected, arr), "expected: %s\ngot: %s\n", expected, arr)

			data, err := json.Marshal(arr)
			assert.NoError(t, err)
			assert.JSONEq(t, tt.jsonexp, string(data))
		})
	}
}

func TestDecimal128JSON(t *testing.T) {
	dt := &arrow.Decimal128Type{Precision: 10, Scale: 4}
	bldr := array.NewDecimal128Builder(memory.DefaultAllocator, dt)
	defer bldr.Release()

	bldr.AppendValues([]decimal128.Num{decimal128.FromU64(1234567), {}, decimal128.FromI64(-789000)}, []bool{true, false, true})
	expected := bldr.NewArray()
	defer expected.Release()

	arr, _, err := array.FromJSON(memory.DefaultAllocator, dt, strings.NewReader(`["123.4567", null, "-78.9000"]`))
	assert.NoError(t, err)
	defer arr.Release()

	assert.Truef(t, array.Equal(expected, arr), "expected: %s\ngot: %s\n", expected, arr)

	data, err := json.Marshal(arr)
	assert.NoError(t, err)
	assert.JSONEq(t, `["123.4567", null, "-78.9"]`, string(data))
}

func TestDecimal256JSON(t *testing.T) {
	dt := &arrow.Decimal256Type{Precision: 10, Scale: 4}
	bldr := array.NewDecimal256Builder(memory.DefaultAllocator, dt)
	defer bldr.Release()

	bldr.AppendValues([]decimal256.Num{decimal256.FromU64(1234567), {}, decimal256.FromI64(-789000)}, []bool{true, false, true})
	expected := bldr.NewArray()
	defer expected.Release()

	arr, _, err := array.FromJSON(memory.DefaultAllocator, dt, strings.NewReader(`["123.4567", null, "-78.9000"]`))
	assert.NoError(t, err)
	defer arr.Release()

	assert.Truef(t, array.Equal(expected, arr), "expected: %s\ngot: %s\n", expected, arr)

	data, err := json.Marshal(arr)
	assert.NoError(t, err)
	assert.JSONEq(t, `["123.4567", null, "-78.9"]`, string(data))
}

func TestArrRecordsJSONRoundTrip(t *testing.T) {
	for k, v := range arrdata.Records {
		if k == "decimal128" || k == "decimal256" || k == "fixed_width_types" {
			// test these separately since the sample data in the arrdata
			// records doesn't lend itself to exactness when going to/from
			// json. The fixed_width_types one uses negative values for
			// time32 and time64 which correctly get interpreted into times,
			// but re-encoding them in json produces the normalized positive
			// values instead of re-creating negative ones.
			// the decimal128/decimal256 values don't get parsed *exactly* due to fun
			// float weirdness due to their size, so smaller tests will work fine.
			continue
		}
		t.Run(k, func(t *testing.T) {
			var buf bytes.Buffer
			assert.NotPanics(t, func() {
				enc := json.NewEncoder(&buf)
				for _, r := range v {
					if err := enc.Encode(r); err != nil {
						panic(err)
					}
				}
			})

			rdr := bytes.NewReader(buf.Bytes())
			var cur int64

			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			for _, r := range v {
				rec, off, err := array.RecordFromJSON(mem, r.Schema(), rdr, array.WithStartOffset(cur))
				assert.NoError(t, err)
				defer rec.Release()

				assert.Truef(t, array.RecordApproxEqual(r, rec), "expected: %s\ngot: %s\n", r, rec)
				cur += off
			}
		})
	}
}

func TestStructBuilderJSONUnknownNested(t *testing.T) {
	dt := arrow.StructOf(
		arrow.Field{Name: "region", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "model", Type: arrow.PrimitiveTypes.Int32},
		arrow.Field{Name: "sales", Type: arrow.PrimitiveTypes.Float32})

	const data = `[
		{"region": "NY", "model": "3", "sales": 742.0},
		{"region": "CT", "model": "5", "sales": 742.0}
	]`

	const dataWithExtra = `[
		{"region": "NY", "model": "3", "sales": 742.0, "extra": 1234},
		{"region": "CT", "model": "5", "sales": 742.0, "extra_array": [1234], "extra_obj": {"nested": ["deeply"]}}
	]`

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	arr, _, err := array.FromJSON(mem, dt, strings.NewReader(data))
	require.NoError(t, err)
	require.NotNil(t, arr)
	defer arr.Release()

	arr2, _, err := array.FromJSON(mem, dt, strings.NewReader(dataWithExtra))
	require.NoError(t, err)
	require.NotNil(t, arr2)
	defer arr2.Release()

	assert.Truef(t, array.Equal(arr, arr2), "expected: %s\n actual: %s", arr, arr2)
}

func TestRecordBuilderUnmarshalJSONExtraFields(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "region", Type: arrow.BinaryTypes.String},
		{Name: "model", Type: arrow.PrimitiveTypes.Int32},
		{Name: "sales", Type: arrow.PrimitiveTypes.Float32},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	const data = `{"region": "NY", "model": "3", "sales": 742.0, "extra": 1234}
	{"region": "NY", "model": "3", "sales": 742.0, "extra_array": [1234], "extra_obj": {"nested": ["deeply"]}}`

	s := bufio.NewScanner(strings.NewReader(data))
	require.True(t, s.Scan())
	require.NoError(t, bldr.UnmarshalJSON(s.Bytes()))

	rec1 := bldr.NewRecordBatch()
	defer rec1.Release()

	require.True(t, s.Scan())
	require.NoError(t, bldr.UnmarshalJSON(s.Bytes()))

	rec2 := bldr.NewRecordBatch()
	defer rec2.Release()

	assert.Truef(t, array.RecordEqual(rec1, rec2), "expected: %s\nactual: %s", rec1, rec2)
}

func TestRecordFromJSONLargeInt64Default(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	const data = `[{"a": 9223372036854775807}, {"a": -9223372036854775808}]`
	batch, _, err := array.RecordFromJSON(mem, schema, strings.NewReader(data))
	require.NoError(t, err)
	require.NotNil(t, batch)
	defer batch.Release()

	col := batch.Column(0).(*array.Int64)
	assert.EqualValues(t, int64(9223372036854775807), col.Value(0))
	assert.EqualValues(t, int64(-9223372036854775808), col.Value(1))
}

func TestRecordFromJSONLargeInt64WithUseNumber(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	const data = `[{"a": 9223372036854775807}, {"a": -9223372036854775808}]`
	//nolint:staticcheck // SA1019: explicitly verifying deprecated WithUseNumber still works
	batch, _, err := array.RecordFromJSON(mem, schema, strings.NewReader(data), array.WithUseNumber())
	require.NoError(t, err)
	require.NotNil(t, batch)
	defer batch.Release()

	col := batch.Column(0).(*array.Int64)
	assert.EqualValues(t, int64(9223372036854775807), col.Value(0))
	assert.EqualValues(t, int64(-9223372036854775808), col.Value(1))
}

func TestRecordFromJSONLargeDuration(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.FixedWidthTypes.Duration_s},
	}, nil)

	const data = `[{"a": 9223372036854775807}]`
	batch, _, err := array.RecordFromJSON(mem, schema, strings.NewReader(data))
	require.NoError(t, err)
	require.NotNil(t, batch)
	defer batch.Release()

	col := batch.Column(0).(*array.Duration)
	assert.EqualValues(t, arrow.Duration(9223372036854775807), col.Value(0))
}

func TestRecordBuilderUnmarshalJSONLargeInt64(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	require.NoError(t, bldr.UnmarshalJSON([]byte(`{"a": 9223372036854775807}`)))

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	col := rec.Column(0).(*array.Int64)
	assert.EqualValues(t, int64(9223372036854775807), col.Value(0))
}

func TestRecordBuilderUnmarshalOnePreservesUserDecoderOptions(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	src := strings.NewReader(`{"a": 9223372036854775807}`)
	dec := json.NewDecoder(src)
	dec.UseNumber()

	require.NoError(t, bldr.UnmarshalOne(dec))

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	col := rec.Column(0).(*array.Int64)
	assert.EqualValues(t, int64(9223372036854775807), col.Value(0))
}

func TestDurationBuilderJSONStringInteger(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	arr, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Duration_s,
		strings.NewReader(`["9223372036854775807"]`))
	require.NoError(t, err)
	require.NotNil(t, arr)
	defer arr.Release()

	col := arr.(*array.Duration)
	assert.EqualValues(t, arrow.Duration(9223372036854775807), col.Value(0))
}

func TestTimestampBuilderJSONStringInteger(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	arr, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Timestamp_s,
		strings.NewReader(`["9223372036854775807"]`))
	require.NoError(t, err)
	require.NotNil(t, arr)
	defer arr.Release()

	col := arr.(*array.Timestamp)
	assert.EqualValues(t, arrow.Timestamp(9223372036854775807), col.Value(0))
}

func TestTime32BuilderJSONStringInteger(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	arr, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Time32s,
		strings.NewReader(`["2147483647"]`))
	require.NoError(t, err)
	require.NotNil(t, arr)
	defer arr.Release()

	col := arr.(*array.Time32)
	assert.EqualValues(t, arrow.Time32(2147483647), col.Value(0))
}

func TestTime64BuilderJSONStringInteger(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	arr, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Time64us,
		strings.NewReader(`["9223372036854775807"]`))
	require.NoError(t, err)
	require.NotNil(t, arr)
	defer arr.Release()

	col := arr.(*array.Time64)
	assert.EqualValues(t, arrow.Time64(9223372036854775807), col.Value(0))
}

func TestDate32BuilderJSONStringInteger(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	arr, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Date32,
		strings.NewReader(`["2147483647"]`))
	require.NoError(t, err)
	require.NotNil(t, arr)
	defer arr.Release()

	col := arr.(*array.Date32)
	assert.EqualValues(t, arrow.Date32(2147483647), col.Value(0))
}

func TestDate64BuilderJSONStringInteger(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	arr, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Date64,
		strings.NewReader(`["9223372036854775807"]`))
	require.NoError(t, err)
	require.NotNil(t, arr)
	defer arr.Release()

	col := arr.(*array.Date64)
	assert.EqualValues(t, arrow.Date64(9223372036854775807), col.Value(0))
}

func TestDurationBuilderJSONStringIntegerInvalid(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	_, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Duration_s,
		strings.NewReader(`["abc"]`))
	assert.Error(t, err)
}

func TestDurationBuilderJSONStringDurationFormat(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	arr, _, err := array.FromJSON(mem, arrow.FixedWidthTypes.Duration_s,
		strings.NewReader(`["3h2m0.5s"]`))
	require.NoError(t, err)
	require.NotNil(t, arr)
	defer arr.Release()

	col := arr.(*array.Duration)
	assert.EqualValues(t, arrow.Duration(10920), col.Value(0))
}

// TestJSONNumberStrictValidation verifies that with UseNumber always enabled
// (issue #804), invalid integer JSON inputs are rejected rather than silently
// truncated, wrapped, or coerced.
func TestJSONNumberStrictValidation(t *testing.T) {
	cases := []struct {
		name string
		dt   arrow.DataType
		json string
	}{
		{"Int64Fractional", arrow.PrimitiveTypes.Int64, `[1.5]`},
		{"Int8OutOfRangePositive", arrow.PrimitiveTypes.Int8, `[128]`},
		{"Int8OutOfRangeNegative", arrow.PrimitiveTypes.Int8, `[-129]`},
		{"Int16OutOfRange", arrow.PrimitiveTypes.Int16, `[32768]`},
		{"Uint8Negative", arrow.PrimitiveTypes.Uint8, `[-1]`},
		{"Uint8Fractional", arrow.PrimitiveTypes.Uint8, `[0.5]`},
		{"Uint16OutOfRange", arrow.PrimitiveTypes.Uint16, `[65536]`},
		{"Uint64Negative", arrow.PrimitiveTypes.Uint64, `[-1]`},
		{"Uint64ExactBoundary", arrow.PrimitiveTypes.Uint64, `[18446744073709551616]`},
		{"Uint64ExponentialOverflow", arrow.PrimitiveTypes.Uint64, `[1.8446744073709552e+19]`},
		{"DurationFractional", arrow.FixedWidthTypes.Duration_s, `[1.5]`},
		{"TimestampFractional", arrow.FixedWidthTypes.Timestamp_s, `[1.5]`},
		{"Date32Fractional", arrow.FixedWidthTypes.Date32, `[1.5]`},
		{"Date32OverflowPositive", arrow.FixedWidthTypes.Date32, `[2147483648]`},
		{"Date32OverflowNegative", arrow.FixedWidthTypes.Date32, `[-2147483649]`},
		{"Date64Fractional", arrow.FixedWidthTypes.Date64, `[1.5]`},
		{"Time32Fractional", arrow.FixedWidthTypes.Time32s, `[1.5]`},
		{"Time32OverflowPositive", arrow.FixedWidthTypes.Time32s, `[2147483648]`},
		{"Time32OverflowNegative", arrow.FixedWidthTypes.Time32s, `[-2147483649]`},
		{"Time64Fractional", arrow.FixedWidthTypes.Time64us, `[1.5]`},
		{"Int64NaNString", arrow.PrimitiveTypes.Int64, `["NaN"]`},
		{"Int64InfString", arrow.PrimitiveTypes.Int64, `["+Inf"]`},
		{"Uint64NaNString", arrow.PrimitiveTypes.Uint64, `["NaN"]`},
		{"Uint64InfString", arrow.PrimitiveTypes.Uint64, `["+Inf"]`},
		{"Uint64ExponentialOverflowString", arrow.PrimitiveTypes.Uint64, `["1.8446744073709552e+19"]`},
		{"Int64ExponentBeyondMantissa", arrow.PrimitiveTypes.Int64, `[9.007199254740993e15]`},
		{"Uint64ExponentBeyondMantissa", arrow.PrimitiveTypes.Uint64, `[9.007199254740993e15]`},
		{"Int64ExponentBeyondMantissaString", arrow.PrimitiveTypes.Int64, `["9.007199254740993e15"]`},
		{"Uint64ExponentBeyondMantissaString", arrow.PrimitiveTypes.Uint64, `["9.007199254740993e15"]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			arr, _, err := array.FromJSON(mem, tc.dt, strings.NewReader(tc.json))
			if err == nil {
				arr.Release()
				t.Fatalf("expected error for %s with input %s, got nil", tc.name, tc.json)
			}
		})
	}
}
