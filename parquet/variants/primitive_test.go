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

package variants

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func diffByteArrays(t *testing.T, got, want []byte) {
	t.Helper()
	if diff := cmp.Diff(got, want); diff != "" {
		t.Fatalf("Incorrect encoding. Diff (-got, +want):\n%s", diff)
	}
}

func checkSize(t *testing.T, wantSize int, buf []byte) {
	t.Helper()
	if gotSize := len(buf); gotSize != wantSize {
		t.Errorf("Incorrect reported size: got %d, want %d", gotSize, wantSize)
	}
}

func TestBoolean(t *testing.T) {
	var b bytes.Buffer
	size := marshalBoolean(true, &b)
	encodedTrue := b.Bytes()
	checkSize(t, size, encodedTrue)
	diffByteArrays(t, encodedTrue, []byte{0b100})
	got, err := unmarshalBoolean(encodedTrue, 0)
	if err != nil {
		t.Fatalf("unmarshalBoolean(): %v", err)
	}
	if got != true {
		t.Fatalf("Incorrect boolean returned. Got %t, want true", got)
	}

	b.Reset()
	marshalBoolean(false, &b)
	encodedFalse := b.Bytes()
	diffByteArrays(t, encodedFalse, []byte{0b1000})
	got, err = unmarshalBoolean(encodedFalse, 0)
	if err != nil {
		t.Fatalf("unmarshalBoolean(): %v", err)
	}
	if got != false {
		t.Fatalf("Incorrect boolean returned. Got %t, want false", got)
	}
}

func TestInt(t *testing.T) {
	cases := []struct {
		name       string
		val        int64
		wantHdr    byte
		wantHexVal []byte
	}{
		{
			name:       "Positive Int8",
			val:        8,
			wantHdr:    0b1100,
			wantHexVal: []byte{0x08},
		},
		{
			name:       "Negative Int8",
			val:        -8,
			wantHdr:    0b1100,
			wantHexVal: []byte{0xF8},
		},
		{
			name:       "Positive Int16",
			val:        200,
			wantHdr:    0b10000,
			wantHexVal: []byte{0xC8, 0x00},
		},
		{
			name:       "NegativeInt16",
			val:        -200,
			wantHdr:    0b10000,
			wantHexVal: []byte{0x38, 0xFF},
		},
		{
			name:       "Positive Int32",
			val:        32768,
			wantHdr:    0b10100,
			wantHexVal: []byte{0x00, 0x80, 0x00, 0x00},
		},
		{
			name:       "Negative Int32",
			val:        -32768,
			wantHdr:    0b10100,
			wantHexVal: []byte{0x00, 0x80, 0xFF, 0xFF},
		},
		{
			name:       "Positive Int64",
			val:        9223372036854775807,
			wantHdr:    0b11000,
			wantHexVal: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F},
		},
		{
			name:       "Negative Int64",
			val:        -9223372036854775807,
			wantHdr:    0b11000,
			wantHexVal: []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var b bytes.Buffer
			size, err := marshalNumeric(c.val, &b)
			require.NoError(t, err)
			encoded := b.Bytes()
			checkSize(t, size, encoded)
			if gotHdr := encoded[0]; gotHdr != c.wantHdr {
				t.Fatalf("Incorrect header: got %x, want %x", gotHdr, c.wantHdr)
			}
			diffByteArrays(t, encoded[1:], c.wantHexVal)
			gotVal, err := decodeIntPhysical(encoded, 0)
			if err != nil {
				t.Fatalf("decodeIntPhysical(): %v", err)
			}
			if wantVal := c.val; gotVal != wantVal {
				t.Fatalf("Incorrect decoded value: got %d, want %d", gotVal, wantVal)
			}
		})
	}
}

func TestUUID(t *testing.T) {
	cases := []struct {
		name string
		uuid uuid.UUID
		want []byte
	}{
		{
			name: "UUID no padding",
			uuid: func() uuid.UUID {
				u, _ := uuid.FromBytes([]byte("sixteencharacter"))
				return u
			}(),
			want: []byte{
				0b1010000, // Basic primitive UUID
				's', 'i', 'x', 't', 'e', 'e', 'n',
				'c', 'h', 'a', 'r', 'a', 'c', 't', 'e', 'r',
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var b bytes.Buffer
			size, err := marshalUUID(c.uuid, &b)
			require.NoError(t, err)
			if size != 17 {
				t.Fatalf("Incorrect size. Got %d, want 17", size)
			}
			diff(t, b.Bytes(), c.want)

			gotUUID, err := unmarshalUUID(b.Bytes(), 0)
			if err != nil {
				t.Fatalf("unmarshalUUID(): %v", err)
			}
			gotUUIDBytes, _ := gotUUID.MarshalBinary()
			diff(t, gotUUIDBytes, c.want[1:])
		})
	}
}

func TestFloat(t *testing.T) {
	var b bytes.Buffer
	size, err := marshalNumeric(1.1, &b)
	require.NoError(t, err)
	encodedFloat := b.Bytes()
	checkSize(t, size, encodedFloat)
	diffByteArrays(t, encodedFloat, []byte{
		0b111000, // Primitive type, float
		0xCD,
		0xCC,
		0x8C,
		0x3F, // 0x3F8C CCCD ~= 1.1 encoded
	})
	got, err := unmarshalFloat(encodedFloat, 0)
	if err != nil {
		t.Fatalf("unmarshalFloat(): %v", err)
	}
	if want := float32(1.1); got != want {
		t.Fatalf("Incorrect float returned. Got %.2f, want %.2f", got, want)
	}
}

func TestDouble(t *testing.T) {
	var b bytes.Buffer
	size, err := marshalNumeric(float64(1.1), &b)
	require.NoError(t, err)
	encodedDouble := b.Bytes()
	checkSize(t, size, encodedDouble)
	diffByteArrays(t, encodedDouble, []byte{
		0b11100, // Primitive type, double
		0x9A,
		0x99,
		0x99,
		0x99,
		0x99,
		0x99,
		0xF1,
		0x3F, // 0x3FF1 9999 9999 999A ~= 1.1 encoded
	})
	got, err := unmarshalDouble(encodedDouble, 0)
	if err != nil {
		t.Fatalf("unmarshalDouble(): %v", err)
	}
	if want := float64(1.1); got != want {
		t.Fatalf("Incorrect double returned. Got %.2f, want %.2f", got, want)
	}
}

func mustMarshalPrimitive(t *testing.T, val any, opts ...MarshalOpts) []byte {
	t.Helper()
	var buf bytes.Buffer
	if _, err := marshalPrimitive(val, &buf, opts...); err != nil {
		t.Fatalf("marshalPrimitive(): %v", err)
	}
	return buf.Bytes()
}

func TestUnmarshalPrimitive(t *testing.T) {
	cases := []struct {
		name          string
		encoded       []byte
		offset        int
		unmarshalType reflect.Type
		want          any
		wantErr       bool
	}{
		{
			name:    "Unmarshal bool (with offset)",
			encoded: append([]byte{0, 0}, mustMarshalPrimitive(t, true)...),
			offset:  2,
			want:    true,
		},
		{
			name:    "Unmarshal into int (with offset)",
			encoded: append([]byte{0, 0}, mustMarshalPrimitive(t, 1)...), // Encodes to Int8
			offset:  2,
			want:    int(1),
		},
		{
			name:    "Unmarshal into int8",
			encoded: mustMarshalPrimitive(t, 1),
			want:    int8(1),
		},
		{
			name:    "Unmarshal into int16",
			encoded: mustMarshalPrimitive(t, 1),
			want:    int16(1),
		},
		{
			name:    "Unmarshal into int32",
			encoded: mustMarshalPrimitive(t, 1),
			want:    int32(1),
		},
		{
			name:    "Unmarshal into int64",
			encoded: mustMarshalPrimitive(t, 1),
			want:    int64(1),
		},
		{
			name:    "Unmarshal negative",
			encoded: mustMarshalPrimitive(t, -100),
			want:    -100,
		},
		{
			name:    "Unmarshal int into float32",
			encoded: mustMarshalPrimitive(t, 1),
			want:    float32(1),
		},
		{
			name:    "unmarsUnmarshalhal float32",
			encoded: mustMarshalPrimitive(t, float32(1.2)),
			want:    float32(1.2),
		},
		{
			name:    "Unmarshal float64 (with offset)",
			encoded: append([]byte{1, 1}, mustMarshalPrimitive(t, float64(1.2))...),
			offset:  2,
			want:    float64(1.2),
		},
		{
			name:    "Unmarshal timestamp into int64",
			encoded: mustMarshalPrimitive(t, time.Unix(123, 0).Local(), MarshalTimeNanos),
			want:    time.Unix(123, 0).UnixNano(),
		},
		{
			name:    "Unmarshal timestamp into time",
			encoded: mustMarshalPrimitive(t, time.UnixMilli(1742967183000)),
			want:    time.UnixMilli(1742967183000),
		},
		{
			name:    "Unmarshal timestamp into time (nanos)",
			encoded: mustMarshalPrimitive(t, time.UnixMilli(1742967183000), MarshalTimeNanos),
			want:    time.Unix(0, 1742967183000000000),
		},
		{
			name:    "Unmarshal short string with offset",
			encoded: append([]byte{0, 0}, mustMarshalPrimitive(t, "hello")...),
			offset:  2,
			want:    "hello",
		},
		{
			name:    "Unmarshal basic string",
			encoded: []byte{0b1000000, 0x03, 0x00, 0x00, 0x00, 'a', 'b', 'c'},
			want:    "abc",
		},
		{
			name:    "Unmarshal string into byte slice",
			encoded: mustMarshalPrimitive(t, "hello"),
			want:    []byte("hello"),
		},
		{
			name:    "Unmarshal binary into byte slice",
			encoded: mustMarshalPrimitive(t, []byte{'b', 'y', 't', 'e'}),
			want:    []byte("byte"),
		},
		{
			name:    "Unmarshal binary into string",
			encoded: mustMarshalPrimitive(t, []byte{'b', 'y', 't', 'e'}),
			want:    "byte",
		},
		{
			name:    "Unmarshal empty binary",
			encoded: mustMarshalPrimitive(t, []byte{}),
			want:    []byte{},
		},
		{
			name: "Unmarshal UUID",
			encoded: func() []byte {
				u, _ := uuid.FromBytes([]byte("sixteencharacter"))
				return mustMarshalPrimitive(t, u)
			}(),
			want: func() uuid.UUID {
				u, _ := uuid.FromBytes([]byte("sixteencharacter"))
				return u
			}(),
		},
		{
			name: "Unmarshal UUID to byte slice",
			encoded: func() []byte {
				u, _ := uuid.FromBytes([]byte("sixteencharacter"))
				return mustMarshalPrimitive(t, u)
			}(),
			want: []byte("sixteencharacter"),
		},
		{
			name: "Unmarshal UUID to string",
			encoded: func() []byte {
				u, _ := uuid.FromBytes([]byte("sixteencharacter"))
				return mustMarshalPrimitive(t, u)
			}(),
			want: "73697874-6565-6e63-6861-726163746572",
		},
		{
			name:          "Unmarshal into int8 would overflow",
			encoded:       mustMarshalPrimitive(t, 12345),
			unmarshalType: reflect.TypeOf(int8(0)),
			wantErr:       true,
		},
		{
			name:          "Cannot unmarshal int into non-int type",
			encoded:       mustMarshalPrimitive(t, 1),
			unmarshalType: reflect.TypeOf(string("")),
			wantErr:       true,
		},
		{
			name:          "Cannot unmarshal string into non-string type",
			encoded:       mustMarshalPrimitive(t, "hello"),
			unmarshalType: reflect.TypeOf(int(1)),
			wantErr:       true,
		},
		{
			name:          "Cannot unmarshal binary into non-binary type",
			encoded:       mustMarshalPrimitive(t, []byte{0, 1, 2}),
			unmarshalType: reflect.TypeOf(int(1)),
			wantErr:       true,
		},
		{
			name:          "Malformed value",
			encoded:       mustMarshalPrimitive(t, 256)[:1], // int16 is usually 3 bytes
			unmarshalType: reflect.TypeOf(int(1)),
			wantErr:       true,
		},
		{
			name:          "Short string out of bounds",
			encoded:       []byte{0b1001, 'a'},
			unmarshalType: reflect.TypeOf(""),
			wantErr:       true,
		},
		{
			name:          "Binary out of bounds",
			encoded:       []byte{0b111100, 0x09, 0x00, 0x00, 0x00, 'a', 'b', 'c'},
			unmarshalType: reflect.TypeOf([]byte{}),
			wantErr:       true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			typ := reflect.TypeOf(c.want)
			if c.unmarshalType != nil {
				typ = c.unmarshalType
			}
			got := reflect.New(typ)
			if err := unmarshalPrimitive(c.encoded, c.offset, got); err != nil {
				if c.wantErr {
					return
				}
				t.Fatalf("Unexpected error: %v", err)
			} else if c.wantErr {
				t.Fatalf("Got no error when one was expected")
			}

			diff(t, got.Elem().Interface(), c.want)
		})
	}
}

func TestString(t *testing.T) {
	cases := []struct {
		name        string
		str         string
		wantEncoded []byte
	}{
		{
			name: "Short string",
			str:  "short",
			wantEncoded: []byte{
				0b0010101, // Short string type, length=5
				's', 'h', 'o', 'r', 't',
			},
		},
		{
			name: "Basic string",
			str:  "abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890",
			wantEncoded: append([]byte{
				0b1000000,
				0x48, 0x00, 0x00, 0x00, // Length of 72
			}, []byte("abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890")...),
		},
		{
			name:        "Empty string",
			str:         "",
			wantEncoded: []byte{0b01}, // Short string basic type, length = 0
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var b bytes.Buffer
			size, err := marshalString(c.str, &b)
			require.NoError(t, err)
			checkSize(t, size, c.wantEncoded)

			gotEncoded := b.Bytes()
			diff(t, gotEncoded, c.wantEncoded)
		})
	}
}

func TestBinary(t *testing.T) {
	cases := []struct {
		name        string
		bin         []byte
		wantEncoded []byte
	}{
		{
			name: "Binary data",
			bin:  []byte("hello"),
			wantEncoded: []byte{
				0b111100,               // Primitive type, binary
				0x05, 0x00, 0x00, 0x00, // Length of 5
				'h', 'e', 'l', 'l', 'o',
			},
		},
		{
			name:        "Empty data",
			bin:         []byte{},
			wantEncoded: []byte{0b111100, 0x00, 0x00, 0x00, 0x00},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var b bytes.Buffer
			size, err := marshalBinary(c.bin, &b)
			require.NoError(t, err)
			checkSize(t, size, c.wantEncoded)
			diff(t, b.Bytes(), c.wantEncoded)
		})
	}
}

func TestTimestamp(t *testing.T) {
	cases := []struct {
		name    string
		nanos   bool
		ntz     bool
		wantHdr byte
	}{
		{
			name:    "Nanos NTZ",
			nanos:   true,
			ntz:     true,
			wantHdr: 0b1001100,
		},
		{
			name:    "Nanos",
			nanos:   true,
			wantHdr: 0b1001000,
		},
		{
			name:    "Micros NTZ",
			ntz:     true,
			wantHdr: 0b110100,
		},
		{
			name:    "Micros",
			wantHdr: 0b110000,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ref := time.UnixMicro(1000000000)
			if c.ntz {
				ref = ref.UTC()
			} else {
				ref = ref.Local()
			}
			var b bytes.Buffer
			size, err := marshalTimestamp(ref, c.nanos, &b)
			require.NoError(t, err)
			wantEncoded := []byte{c.wantHdr}
			if c.nanos {
				wantEncoded = append(wantEncoded, []byte{
					0x00,
					0x10,
					0xA5,
					0xD4,
					0xE8,
					0x00,
					0x00,
					0x00, // Binary encoding of 1,000,000,000,000
				}...)
			} else {
				wantEncoded = append(wantEncoded, []byte{
					0x00,
					0xCA,
					0x9A,
					0x3B,
					0x00,
					0x00,
					0x00,
					0x00, // Binary encoding of 1,000,000,000
				}...)
			}
			encodedTimestamp := b.Bytes()
			checkSize(t, size, encodedTimestamp)
			diffByteArrays(t, encodedTimestamp, wantEncoded)
			got, err := unmarshalTimestamp(b.Bytes(), 0)
			if err != nil {
				t.Fatalf("unmarshalTimestamp(): %v", err)
			}
			if want := ref; got != want {
				t.Fatalf("Timestamps differ: got %s, want %s", got, want)
			}
		})
	}
}

func TestDate(t *testing.T) {
	day := time.Unix(0, 0).Add(10000 * 24 * time.Hour)
	var b bytes.Buffer
	size, err := marshalNumeric(arrow.Date32FromTime(day), &b)
	require.NoError(t, err)
	encodedDate := b.Bytes()
	checkSize(t, size, encodedDate)
	diffByteArrays(t, encodedDate, []byte{
		0b101100, // Primitive type, date
		0x10,
		0x27,
		0x00,
		0x00, // 10000 = 0x0000 2710
	})
	got, err := unmarshalDate(encodedDate, 0)
	require.NoError(t, err)
	assert.Equal(t, got, day)
}
