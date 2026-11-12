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

package encoding

import (
	"errors"
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/parquet"
)

// FixedLenByteArray and ByteArray are both []byte, so their slices share a layout.
func flbaAsByteArrays(v []parquet.FixedLenByteArray) []parquet.ByteArray {
	return unsafe.Slice((*parquet.ByteArray)(unsafe.SliceData(v)), len(v))
}

// DeltaByteArrayFixedLenByteArrayEncoder writes FixedLenByteArray values in the
// DELTA_BYTE_ARRAY encoding, which has the same layout as for ByteArray values.
type DeltaByteArrayFixedLenByteArrayEncoder struct {
	DeltaByteArrayEncoder

	flbaScratch []parquet.FixedLenByteArray
}

func (*DeltaByteArrayFixedLenByteArrayEncoder) Type() parquet.Type {
	return parquet.Types.FixedLenByteArray
}

// Put writes a slice of FixedLenByteArrays to the encoder, and panics if any
// value's length differs from the column's type length.
func (enc *DeltaByteArrayFixedLenByteArrayEncoder) Put(in []parquet.FixedLenByteArray) {
	for _, v := range in {
		if len(v) != enc.typeLen {
			panic(fmt.Sprintf("parquet: FLBA type requires fixed-length %d but got %d", enc.typeLen, len(v)))
		}
	}
	enc.DeltaByteArrayEncoder.Put(flbaAsByteArrays(in))
}

func (enc *DeltaByteArrayFixedLenByteArrayEncoder) PutSpaced(in []parquet.FixedLenByteArray, validBits []byte, validBitsOffset int64) {
	if validBits == nil {
		enc.Put(in)
		return
	}
	if cap(enc.flbaScratch) < len(in) {
		enc.flbaScratch = make([]parquet.FixedLenByteArray, len(in))
	} else {
		enc.flbaScratch = enc.flbaScratch[:len(in)]
	}
	defer clear(enc.flbaScratch)
	nvalid := spacedCompress(in, enc.flbaScratch, validBits, validBitsOffset)
	enc.Put(enc.flbaScratch[:nvalid])
}

// DeltaByteArrayFixedLenByteArrayDecoder reads FixedLenByteArray values written
// in the DELTA_BYTE_ARRAY encoding.
type DeltaByteArrayFixedLenByteArrayDecoder struct {
	DeltaByteArrayDecoder
}

func (DeltaByteArrayFixedLenByteArrayDecoder) Type() parquet.Type {
	return parquet.Types.FixedLenByteArray
}

func (d *DeltaByteArrayFixedLenByteArrayDecoder) Decode(out []parquet.FixedLenByteArray) (int, error) {
	return d.DeltaByteArrayDecoder.Decode(flbaAsByteArrays(out))
}

func (d *DeltaByteArrayFixedLenByteArrayDecoder) DecodeSpaced(out []parquet.FixedLenByteArray, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	toread := len(out) - nullCount
	values, err := d.Decode(out[:toread])
	if err != nil {
		return values, err
	}
	if values != toread {
		return values, errors.New("parquet: number of values / definition levels read did not match")
	}
	return spacedExpandSwap(out, nullCount, validBits, validBitsOffset), nil
}
