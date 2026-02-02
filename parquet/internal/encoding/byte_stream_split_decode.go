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
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/debug"
)

var decodeByteStreamSplitBatchWidth4SIMD func(data []byte, nValues, stride int, out []byte)

// decodeByteStreamSplitBatchWidth4 decodes the batch of nValues raw bytes representing a 4-byte datatype provided by 'data',
// into the output buffer 'out' using BYTE_STREAM_SPLIT encoding.
// 'out' must have space for at least len(data) bytes.
func decodeByteStreamSplitBatchWidth4(data []byte, nValues, stride int, out []byte) {
	const width = 4
	debug.Assert(len(out) >= nValues*width, fmt.Sprintf("not enough space in output buffer for decoding, out: %d bytes, data: %d bytes", len(out), len(data)))
	for element := 0; element < nValues; element++ {
		out[width*element] = data[element]
		out[width*element+1] = data[stride+element]
		out[width*element+2] = data[2*stride+element]
		out[width*element+3] = data[3*stride+element]
	}
}

func decodeByteStreamSplitBatchWidth4V2(data []byte, nValues, stride int, out []byte) {
	const width = 4
	// Narrow slices to help the compiler eliminate bounds checks.
	s0 := data[:nValues]
	s1 := data[stride : stride+nValues]
	s2 := data[2*stride : 2*stride+nValues]
	s3 := data[3*stride : 3*stride+nValues]

	out = out[:width*nValues]
	out32 := unsafe.Slice((*uint32)(unsafe.Pointer(&out[0])), nValues)
	for i := range nValues {
		out32[i] = uint32(s0[i]) | uint32(s1[i])<<8 | uint32(s2[i])<<16 | uint32(s3[i])<<24
	}
}

// decodeByteStreamSplitBatchWidth8 decodes the batch of nValues raw bytes representing a 8-byte datatype provided by 'data',
// into the output buffer 'out' using BYTE_STREAM_SPLIT encoding.
// 'out' must have space for at least len(data) bytes.
func decodeByteStreamSplitBatchWidth8(data []byte, nValues, stride int, out []byte) {
	const width = 8
	debug.Assert(len(out) >= nValues*width, fmt.Sprintf("not enough space in output buffer for decoding, out: %d bytes, data: %d bytes", len(out), len(data)))
	for element := 0; element < nValues; element++ {
		out[width*element] = data[element]
		out[width*element+1] = data[stride+element]
		out[width*element+2] = data[2*stride+element]
		out[width*element+3] = data[3*stride+element]
		out[width*element+4] = data[4*stride+element]
		out[width*element+5] = data[5*stride+element]
		out[width*element+6] = data[6*stride+element]
		out[width*element+7] = data[7*stride+element]
	}
}

// decodeByteStreamSplitBatchFLBA decodes the batch of nValues FixedLenByteArrays provided by 'data',
// into the output slice 'out' using BYTE_STREAM_SPLIT encoding.
// 'out' must have space for at least nValues slices.
func decodeByteStreamSplitBatchFLBA(data []byte, nValues, stride, width int, out []parquet.FixedLenByteArray) {
	debug.Assert(len(out) >= nValues, fmt.Sprintf("not enough space in output slice for decoding, out: %d values, data: %d values", len(out), nValues))
	for stream := 0; stream < width; stream++ {
		for element := 0; element < nValues; element++ {
			encLoc := stride*stream + element
			out[element][stream] = data[encLoc]
		}
	}
}

// decodeByteStreamSplitBatchFLBAWidth2 decodes the batch of nValues FixedLenByteArrays of length 2 provided by 'data',
// into the output slice 'out' using BYTE_STREAM_SPLIT encoding.
// 'out' must have space for at least nValues slices.
func decodeByteStreamSplitBatchFLBAWidth2(data []byte, nValues, stride int, out []parquet.FixedLenByteArray) {
	debug.Assert(len(out) >= nValues, fmt.Sprintf("not enough space in output slice for decoding, out: %d values, data: %d values", len(out), nValues))
	for element := 0; element < nValues; element++ {
		out[element][0] = data[element]
		out[element][1] = data[stride+element]
	}
}

// decodeByteStreamSplitBatchFLBAWidth4 decodes the batch of nValues FixedLenByteArrays of length 4 provided by 'data',
// into the output slice 'out' using BYTE_STREAM_SPLIT encoding.
// 'out' must have space for at least nValues slices.
func decodeByteStreamSplitBatchFLBAWidth4(data []byte, nValues, stride int, out []parquet.FixedLenByteArray) {
	debug.Assert(len(out) >= nValues, fmt.Sprintf("not enough space in output slice for decoding, out: %d values, data: %d values", len(out), nValues))
	for element := 0; element < nValues; element++ {
		out[element][0] = data[element]
		out[element][1] = data[stride+element]
		out[element][2] = data[stride*2+element]
		out[element][3] = data[stride*3+element]
	}
}

// decodeByteStreamSplitBatchFLBAWidth8 decodes the batch of nValues FixedLenByteArrays of length 8 provided by 'data',
// into the output slice 'out' using BYTE_STREAM_SPLIT encoding.
// 'out' must have space for at least nValues slices.
func decodeByteStreamSplitBatchFLBAWidth8(data []byte, nValues, stride int, out []parquet.FixedLenByteArray) {
	debug.Assert(len(out) >= nValues, fmt.Sprintf("not enough space in output slice for decoding, out: %d values, data: %d values", len(out), nValues))
	for element := 0; element < nValues; element++ {
		out[element][0] = data[element]
		out[element][1] = data[stride+element]
		out[element][2] = data[stride*2+element]
		out[element][3] = data[stride*3+element]
		out[element][4] = data[stride*4+element]
		out[element][5] = data[stride*5+element]
		out[element][6] = data[stride*6+element]
		out[element][7] = data[stride*7+element]
	}
}
