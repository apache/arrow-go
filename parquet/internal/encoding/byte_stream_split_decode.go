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

var (
	decodeByteStreamSplitBatchWidth4InByteOrder func(data []byte, nValues, stride int, out []byte) = decodeByteStreamSplitBatchWidth4InByteOrderDefault
	decodeByteStreamSplitBatchWidth8InByteOrder func(data []byte, nValues, stride int, out []byte) = decodeByteStreamSplitBatchWidth8InByteOrderDefault
)

// decodeByteStreamSplitBatchFLBA decodes the batch of nValues FixedLenByteArrays provided by 'data',
// into the output slice 'out' using BYTE_STREAM_SPLIT encoding.
// 'out' must have space for at least nValues slices.
func decodeByteStreamSplitBatchFLBA(data []byte, nValues, stride, width int, out []parquet.FixedLenByteArray) {
	debug.Assert(len(out) >= nValues, fmt.Sprintf("not enough space in output slice for decoding, out: %d values, data: %d values", len(out), nValues))
	debug.Assert(len(data) >= width*stride, fmt.Sprintf("not enough data for decoding, data: %d bytes, expected at least: %d bytes", len(data), width*stride))
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
	const width = 2
	debug.Assert(len(out) >= nValues, fmt.Sprintf("not enough space in output slice for decoding, out: %d values, data: %d values", len(out), nValues))
	debug.Assert(len(data) >= width*stride, fmt.Sprintf("not enough data for decoding, data: %d bytes, expected at least: %d bytes", len(data), width*stride))
	s0 := data[:nValues]
	s1 := data[stride : stride+nValues]
	for i := range nValues {
		out16 := (*uint16)(unsafe.Pointer(&out[i][0]))
		*out16 = uint16(s0[i]) | uint16(s1[i])<<8
	}
}

// decodeByteStreamSplitBatchFLBAWidth4 decodes the batch of nValues FixedLenByteArrays of length 4 provided by 'data',
// into the output slice 'out' using BYTE_STREAM_SPLIT encoding.
// 'out' must have space for at least nValues slices.
func decodeByteStreamSplitBatchFLBAWidth4(data []byte, nValues, stride int, out []parquet.FixedLenByteArray) {
	const width = 4
	debug.Assert(len(out) >= nValues, fmt.Sprintf("not enough space in output slice for decoding, out: %d values, data: %d values", len(out), nValues))
	debug.Assert(len(data) >= width*stride, fmt.Sprintf("not enough data for decoding, data: %d bytes, expected at least: %d bytes", len(data), width*stride))
	s0 := data[:nValues]
	s1 := data[stride : stride+nValues]
	s2 := data[stride*2 : stride*2+nValues]
	s3 := data[stride*3 : stride*3+nValues]
	for i := range nValues {
		out32 := (*uint32)(unsafe.Pointer(&out[i][0]))
		*out32 = uint32(s0[i]) | uint32(s1[i])<<8 | uint32(s2[i])<<16 | uint32(s3[i])<<24
	}
}

// decodeByteStreamSplitBatchFLBAWidth8 decodes the batch of nValues FixedLenByteArrays of length 8 provided by 'data',
// into the output slice 'out' using BYTE_STREAM_SPLIT encoding.
// 'out' must have space for at least nValues slices.
func decodeByteStreamSplitBatchFLBAWidth8(data []byte, nValues, stride int, out []parquet.FixedLenByteArray) {
	const width = 8
	debug.Assert(len(out) >= nValues, fmt.Sprintf("not enough space in output slice for decoding, out: %d values, data: %d values", len(out), nValues))
	debug.Assert(len(data) >= width*stride, fmt.Sprintf("not enough data for decoding, data: %d bytes, expected at least: %d bytes", len(data), width*stride))
	s0 := data[:nValues]
	s1 := data[stride : stride+nValues]
	s2 := data[stride*2 : stride*2+nValues]
	s3 := data[stride*3 : stride*3+nValues]
	s4 := data[stride*4 : stride*4+nValues]
	s5 := data[stride*5 : stride*5+nValues]
	s6 := data[stride*6 : stride*6+nValues]
	s7 := data[stride*7 : stride*7+nValues]
	for i := range nValues {
		out64 := (*uint64)(unsafe.Pointer(&out[i][0]))
		*out64 = uint64(s0[i]) | uint64(s1[i])<<8 | uint64(s2[i])<<16 | uint64(s3[i])<<24 |
			uint64(s4[i])<<32 | uint64(s5[i])<<40 | uint64(s6[i])<<48 | uint64(s7[i])<<56
	}
}
