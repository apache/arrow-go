package encoding

import (
	"fmt"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/debug"
)

var decodeByteStreamSplitBatchWidth4SIMD func(data []byte, nValues, stride int, out []byte)

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
