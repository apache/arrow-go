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
	"encoding/binary"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// alpTestPack packs deltas at bitWidth bits each, least significant bit first,
// one bit at a time.
func alpTestPack(deltas []uint64, bitWidth int) []byte {
	packed := make([]byte, alpPackedSize(len(deltas), bitWidth))
	bit := 0
	for _, d := range deltas {
		for b := range bitWidth {
			packed[bit/8] |= byte(d>>b&1) << (bit % 8)
			bit++
		}
	}
	return packed
}

func alpTestDeltas(rng *rand.Rand, count, bitWidth int) []uint64 {
	deltas := make([]uint64, count)
	for i := range deltas {
		deltas[i] = rng.Uint64() >> (64 - bitWidth)
	}
	return deltas
}

// The lengths sit either side of the groups of 4 and of 32 that the loops
// work in, and include a vector the default size and one past it.
var alpTestVectorLengths = []int{1, 2, 3, 4, 5, 31, 32, 33, 63, 64, 65, 1000, 1024, 1027}

func TestALPDecodePacked(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	scratch := make([]uint32, 1027)
	for bitWidth := 1; bitWidth <= 64; bitWidth++ {
		for _, count := range alpTestVectorLengths {
			deltas := alpTestDeltas(rng, count, bitWidth)
			ref := rng.Uint64()
			// What follows the deltas must not reach the values: the loops
			// load it, and discard it.
			src := make([]byte, alpUnpackSize(count, bitWidth))
			for i := copy(src, alpTestPack(deltas, bitWidth)); i < len(src); i++ {
				src[i] = 0xFF
			}

			got := make([]float64, count)
			alpDecodePacked64(got, src, uint(bitWidth), ref, 1e3, 1e-7, scratch)
			for i, d := range deltas {
				want := float64(int64(d+ref)) * 1e3 * 1e-7
				require.Equal(t, math.Float64bits(want), math.Float64bits(got[i]),
					"double bitWidth=%d count=%d i=%d", bitWidth, count, i)
			}

			if bitWidth > 32 {
				continue
			}
			got32 := make([]float32, count)
			alpDecodePacked32(got32, src, uint(bitWidth), uint32(ref), 1e3, 1e-7, scratch)
			for i, d := range deltas {
				want := float32(int32(uint32(d)+uint32(ref))) * float32(1e3) * float32(1e-7)
				require.Equal(t, math.Float32bits(want), math.Float32bits(got32[i]),
					"float bitWidth=%d count=%d i=%d", bitWidth, count, i)
			}
		}
	}
}

func TestALPPatch(t *testing.T) {
	const vectorLen = 100
	for _, numExceptions := range []int{0, 1, 3, 4, 5, 8, 11, vectorLen} {
		positions := make([]byte, 2*numExceptions)
		values64 := make([]byte, 8*numExceptions)
		values32 := make([]byte, 4*numExceptions)
		want64, want32 := make([]float64, vectorLen), make([]float32, vectorLen)
		for e := range numExceptions {
			// Spread out and out of order, with NaN payloads that must
			// survive.
			pos := (e*37 + 5) % vectorLen
			bits := 0x7FF8_0000_0000_0000 | uint64(e+1)
			binary.LittleEndian.PutUint16(positions[2*e:], uint16(pos))
			binary.LittleEndian.PutUint64(values64[8*e:], bits)
			binary.LittleEndian.PutUint32(values32[4*e:], 0x7FC0_0000|uint32(e+1))
			want64[pos] = math.Float64frombits(bits)
			want32[pos] = math.Float32frombits(0x7FC0_0000 | uint32(e+1))
		}

		got64, got32 := make([]float64, vectorLen), make([]float32, vectorLen)
		require.Equal(t, -1, alpPatch64(got64, positions, values64))
		require.Equal(t, -1, alpPatch32(got32, positions, values32))
		for i := range vectorLen {
			require.Equal(t, math.Float64bits(want64[i]), math.Float64bits(got64[i]), "exceptions=%d i=%d", numExceptions, i)
			require.Equal(t, math.Float32bits(want32[i]), math.Float32bits(got32[i]), "exceptions=%d i=%d", numExceptions, i)
		}

		// A position outside the vector is reported wherever it falls among
		// the exceptions.
		for bad := range numExceptions {
			corrupt := append([]byte(nil), positions...)
			binary.LittleEndian.PutUint16(corrupt[2*bad:], vectorLen+7)
			assert.Equal(t, vectorLen+7, alpPatch64(make([]float64, vectorLen), corrupt, values64), "exceptions=%d bad=%d", numExceptions, bad)
			assert.Equal(t, vectorLen+7, alpPatch32(make([]float32, vectorLen), corrupt, values32), "exceptions=%d bad=%d", numExceptions, bad)
		}
	}
}

// alpTestPackedPage builds a page of one vector with no exceptions, whose packed
// deltas are the page's last bytes.
func alpTestPackedPage[T alpFloat](deltas []uint64, ref uint64, bitWidth int) []byte {
	page := []byte{alpCompressionMode, alpIntegerEncodingFOR, alpDefaultLogVectorSize}
	page = binary.LittleEndian.AppendUint32(page, uint32(len(deltas)))
	page = binary.LittleEndian.AppendUint32(page, 4) // the vector follows its own offset
	page = append(page, 0, 0)                        // exponent and factor: the integers are the values
	page = binary.LittleEndian.AppendUint16(page, 0)
	if alpEncodedBytes[T]() == 4 {
		page = binary.LittleEndian.AppendUint32(page, uint32(ref))
	} else {
		page = binary.LittleEndian.AppendUint64(page, ref)
	}
	page = append(page, byte(bitWidth))
	return append(page, alpTestPack(deltas, bitWidth)...)
}

// The last vector of a page has nothing after its packed deltas for the
// unpacking loops to load, at any width and any length.
func TestALPDecodeVectorEndingPage(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	for bitWidth := 0; bitWidth <= 64; bitWidth++ {
		for _, count := range []int{1, 2, 5, 31, 32, 33, 100, 1024} {
			deltas := make([]uint64, count)
			if bitWidth > 0 {
				deltas = alpTestDeltas(rng, count, bitWidth)
			}
			ref := rng.Uint64()

			dec64 := newALPDecoder[float64](format.Encoding_ALP, newFloat64Column())
			require.NoError(t, dec64.SetData(count, alpTestPackedPage[float64](deltas, ref, bitWidth)))
			got64 := make([]float64, count)
			n, err := dec64.Decode(got64)
			require.NoError(t, err, "double bitWidth=%d count=%d", bitWidth, count)
			require.Equal(t, count, n)
			for i, d := range deltas {
				require.Equal(t, float64(int64(d+ref)), got64[i], "double bitWidth=%d count=%d i=%d", bitWidth, count, i)
			}

			if bitWidth > 32 {
				continue
			}
			dec32 := newALPDecoder[float32](format.Encoding_ALP, newFloat32Column())
			require.NoError(t, dec32.SetData(count, alpTestPackedPage[float32](deltas, ref, bitWidth)))
			got32 := make([]float32, count)
			n, err = dec32.Decode(got32)
			require.NoError(t, err, "float bitWidth=%d count=%d", bitWidth, count)
			require.Equal(t, count, n)
			for i, d := range deltas {
				require.Equal(t, float32(int32(uint32(d)+uint32(ref))), got32[i], "float bitWidth=%d count=%d i=%d", bitWidth, count, i)
			}
		}
	}
}

// A read that starts on a vector and takes all of it is decoded straight into
// the caller's slice; any other goes through the decoder's buffer. Whatever the
// reads, the values are the same.
func TestALPDecodeReadSizes(t *testing.T) {
	const count = 5*alpDefaultVectorSize + 300
	values := make([]float64, count)
	for i := range values {
		values[i] = float64(i%10007) / 100
		if i%97 == 0 {
			values[i] = math.NaN()
		}
	}
	enc := newALPEncoder[float64](format.Encoding_ALP, newFloat64Column(), memory.DefaultAllocator)
	enc.Put(values)
	buf, err := enc.FlushValues()
	require.NoError(t, err)
	defer buf.Release()

	for _, sizes := range [][]int{
		{count},
		{alpDefaultVectorSize},
		{2 * alpDefaultVectorSize},
		{1000},
		{1},
		// Whole vectors, then part of one, then the rest of it and more.
		{alpDefaultVectorSize, 10, 2*alpDefaultVectorSize + 500, alpDefaultVectorSize, 3},
		{alpDefaultVectorSize + 1, alpDefaultVectorSize - 1, alpDefaultVectorSize},
	} {
		dec := newALPDecoder[float64](format.Encoding_ALP, newFloat64Column())
		require.NoError(t, dec.SetData(count, buf.Bytes()))
		got := make([]float64, 0, count)
		for i := 0; len(got) < count; i++ {
			size := min(sizes[i%len(sizes)], count-len(got))
			n, err := dec.Decode(got[len(got) : len(got)+size])
			require.NoError(t, err, "sizes=%v", sizes)
			require.Equal(t, size, n, "sizes=%v", sizes)
			got = got[:len(got)+n]
		}
		for i := range values {
			require.Equal(t, math.Float64bits(values[i]), math.Float64bits(got[i]), "sizes=%v i=%d", sizes, i)
		}
	}
}
