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
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/internal/flatbuf"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testCodecs = []struct {
	name  string
	codec flatbuf.CompressionType
	opt   Option
}{
	{"zstd", flatbuf.CompressionTypeZSTD, WithZstd()},
	{"lz4", flatbuf.CompressionTypeLZ4_FRAME, WithLZ4()},
}

func compressBuffer(t *testing.T, codec flatbuf.CompressionType, src []byte) []byte {
	t.Helper()

	var out bytes.Buffer
	c := getCompressor(codec)
	c.Reset(&out)
	_, err := c.Write(src)
	require.NoError(t, err)
	require.NoError(t, c.Close())
	return out.Bytes()
}

// compressibleBytes returns n deterministic bytes drawn from a small alphabet,
// so they compress but aren't trivially uniform.
func compressibleBytes(n int, seed int64) []byte {
	rng := rand.New(rand.NewSource(seed))
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(rng.Intn(16))
	}
	return out
}

func TestDecompressorRoundTrip(t *testing.T) {
	for _, tc := range testCodecs {
		t.Run(tc.name, func(t *testing.T) {
			dec := getDecompressor(tc.codec)
			defer dec.Close()

			for i, n := range []int{1, 100, 4096, 300_000, 17, 300_000} {
				want := compressibleBytes(n, int64(i))
				got := make([]byte, n)
				require.NoError(t, dec.Decompress(got, compressBuffer(t, tc.codec, want)), "size %d", n)
				assert.Equal(t, want, got, "size %d", n)
			}
		})
	}
}

func TestDecompressorEmptyDestination(t *testing.T) {
	for _, tc := range testCodecs {
		t.Run(tc.name, func(t *testing.T) {
			dec := getDecompressor(tc.codec)
			defer dec.Close()
			assert.NoError(t, dec.Decompress(nil, nil))
			assert.NoError(t, dec.Decompress([]byte{}, []byte{1, 2, 3}))
		})
	}
}

func TestDecompressorSizeMismatch(t *testing.T) {
	want := compressibleBytes(1000, 1)

	for _, tc := range testCodecs {
		t.Run(tc.name, func(t *testing.T) {
			dec := getDecompressor(tc.codec)
			defer dec.Close()
			compressed := compressBuffer(t, tc.codec, want)

			// the uncompressed length prefix claims more than the frame holds
			assert.Error(t, dec.Decompress(make([]byte, len(want)+1), compressed))

			// and the decoder is still usable afterwards
			got := make([]byte, len(want))
			require.NoError(t, dec.Decompress(got, compressed))
			assert.Equal(t, want, got)
		})
	}

	// zstd frames declare their content size, so a prefix that is too small
	// is caught instead of silently truncating.
	t.Run("zstd/short-destination", func(t *testing.T) {
		dec := getDecompressor(flatbuf.CompressionTypeZSTD)
		defer dec.Close()
		assert.Error(t, dec.Decompress(make([]byte, len(want)-1), compressBuffer(t, flatbuf.CompressionTypeZSTD, want)))
	})
}

func TestZstdDecompressorBoundedByDestination(t *testing.T) {
	// 64 MiB of zeros compresses to a few bytes
	compressed := compressBuffer(t, flatbuf.CompressionTypeZSTD, make([]byte, 64<<20))
	require.Less(t, len(compressed), 64<<10)

	dec := getDecompressor(flatbuf.CompressionTypeZSTD)
	defer dec.Close()

	dst := make([]byte, 16)
	err := dec.Decompress(dst, compressed)
	require.Error(t, err)
	assert.True(t, errors.Is(err, zstd.ErrDecoderSizeExceeded), "unexpected error: %v", err)
}

// A frame header can declare the content size up front. A hostile frame can
// declare far more than it holds, hoping the decoder allocates that much
// before it ever reads the (tiny) body. The cap limit has to reject it up
// front, without allocating: check both the error and the allocation, since a
// decoder that allocated first and failed afterwards would return an error too.
func TestZstdDecompressorRejectsHugeDeclaredSizeWithoutAllocating(t *testing.T) {
	const declared = 64 << 20

	// Hand-built 17-byte zstd frame that is well formed but lies about its
	// size: the header declares 64 MiB of content, the frame holds one byte.
	frame := []byte{0x28, 0xb5, 0x2f, 0xfd, 0xe0}
	frame = binary.LittleEndian.AppendUint64(frame, declared)
	frame = append(frame, 0x09, 0x00, 0x00, 0x41)

	var hdr zstd.Header
	require.NoError(t, hdr.Decode(frame))
	require.True(t, hdr.HasFCS)
	require.Equal(t, uint64(declared), hdr.FrameContentSize)

	dec := getDecompressor(flatbuf.CompressionTypeZSTD)
	defer dec.Close()

	// warm up, so decoder state that is created lazily isn't counted below
	dst := make([]byte, 100)
	require.NoError(t, dec.Decompress(dst, compressBuffer(t, flatbuf.CompressionTypeZSTD, compressibleBytes(100, 3))))

	// collect garbage first so GC bookkeeping stays out of the measured window
	runtime.GC()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := dec.Decompress(dst, frame)
	runtime.ReadMemStats(&after)

	require.Error(t, err)
	assert.True(t, errors.Is(err, zstd.ErrDecoderSizeExceeded), "unexpected error: %v", err)
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20),
		"decoding a frame declaring %d bytes allocated too much", declared)
}

// Reader destinations are allocated in 64-byte aligned blocks, so they usually
// have spare capacity past their length (e.g. len 100 has cap 128). Decoding
// must fill exactly len(dst) and reject frames of any other size, including
// ones that would fit in the spare capacity.
func TestZstdDecompressorSpareCapacity(t *testing.T) {
	const n, capacity = 100, 128

	dec := getDecompressor(flatbuf.CompressionTypeZSTD)
	defer dec.Close()

	compress := func(size int) []byte {
		return compressBuffer(t, flatbuf.CompressionTypeZSTD, bytes.Repeat([]byte{0xAB}, size))
	}

	t.Run("valid frame fills dst and leaves the spare capacity untouched", func(t *testing.T) {
		dst := make([]byte, n, capacity)
		require.NoError(t, dec.Decompress(dst, compress(n)))
		assert.Equal(t, bytes.Repeat([]byte{0xAB}, n), dst)
		assert.Equal(t, make([]byte, capacity-n), dst[n:capacity])
	})

	t.Run("frame larger than dst but within the spare capacity is rejected", func(t *testing.T) {
		dst := make([]byte, n, capacity)
		assert.Error(t, dec.Decompress(dst, compress(capacity-1)))
	})

	t.Run("frame larger than the spare capacity is rejected", func(t *testing.T) {
		dst := make([]byte, n, capacity)
		err := dec.Decompress(dst, compress(capacity+1))
		require.Error(t, err)
		assert.True(t, errors.Is(err, zstd.ErrDecoderSizeExceeded), "unexpected error: %v", err)
	})

	t.Run("decoder is still usable after the rejected frames", func(t *testing.T) {
		dst := make([]byte, n, capacity)
		require.NoError(t, dec.Decompress(dst, compress(n)))
		assert.Equal(t, bytes.Repeat([]byte{0xAB}, n), dst)
	})
}

func TestDecompressorRecoversAfterTruncatedInput(t *testing.T) {
	want := compressibleBytes(4096, 2)

	for _, tc := range testCodecs {
		t.Run(tc.name, func(t *testing.T) {
			dec := getDecompressor(tc.codec)
			defer dec.Close()
			compressed := compressBuffer(t, tc.codec, want)

			// lz4 frames are written without checksums, so flipped bytes can go
			// undetected; a truncated frame is always an error.
			truncated := compressed[:len(compressed)/2]
			assert.Error(t, dec.Decompress(make([]byte, len(want)), truncated))

			got := make([]byte, len(want))
			require.NoError(t, dec.Decompress(got, compressed))
			assert.Equal(t, want, got)
		})
	}
}

func TestReadCompressedConcurrently(t *testing.T) {
	for _, tc := range testCodecs {
		t.Run(tc.name, func(t *testing.T) {
			rec := benchmarkRecordBatch(4, 2048)
			defer rec.Release()

			var buf bytes.Buffer
			w := NewWriter(&buf, WithSchema(rec.Schema()), tc.opt)
			for range 8 {
				require.NoError(t, w.Write(rec))
			}
			require.NoError(t, w.Close())
			data := buf.Bytes()

			var wg sync.WaitGroup
			errs := make(chan error, 16)
			for range 16 {
				wg.Go(func() {
					for range 10 {
						rdr, err := NewReader(bytes.NewReader(data))
						if err != nil {
							errs <- err
							return
						}
						n := 0
						for rdr.Next() {
							if !array.RecordEqual(rec, rdr.RecordBatch()) {
								errs <- fmt.Errorf("record %d differs", n)
							}
							n++
						}
						err = rdr.Err()
						rdr.Release()
						if err == nil && n != 8 {
							err = fmt.Errorf("read %d batches, want 8", n)
						}
						if err != nil {
							errs <- err
							return
						}
					}
				})
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				assert.NoError(t, err)
			}
		})
	}
}
