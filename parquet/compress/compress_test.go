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

package compress_test

import (
	"bytes"
	"io"
	"math/rand"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
)

const (
	RandomDataSize       = 3 * 1024 * 1024
	CompressibleDataSize = 8 * 1024 * 1024
)

func makeRandomData(size int) []byte {
	ret := make([]byte, size)
	r := rand.New(rand.NewSource(1234))
	r.Read(ret)
	return ret
}

func makeCompressibleData(size int) []byte {
	const base = "Apache Arrow is a cross-language development platform for in-memory data"

	data := make([]byte, size)
	n := copy(data, base)
	for i := n; i < len(data); i *= 2 {
		copy(data[i:], data[:i])
	}
	return data
}

func TestErrorForUnimplemented(t *testing.T) {
	_, err := compress.GetCodec(compress.Codecs.Lzo)
	assert.Error(t, err)

	_, err = compress.GetCodec(compress.Codecs.Lz4)
	assert.Error(t, err)
}

func TestCompressDataOneShot(t *testing.T) {
	tests := []struct {
		c compress.Compression
	}{
		{compress.Codecs.Uncompressed},
		{compress.Codecs.Snappy},
		{compress.Codecs.Gzip},
		{compress.Codecs.Brotli},
		{compress.Codecs.Zstd},
		{compress.Codecs.Lz4Raw},
		// {compress.Codecs.Lzo},
	}

	for _, tt := range tests {
		t.Run(tt.c.String(), func(t *testing.T) {
			codec, err := compress.GetCodec(tt.c)
			assert.NoError(t, err)
			data := makeCompressibleData(CompressibleDataSize)

			buf := make([]byte, codec.CompressBound(int64(len(data))))
			compressed := codec.Encode(buf, data)
			assert.Same(t, &buf[0], &compressed[0])

			out := make([]byte, len(data))
			uncompressed := codec.Decode(out, compressed)
			assert.Same(t, &out[0], &uncompressed[0])

			assert.Exactly(t, data, uncompressed)
		})
	}
}

func TestCompressReaderWriter(t *testing.T) {
	tests := []struct {
		c compress.Compression
	}{
		{compress.Codecs.Uncompressed},
		{compress.Codecs.Snappy},
		{compress.Codecs.Gzip},
		{compress.Codecs.Brotli},
		{compress.Codecs.Zstd},
		// {compress.Codecs.Lzo},
		// {compress.Codecs.Lz4},
	}

	for _, tt := range tests {
		t.Run(tt.c.String(), func(t *testing.T) {
			var buf bytes.Buffer
			codec, err := compress.GetCodec(tt.c)
			assert.NoError(t, err)
			streamingCodec, ok := codec.(compress.StreamingCodec)
			assert.True(t, ok)
			data := makeRandomData(RandomDataSize)

			wr := streamingCodec.NewWriter(&buf)

			const chunkSize = 1111
			input := data
			for len(input) > 0 {
				var (
					n   int
					err error
				)
				if len(input) > chunkSize {
					n, err = wr.Write(input[:chunkSize])
				} else {
					n, err = wr.Write(input)
				}

				assert.NoError(t, err)
				input = input[n:]
			}
			wr.Close()

			rdr := streamingCodec.NewReader(&buf)
			out, err := io.ReadAll(rdr)
			assert.NoError(t, err)
			assert.Exactly(t, data, out)
		})
	}
}

var marshalTests = []struct {
	text  string
	codec compress.Compression
}{
	{"UNCOMPRESSED", compress.Codecs.Uncompressed},
	{"SNAPPY", compress.Codecs.Snappy},
	{"GZIP", compress.Codecs.Gzip},
	{"LZO", compress.Codecs.Lzo},
	{"BROTLI", compress.Codecs.Brotli},
	{"LZ4", compress.Codecs.Lz4},
	{"ZSTD", compress.Codecs.Zstd},
	{"LZ4_RAW", compress.Codecs.Lz4Raw},
}

func TestMarshalText(t *testing.T) {
	for _, tt := range marshalTests {
		t.Run(tt.text, func(t *testing.T) {
			data, err := tt.codec.MarshalText()
			assert.NoError(t, err)
			assert.Equal(t, tt.text, string(data))
		})
	}
}

func TestUnmarshalText(t *testing.T) {
	for _, tt := range marshalTests {
		t.Run(tt.text, func(t *testing.T) {
			var compression compress.Compression
			err := compression.UnmarshalText([]byte(tt.text))
			assert.NoError(t, err)
			assert.Equal(t, tt.codec, compression)
		})
	}
}

func TestUnmarshalTextError(t *testing.T) {
	var compression compress.Compression
	err := compression.UnmarshalText([]byte("NO SUCH CODEC"))
	assert.EqualError(t, err, "not a valid CompressionCodec string")
}

// BenchmarkZstdPooledEncodeAll compares zstd EncodeAll throughput and allocation
// overhead for pooled encoders created with the default concurrency (GOMAXPROCS
// inner block encoders) vs concurrency=1 (single inner block encoder).
//
// Each inner block encoder carries a ~1 MiB history buffer allocated on first
// use (ensureHist). With the default, a pooled encoder pre-allocates GOMAXPROCS
// of these buffers even though EncodeAll only ever uses one at a time. Setting
// concurrency=1 eliminates the wasted allocations.
//
// The benchmark uses semi-random data (seeded random blocks mixed with repeated
// patterns) to exercise the encoder's history window realistically — matching
// typical parquet page payloads that contain a mix of unique and repeated values.
func BenchmarkZstdPooledEncodeAll(b *testing.B) {
	// 256 KiB of semi-random data — typical parquet page size.
	// Mix random and repeated segments so the encoder exercises its full
	// match-finding and history-window code paths.
	data := make([]byte, 256*1024)
	r := rand.New(rand.NewSource(42))
	r.Read(data)
	// Overwrite ~25% with repeated pattern to give the encoder something to match.
	pattern := []byte("parquet-page-data-pattern-0123456789abcdef")
	for i := 0; i < len(data)/4; i += len(pattern) {
		copy(data[i:], pattern)
	}

	for _, tc := range []struct {
		name        string
		concurrency int
	}{
		{"Default", 0},      // GOMAXPROCS inner encoders
		{"Concurrency1", 1}, // single inner encoder
	} {
		b.Run(tc.name, func(b *testing.B) {
			pool := &sync.Pool{
				New: func() interface{} {
					opts := []zstd.EOption{
						zstd.WithZeroFrames(true),
						zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(3)),
					}
					if tc.concurrency > 0 {
						opts = append(opts, zstd.WithEncoderConcurrency(tc.concurrency))
					}
					enc, _ := zstd.NewWriter(nil, opts...)
					return enc
				},
			}
			codec, err := compress.GetCodec(compress.Codecs.Zstd)
			if err != nil {
				b.Fatal(err)
			}
			dst := make([]byte, codec.CompressBound(int64(len(data))))

			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				enc := pool.Get().(*zstd.Encoder)
				enc.EncodeAll(data, dst[:0])
				enc.Reset(nil)
				pool.Put(enc)
			}
		})
	}
}
