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
	"fmt"
	"io"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/internal/debug"
	"github.com/apache/arrow-go/v18/arrow/internal/flatbuf"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

type compressor interface {
	MaxCompressedLen(n int) int
	Reset(io.Writer)
	io.WriteCloser
	Type() flatbuf.CompressionType
}

type lz4Compressor struct {
	*lz4.Writer
}

func (lz4Compressor) MaxCompressedLen(n int) int {
	return lz4.CompressBlockBound(n)
}

func (lz4Compressor) Type() flatbuf.CompressionType {
	return flatbuf.CompressionTypeLZ4_FRAME
}

type zstdCompressor struct {
	*zstd.Encoder
}

// from zstd.h, ZSTD_COMPRESSBOUND
func (zstdCompressor) MaxCompressedLen(len int) int {
	debug.Assert(len >= 0, "MaxCompressedLen called with len less than 0")
	extra := uint((uint(128<<10) - uint(len)) >> 11)
	if len >= (128 << 10) {
		extra = 0
	}
	return int(uint(len+(len>>8)) + extra)
}

func (zstdCompressor) Type() flatbuf.CompressionType {
	return flatbuf.CompressionTypeZSTD
}

func getCompressor(codec flatbuf.CompressionType) compressor {
	switch codec {
	case flatbuf.CompressionTypeLZ4_FRAME:
		w := lz4.NewWriter(nil)
		// options here chosen in order to match the C++ implementation
		w.Apply(lz4.ChecksumOption(false), lz4.BlockSizeOption(lz4.Block64Kb))
		return &lz4Compressor{w}
	case flatbuf.CompressionTypeZSTD:
		enc, err := zstd.NewWriter(nil)
		if err != nil {
			panic(err)
		}
		return zstdCompressor{enc}
	}
	return nil
}

type decompressor interface {
	Decompress(dst, src []byte) error
	Close()
}

var zstdDecompressorPool = sync.Pool{
	New: func() any {
		// WithDecoderConcurrency(1): Each pooled decoder is used by one goroutine at a time, so a single
		// block decoder is enough. The default would create up to four that
		// could never run in parallel;
		//
		// WithDecodeAllCapLimit(true): The cap limit bounds DecodeAll to cap(dst), so a frame claiming a
		// larger content size can't allocate beyond the buffer the caller
		// sized from the uncompressed length prefix.
		dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecodeAllCapLimit(true))
		if err != nil {
			panic(err)
		}
		return &zstdDecompressor{Decoder: dec}
	},
}

type zstdDecompressor struct {
	*zstd.Decoder
}

func (z *zstdDecompressor) Decompress(dst, src []byte) error {
	if len(dst) == 0 {
		return nil
	}

	// The decoder was created with WithDecodeAllCapLimit, so it decodes into
	// dst's own capacity and fails rather than allocating a larger slice.
	out, err := z.DecodeAll(src, dst[:0])
	if err != nil {
		return err
	}
	// Catch cases where the prefix says fewer bytes than the content, but the content fits in the dst's spare capacity
	if len(out) != len(dst) {
		return fmt.Errorf("arrow/ipc: zstd decompressed to %d bytes, expected %d", len(out), len(dst))
	}
	return nil
}

func (z *zstdDecompressor) Close() {
	zstdDecompressorPool.Put(z)
}

type lz4Decompressor struct {
	*lz4.Reader
}

func (z *lz4Decompressor) Decompress(dst, src []byte) error {
	z.Reset(bytes.NewReader(src))
	_, err := io.ReadFull(z.Reader, dst)
	return err
}

func (z *lz4Decompressor) Close() {
	z.Reset(nil)
}

func getDecompressor(codec flatbuf.CompressionType) decompressor {
	switch codec {
	case flatbuf.CompressionTypeLZ4_FRAME:
		return &lz4Decompressor{lz4.NewReader(nil)}
	case flatbuf.CompressionTypeZSTD:
		return zstdDecompressorPool.Get().(*zstdDecompressor)
	}
	return nil
}

type bufferWriter struct {
	buf *memory.Buffer
	pos int
}

func (bw *bufferWriter) Write(p []byte) (n int, err error) {
	if bw.pos+len(p) >= bw.buf.Cap() {
		bw.buf.Reserve(bw.pos + len(p))
	}
	n = copy(bw.buf.Buf()[bw.pos:], p)
	bw.pos += n
	return
}
