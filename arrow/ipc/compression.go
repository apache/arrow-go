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
	pool interface{ Put(any) }
}

// zstdEncoderPools holds reusable encoders: index 0 for the library default
// level, 1-4 for the zstd.EncoderLevel a WithZstdLevel value maps to. A new
// encoder allocates its window and match tables (about 2 MB at the default
// level), which dominates the cost of compressing a small record batch.
//
// Unlike decoders, encoders can be shared process-wide: they are only used
// through the streaming API (Reset, Write, Close), which keeps no channel
// between streams, so an encoder used inside a testing/synctest bubble can be
// reused outside it. EncodeAll would break that, since it receives from a
// channel the encoder makes on first use.
var zstdEncoderPools [zstd.SpeedBestCompression + 1]sync.Pool

func zstdEncoderPoolIndex(zstdLevel int) int {
	if zstdLevel <= 0 {
		return 0
	}
	return int(zstd.EncoderLevelFromZstd(zstdLevel))
}

// releaseCompressors returns the pooled compressors in cs and clears their
// slots, so calling it twice on the same slice is safe.
func releaseCompressors(cs []compressor) {
	for i, c := range cs {
		z, ok := c.(*zstdCompressor)
		if !ok {
			continue
		}
		// Reset waits for in-flight block goroutines, also after a failed
		// Write, and drops the reference to the last output buffer.
		z.Reset(nil)
		z.pool.Put(z)
		cs[i] = nil
	}
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

func getCompressor(codec flatbuf.CompressionType, zstdLevel int) compressor {
	switch codec {
	case flatbuf.CompressionTypeLZ4_FRAME:
		w := lz4.NewWriter(nil)
		// options here chosen in order to match the C++ implementation
		w.Apply(lz4.ChecksumOption(false), lz4.BlockSizeOption(lz4.Block64Kb))
		return &lz4Compressor{w}
	case flatbuf.CompressionTypeZSTD:
		pool := &zstdEncoderPools[zstdEncoderPoolIndex(zstdLevel)]
		if c, ok := pool.Get().(*zstdCompressor); ok {
			return c
		}
		var enc *zstd.Encoder
		var err error
		if zstdLevel > 0 {
			enc, err = zstd.NewWriter(nil,
				zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(zstdLevel)),
			)
		} else {
			enc, err = zstd.NewWriter(nil)
		}
		if err != nil {
			panic(err)
		}
		return &zstdCompressor{Encoder: enc, pool: pool}
	}
	return nil
}

type decompressor interface {
	// Decompress decodes src into dst, which must be sized to the exact
	// uncompressed length.
	Decompress(dst, src []byte) error
	// Close releases the decompressor; it must not be used afterwards.
	// Implementations may return it to a pool, so calling Close twice hands
	// the same decompressor to two owners.
	Close()
}

// zstdDecoders reuses zstd decoders within one reader. Decoders are not
// shared between readers: a zstd.Decoder owns a channel, and a channel made
// inside a testing/synctest bubble must not be used outside that bubble, so a
// decoder has to stay with the reader whose goroutines created it.
//
// The zero value is ready to use. It is safe for concurrent use, since
// FileReader.RecordBatchAt may be called concurrently, and keeps at most as
// many decoders as were in use at once.
type zstdDecoders struct {
	mu   sync.Mutex
	idle []*zstdDecompressor
}

func (c *zstdDecoders) get() *zstdDecompressor {
	c.mu.Lock()
	if n := len(c.idle); n > 0 {
		z := c.idle[n-1]
		c.idle = c.idle[:n-1]
		c.mu.Unlock()
		return z
	}
	c.mu.Unlock()

	// WithDecoderConcurrency(1): Each decoder is used by one goroutine at a time, so a single
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
	return &zstdDecompressor{Decoder: dec, owner: c}
}

type zstdDecompressor struct {
	*zstd.Decoder
	owner *zstdDecoders
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

// Close returns the decoder to its reader. z.Decoder.Close() is deliberately
// not called: zstd.NewReader(nil) starts no goroutines, so an idle decoder
// only holds GC-reclaimable state and there is nothing to tear down. Closing
// it would make the decoder unusable for the next borrower.
func (z *zstdDecompressor) Close() {
	c := z.owner
	c.mu.Lock()
	c.idle = append(c.idle, z)
	c.mu.Unlock()
}

type lz4Decompressor struct {
	*lz4.Reader
	extra [1]byte // probe buffer for trailing data
}

func (z *lz4Decompressor) Decompress(dst, src []byte) error {
	if len(dst) == 0 {
		return nil
	}

	z.Reset(bytes.NewReader(src))
	if _, err := io.ReadFull(z.Reader, dst); err != nil {
		return err
	}
	// ReadFull stops once dst is full, so probe for one more byte: if there is
	// one, the frame decompresses to more than the length prefix claims, and we
	// reject it rather than silently truncate (mirrors the zstd path).
	n, err := z.Read(z.extra[:])
	if n > 0 {
		return fmt.Errorf("arrow/ipc: lz4 decompressed to more than the expected %d bytes", len(dst))
	}
	if err != io.EOF {
		return err
	}
	return nil
}

func (z *lz4Decompressor) Close() {
	z.Reset(nil)
}

// getDecompressor returns a decompressor for codec. zstd decoders come from,
// and return to, zd.
func getDecompressor(codec flatbuf.CompressionType, zd *zstdDecoders) decompressor {
	switch codec {
	case flatbuf.CompressionTypeLZ4_FRAME:
		return &lz4Decompressor{Reader: lz4.NewReader(nil)}
	case flatbuf.CompressionTypeZSTD:
		return zd.get()
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
