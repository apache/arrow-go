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

package compress

import (
	"bytes"
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/stretchr/testify/assert"
)

func TestBrotliWriterPoolMemoryPolicy(t *testing.T) {
	for level := brotli.BestSpeed; level <= brotli.BestCompression; level++ {
		pool := brotliWriterPool(level)
		if level <= maxPooledBrotliLevel {
			assert.NotNil(t, pool, "level %d should be pooled", level)
			assert.Equal(t, brotliWriterPoolSize, cap(pool), "level %d pool size", level)
		} else {
			assert.Nil(t, pool, "level %d should not retain its workspace", level)
		}
	}

	for _, level := range []int{brotli.BestSpeed - 1, brotli.BestCompression + 1} {
		assert.Nil(t, brotliWriterPool(level), "invalid level %d should not be pooled", level)
	}
}

func TestBrotliHighQualityWritersAreNotRetained(t *testing.T) {
	const pattern = "parquet-page-data-pattern-0123456789abcdef"
	src := bytes.Repeat([]byte(pattern), (256*1024+len(pattern)-1)/len(pattern))
	codec := brotliCodec{}

	for level := maxPooledBrotliLevel + 1; level <= brotli.BestCompression; level++ {
		codec.EncodeLevel(nil, src, level)
		assert.Nil(t, brotliWriterPool(level), "level %d should not retain its workspace", level)
	}
}

func TestReleaseBrotliWriterIsBounded(t *testing.T) {
	pool := make(chan *brotli.Writer, brotliWriterPoolSize)
	w1 := brotli.NewWriterLevel(nil, brotli.DefaultCompression)
	w2 := brotli.NewWriterLevel(nil, brotli.DefaultCompression)

	releaseBrotliWriter(pool, w1)
	releaseBrotliWriter(pool, w2)

	assert.Len(t, pool, brotliWriterPoolSize)
}
