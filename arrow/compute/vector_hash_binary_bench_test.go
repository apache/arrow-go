// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build go1.18

package compute_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkDictionaryEncodeBinary(b *testing.B) {
	const (
		nvalues = 1 << 16
		nunique = 100
	)

	mem := memory.DefaultAllocator
	ctx := compute.WithAllocator(context.Background(), mem)

	b.Run("string", func(b *testing.B) {
		values := make([]string, nvalues)
		bytes := 0
		for i := range values {
			values[i] = fmt.Sprintf("value-%08d", i%nunique)
			bytes += len(values[i])
		}

		builder := array.NewStringBuilder(mem)
		builder.AppendValues(values, nil)
		input := builder.NewStringArray()
		builder.Release()
		defer input.Release()

		benchmarkDictionaryEncodeBinary(b, ctx, input, bytes)
	})

	b.Run("fixed-size-binary-16", func(b *testing.B) {
		values := make([][]byte, nvalues)
		for i := range values {
			values[i] = make([]byte, 16)
			binary.LittleEndian.PutUint32(values[i], uint32(i%nunique))
		}

		builder := array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: 16})
		builder.AppendValues(values, nil)
		input := builder.NewFixedSizeBinaryArray()
		builder.Release()
		defer input.Release()

		benchmarkDictionaryEncodeBinary(b, ctx, input, nvalues*16)
	})
}

func benchmarkDictionaryEncodeBinary(b *testing.B, ctx context.Context, input arrow.Array, bytes int) {
	b.ReportAllocs()
	b.SetBytes(int64(bytes))
	b.ResetTimer()
	for b.Loop() {
		result, err := compute.DictionaryEncodeArray(ctx, compute.DictionaryEncodeOptions{}, input)
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}
