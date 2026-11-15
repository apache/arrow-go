//go:build darwin || linux

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

package scalar_test

import (
	"math"
	"strconv"
	"syscall"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mappedBytes returns n bytes of address space that the system commits only
// when they are touched.
func mappedBytes(t *testing.T, n int64) []byte {
	t.Helper()
	if strconv.IntSize < 64 {
		t.Skip("values above the view size limit require 64-bit lengths")
	}
	b, err := syscall.Mmap(-1, 0, int(n), syscall.PROT_READ, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, syscall.Munmap(b)) })
	return b
}

func TestViewScalarSizeLimit(t *testing.T) {
	var limit int64 = math.MaxInt32
	value := mappedBytes(t, limit+1)
	for _, n := range []int64{limit, limit + 1} {
		buf := memory.NewBufferBytes(value[:n])
		for _, sc := range []scalar.Scalar{
			scalar.NewBinaryViewScalar(buf),
			scalar.NewStringViewScalarFromBuffer(buf),
			scalar.NewBinaryScalar(buf, arrow.BinaryTypes.BinaryView),
			scalar.NewBinaryScalar(buf, arrow.BinaryTypes.StringView),
		} {
			if n <= limit {
				assert.NoError(t, sc.Validate(), "%T %s of %d bytes", sc, sc.DataType(), n)
				continue
			}
			assert.ErrorIs(t, sc.Validate(), arrow.ErrInvalid, "%T %s", sc, sc.DataType())
			assert.ErrorContains(t, sc.ValidateFull(), "exceeds the view size limit", "%T %s", sc, sc.DataType())
			_, err := scalar.MakeArrayFromScalar(sc, 1, memory.DefaultAllocator)
			assert.ErrorIs(t, err, arrow.ErrInvalid, "%T %s", sc, sc.DataType())
			assert.ErrorContains(t, err, "exceeds the view size limit", "%T %s", sc, sc.DataType())
		}
	}
}
