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

package exec_test

import (
	"fmt"
	"math"
	"strconv"
	"syscall"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArraySpan_FillFromScalarViewSizeLimit(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("values above the view size limit require 64-bit lengths")
	}
	// The system commits mapped pages only when they are touched.
	var size int64 = math.MaxInt32 + 1
	value, err := syscall.Mmap(-1, 0, int(size), syscall.PROT_READ, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	require.NoError(t, err)
	defer func() { require.NoError(t, syscall.Munmap(value)) }()

	buf := memory.NewBufferBytes(value)
	for _, sc := range []scalar.Scalar{
		scalar.NewBinaryViewScalar(buf),
		scalar.NewStringViewScalarFromBuffer(buf),
		scalar.NewBinaryScalar(buf, arrow.BinaryTypes.BinaryView),
		scalar.NewBinaryScalar(buf, arrow.BinaryTypes.StringView),
	} {
		var span exec.ArraySpan
		want := fmt.Sprintf("invalid: %s value of %d bytes exceeds the view size limit", sc.DataType(), size)
		assert.PanicsWithError(t, want, func() { span.FillFromScalar(sc) }, "%T %s", sc, sc.DataType())
	}
}
