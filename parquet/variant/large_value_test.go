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

package variant_test

import (
	"encoding/binary"
	"strconv"
	"syscall"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/stretchr/testify/require"
)

const maxEncodedOffset = uint64(1<<32 - 1)

func mappedVariantValue(t *testing.T, dataStart uint64) []byte {
	t.Helper()
	if strconv.IntSize < 64 {
		t.Skip("large variant values require 64-bit indexes")
	}

	value, err := syscall.Mmap(
		-1,
		0,
		int(dataStart+maxEncodedOffset),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_ANON|syscall.MAP_PRIVATE,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, syscall.Munmap(value)) })
	return value
}

func appendMaxSizeBinary(value []byte, dataStart int) {
	value[dataStart] = byte(variant.PrimitiveBinary << 2)
	binary.LittleEndian.PutUint32(value[dataStart+1:], uint32(maxEncodedOffset-5))
}

func TestCompoundValueMayExceedUint32(t *testing.T) {
	metaBytes := []byte{1, 1, 0, 1, 'a'}
	meta, err := variant.NewMetadata(metaBytes)
	require.NoError(t, err)

	t.Run("array", func(t *testing.T) {
		const dataStart = uint64(10)
		value := mappedVariantValue(t, dataStart)
		value[0] = byte(3<<2) | byte(variant.BasicArray)
		value[1] = 1
		binary.LittleEndian.PutUint32(value[2:6], 0)
		binary.LittleEndian.PutUint32(value[6:10], uint32(maxEncodedOffset))
		appendMaxSizeBinary(value, int(dataStart))

		parsed, err := variant.NewWithMetadata(meta, value)
		require.NoError(t, err)
		array := parsed.Value().(variant.ArrayValue)
		child, err := array.Value(0)
		require.NoError(t, err)
		require.Equal(t, variant.Binary, child.Type())
		require.Len(t, child.Bytes(), int(maxEncodedOffset))
	})

	t.Run("object", func(t *testing.T) {
		const dataStart = uint64(11)
		value := mappedVariantValue(t, dataStart)
		value[0] = byte(3<<2) | byte(variant.BasicObject)
		value[1] = 1
		value[2] = 0
		binary.LittleEndian.PutUint32(value[3:7], 0)
		binary.LittleEndian.PutUint32(value[7:11], uint32(maxEncodedOffset))
		appendMaxSizeBinary(value, int(dataStart))

		parsed, err := variant.NewWithMetadata(meta, value)
		require.NoError(t, err)
		object := parsed.Value().(variant.ObjectValue)
		field, err := object.FieldAt(0)
		require.NoError(t, err)
		require.Equal(t, variant.Binary, field.Value.Type())
		require.Len(t, field.Value.Bytes(), int(maxEncodedOffset))
	})
}
