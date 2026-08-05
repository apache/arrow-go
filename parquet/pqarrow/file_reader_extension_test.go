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

package pqarrow

import (
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

type mismatchingExtensionType struct {
	arrow.ExtensionBase
	storageTypeCalls int
}

type mismatchingExtensionArray struct {
	array.ExtensionArrayBase
}

type stableExtensionType struct {
	arrow.ExtensionBase
}

type stableExtensionArray struct {
	array.ExtensionArrayBase
}

func (*stableExtensionType) StorageType() arrow.DataType { return arrow.PrimitiveTypes.Int32 }

func (*stableExtensionType) ArrayType() reflect.Type {
	return reflect.TypeFor[stableExtensionArray]()
}

func (*stableExtensionType) ExtensionName() string { return "test.stable" }

func (*stableExtensionType) ExtensionEquals(other arrow.ExtensionType) bool {
	_, ok := other.(*stableExtensionType)
	return ok
}

func (*stableExtensionType) Serialize() string { return "" }

func (*stableExtensionType) Deserialize(arrow.DataType, string) (arrow.ExtensionType, error) {
	return &stableExtensionType{
		ExtensionBase: arrow.ExtensionBase{Storage: arrow.PrimitiveTypes.Int32},
	}, nil
}

func (t *mismatchingExtensionType) StorageType() arrow.DataType {
	t.storageTypeCalls++
	if t.storageTypeCalls > 2 {
		return arrow.PrimitiveTypes.Int64
	}
	return arrow.PrimitiveTypes.Int32
}

func (*mismatchingExtensionType) ArrayType() reflect.Type {
	return reflect.TypeFor[mismatchingExtensionArray]()
}

func (*mismatchingExtensionType) ExtensionName() string { return "test.mismatching" }

func (*mismatchingExtensionType) ExtensionEquals(other arrow.ExtensionType) bool {
	_, ok := other.(*mismatchingExtensionType)
	return ok
}

func (*mismatchingExtensionType) Serialize() string { return "" }

func (*mismatchingExtensionType) Deserialize(arrow.DataType, string) (arrow.ExtensionType, error) {
	return &mismatchingExtensionType{
		ExtensionBase: arrow.ExtensionBase{Storage: arrow.PrimitiveTypes.Int32},
	}, nil
}

type chunkedColumnReader struct {
	colReaderImpl
	chunks *arrow.Chunked
}

func (r *chunkedColumnReader) BuildArray(int64) (*arrow.Chunked, error) {
	return r.chunks, nil
}

func TestExtensionReaderBuildArrayReleasesPartialChunksOnPanic(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b := array.NewInt32Builder(mem)
	b.Append(1)
	first := b.NewInt32Array()
	b.Append(2)
	second := b.NewInt32Array()
	b.Release()

	chunks := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{first, second})
	first.Release()
	second.Release()

	extType := &mismatchingExtensionType{
		ExtensionBase: arrow.ExtensionBase{Storage: arrow.PrimitiveTypes.Int32},
	}
	r := extensionReader{
		colReaderImpl: &chunkedColumnReader{chunks: chunks},
		fieldWithExt:  arrow.Field{Name: "extension", Type: extType},
	}

	require.Panics(t, func() {
		_, _ = r.BuildArray(0)
	})
	require.Zero(t, mem.CurrentAlloc())
}

func TestExtensionReaderBuildArrayReleasesChunks(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b := array.NewInt32Builder(mem)
	b.Append(1)
	first := b.NewInt32Array()
	b.Append(2)
	second := b.NewInt32Array()
	b.Release()

	chunks := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{first, second})
	first.Release()
	second.Release()

	extType := &stableExtensionType{
		ExtensionBase: arrow.ExtensionBase{Storage: arrow.PrimitiveTypes.Int32},
	}
	r := extensionReader{
		colReaderImpl: &chunkedColumnReader{chunks: chunks},
		fieldWithExt:  arrow.Field{Name: "extension", Type: extType},
	}

	out, err := r.BuildArray(0)
	require.NoError(t, err)
	out.Release()
}
